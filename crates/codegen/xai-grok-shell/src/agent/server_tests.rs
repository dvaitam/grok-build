//! Slot-lifecycle tests for the persistent agent boot path.

use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol as acp;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::{AgentSlot, BootSlotGuard, fail_boot, reclaim_abandoned_boot};

fn booting(boot_id: u64) -> (tokio::sync::watch::Sender<()>, AgentSlot) {
    let (boot_tx, boot_rx) = tokio::sync::watch::channel(());
    (
        boot_tx,
        AgentSlot::Booting {
            boot_id,
            rx: boot_rx,
        },
    )
}

fn boot_rx(slot: &AgentSlot) -> tokio::sync::watch::Receiver<()> {
    match slot {
        AgentSlot::Booting { rx, .. } => rx.clone(),
        _ => panic!("expected Booting"),
    }
}

#[tokio::test]
async fn dropped_boot_sender_reclaims_booting_slot() {
    let (boot_tx, slot_val) = booting(1);
    let slot = tokio::sync::Mutex::new(slot_val);
    let rx = boot_rx(&*slot.lock().await);
    drop(boot_tx);

    timeout(Duration::from_secs(1), reclaim_abandoned_boot(&slot, rx, 1))
        .await
        .expect("reclaim must not wait after the sender is gone");
    assert!(matches!(*slot.lock().await, AgentSlot::Down));
}

#[tokio::test]
async fn boot_guard_drop_resets_booting_and_wakes_waiter() {
    let (boot_tx, slot_val) = booting(1);
    let slot = Arc::new(tokio::sync::Mutex::new(slot_val));
    let rx = boot_rx(&*slot.lock().await);

    let waiter = {
        let slot = Arc::clone(&slot);
        tokio::spawn(async move { reclaim_abandoned_boot(&slot, rx, 1).await })
    };

    drop(BootSlotGuard::new(&slot, boot_tx, 1));
    timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter must observe the dropped boot sender")
        .expect("waiter task");
    assert!(matches!(*slot.lock().await, AgentSlot::Down));
}

#[tokio::test]
async fn boot_guard_does_not_clobber_up_after_notify() {
    let (boot_tx, slot_val) = booting(1);
    let (conn_tx, _conn_rx) = mpsc::unbounded_channel();
    let slot = tokio::sync::Mutex::new(slot_val);
    let mut guard = BootSlotGuard::new(&slot, boot_tx, 1);
    *slot.lock().await = AgentSlot::Up(conn_tx);
    guard.notify_waiters();
    drop(guard);
    assert!(matches!(*slot.lock().await, AgentSlot::Up(_)));
}

#[tokio::test]
async fn waiter_keeps_up_when_sender_drops_after_success() {
    let (boot_tx, slot_val) = booting(1);
    let (conn_tx, _conn_rx) = mpsc::unbounded_channel();
    let slot = tokio::sync::Mutex::new(slot_val);
    let rx = boot_rx(&*slot.lock().await);
    *slot.lock().await = AgentSlot::Up(conn_tx);
    drop(boot_tx);

    timeout(Duration::from_secs(1), reclaim_abandoned_boot(&slot, rx, 1))
        .await
        .expect("reclaim must return");
    assert!(matches!(*slot.lock().await, AgentSlot::Up(_)));
}

#[tokio::test]
async fn stale_reclaim_does_not_clobber_newer_boot() {
    let (old_tx, old) = booting(1);
    let slot = tokio::sync::Mutex::new(old);
    let old_rx = boot_rx(&*slot.lock().await);
    let (_new_tx, new) = booting(2);
    *slot.lock().await = new;
    drop(old_tx);

    timeout(
        Duration::from_secs(1),
        reclaim_abandoned_boot(&slot, old_rx, 1),
    )
    .await
    .expect("stale reclaim must return");
    assert!(matches!(
        *slot.lock().await,
        AgentSlot::Booting { boot_id: 2, .. }
    ));
}

#[tokio::test]
async fn stale_guard_drop_does_not_clobber_newer_boot() {
    let (old_tx, old) = booting(1);
    let slot = tokio::sync::Mutex::new(old);
    let guard = BootSlotGuard::new(&slot, old_tx, 1);
    let (_new_tx, new) = booting(2);
    *slot.lock().await = new;
    drop(guard);
    assert!(matches!(
        *slot.lock().await,
        AgentSlot::Booting { boot_id: 2, .. }
    ));
}

#[tokio::test]
async fn stale_fail_boot_does_not_clobber_newer_boot() {
    let (_old_tx, old) = booting(1);
    let slot = tokio::sync::Mutex::new(old);
    let (_new_tx, new) = booting(2);
    *slot.lock().await = new;
    let _ = fail_boot(&slot, 1).await;
    assert!(matches!(
        *slot.lock().await,
        AgentSlot::Booting { boot_id: 2, .. }
    ));
}

#[tokio::test]
async fn matching_fail_boot_resets_slot() {
    let (_tx, val) = booting(3);
    let slot = tokio::sync::Mutex::new(val);
    let _ = fail_boot(&slot, 3).await;
    assert!(matches!(*slot.lock().await, AgentSlot::Down));
}

#[test]
fn browser_urls_include_lan_hosts_when_unspecified() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    let bind: SocketAddr = "0.0.0.0:2419".parse().unwrap();
    let urls = super::browser_urls(
        bind,
        &[
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ],
        "a b",
    );
    assert_eq!(
        urls,
        vec![
            "http://127.0.0.1:2419/?server-key=a%20b".to_string(),
            "http://192.168.1.20:2419/?server-key=a%20b".to_string(),
        ]
    );
}

#[test]
fn browser_urls_stay_on_explicit_bind() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    let bind: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let urls = super::browser_urls(bind, &[IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))], "k");
    assert_eq!(urls, vec!["http://127.0.0.1:9/?server-key=k".to_string()]);
}

#[tokio::test]
async fn web_ui_serves_page_and_gates_info() {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use super::{AgentConfig, AgentSlot, ServerState, agent_router};

    let state = Arc::new(ServerState {
        agent_config: AgentConfig::default(),
        secret: "test-secret".to_string(),
        agent_slot: tokio::sync::Mutex::new(AgentSlot::Down),
        boot_gen: AtomicU64::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            agent_router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    struct Stop(tokio::task::JoinHandle<()>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _stop = Stop(server);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut index = None;
    for _ in 0..20 {
        match client.get(format!("http://{addr}/")).send().await {
            Ok(response) => {
                index = Some(response);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let index = index.expect("index route must accept connections");
    assert_eq!(index.status(), reqwest::StatusCode::OK);
    let html = index.text().await.unwrap();
    assert!(html.contains("/ws?server-key="));
    assert!(html.contains("session/prompt"));
    assert!(html.contains("session/update"));
    assert!(!html.contains("details.open = true"));
    assert!(html.contains("previewWords"));
    assert!(html.contains("id=\"attach\""));
    assert!(html.contains("Attach image"));
    assert!(html.contains("id=\"usage\""));
    assert!(
        html.contains("_x.ai/billing"),
        "the header reads weekly usage from the billing extension"
    );
    assert!(
        html.contains("type: \"image\""),
        "the composer must send ACP image content blocks"
    );
    assert!(
        html.contains("[hidden] { display: none !important; }"),
        "author display rules must not keep #gate or #app visible when hidden"
    );
    assert!(html.contains("rel=\"icon\""));
    assert!(html.contains("/favicon.svg"));

    let icon = client
        .get(format!("http://{addr}/favicon.svg"))
        .send()
        .await
        .unwrap();
    assert_eq!(icon.status(), reqwest::StatusCode::OK);
    let icon_type = icon
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        icon_type.starts_with("image/svg+xml"),
        "favicon content type was {icon_type}"
    );
    let icon_body = icon.text().await.unwrap();
    assert!(icon_body.contains("<svg"));
    assert!(icon_body.contains("#1d1d1f"));

    let ico = client
        .get(format!("http://{addr}/favicon.ico"))
        .send()
        .await
        .unwrap();
    assert_eq!(ico.status(), reqwest::StatusCode::OK);

    let denied = client
        .get(format!("http://{addr}/api/info"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong = client
        .get(format!("http://{addr}/api/info?server-key=nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    let ok = client
        .get(format!("http://{addr}/api/info?server-key=test-secret"))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&ok.text().await.unwrap()).unwrap();
    let cwd = body.get("cwd").and_then(|value| value.as_str()).unwrap();
    assert!(std::path::Path::new(cwd).is_absolute());
}

fn notification(session_id: &str) -> xai_acp_lib::AcpClientMessage {
    let (response_tx, _response_rx) = tokio::sync::oneshot::channel();
    xai_acp_lib::AcpClientMessage::SessionNotification(xai_acp_lib::AcpArgs {
        request: acp::SessionNotification::new(
            session_id.to_owned(),
            acp::SessionUpdate::AgentMessageChunk(acp::ContentChunk::new("hi".into())),
        ),
        response_tx,
    })
}

fn bound_session(relay: &super::RelayTable, conn_id: u64) -> Option<String> {
    relay.borrow().iter().find_map(|conn| {
        (conn.id == conn_id)
            .then(|| conn.session_id.borrow().clone())
            .flatten()
    })
}

#[test]
fn inbound_bind_selects_one_session_and_new_clears_it() {
    assert_eq!(
        super::inbound_bind(
            r#"{"jsonrpc":"2.0","id":4,"method":"session/load","params":{"sessionId":"sess-a","cwd":"/tmp","mcpServers":[]}}"#
        ),
        Some(super::InboundBind::Exclusive("sess-a".into()))
    );
    assert_eq!(
        super::inbound_bind(
            r#"{"jsonrpc":"2.0","id":9,"method":"session/prompt","params":{"session_id":"sess-b"}}"#
        ),
        Some(super::InboundBind::Exclusive("sess-b".into()))
    );
    assert_eq!(
        super::inbound_bind(
            r#"{"jsonrpc":"2.0","id":5,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}"#
        ),
        Some(super::InboundBind::Clear {
            rpc_id: Some("5".into())
        })
    );
    assert_eq!(
        super::inbound_bind(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#),
        None
    );
}

#[test]
fn outbound_created_session_ignores_a_list_payload() {
    assert_eq!(
        super::outbound_created_session(
            r#"{"jsonrpc":"2.0","id":5,"result":{"sessionId":"sess-new"}}"#
        ),
        Some(("5".into(), "sess-new".into()))
    );
    assert_eq!(
        super::outbound_created_session(
            r#"{"jsonrpc":"2.0","id":2,"result":{"sessions":[],"sessionId":"nope"}}"#
        ),
        None
    );
}

#[test]
fn route_delivers_each_session_only_to_its_socket() {
    let (tx_a, mut rx_a) = mpsc::unbounded_channel();
    let (tx_b, mut rx_b) = mpsc::unbounded_channel();
    let relay = std::rc::Rc::new(std::cell::RefCell::new(vec![
        super::RelayConn {
            id: 1,
            tx: tx_a,
            session_id: std::cell::RefCell::new(None),
        },
        super::RelayConn {
            id: 2,
            tx: tx_b,
            session_id: std::cell::RefCell::new(None),
        },
    ]));
    let pending_a = std::cell::RefCell::new(None);
    let pending_b = std::cell::RefCell::new(None);
    super::apply_inbound_bind(
        &relay,
        1,
        &pending_a,
        r#"{"id":1,"method":"session/load","params":{"sessionId":"sess-a"}}"#,
    );
    super::apply_inbound_bind(
        &relay,
        2,
        &pending_b,
        r#"{"id":1,"method":"session/prompt","params":{"sessionId":"sess-b"}}"#,
    );

    super::route_client_message(&relay, notification("sess-a"));
    super::route_client_message(&relay, notification("sess-b"));
    super::route_client_message(&relay, notification("sess-c"));

    let got_a = rx_a.try_recv().expect("socket A gets its session");
    assert_eq!(
        super::client_message_session_id(&got_a).as_deref(),
        Some("sess-a")
    );
    assert!(
        rx_a.try_recv().is_err(),
        "socket A must not see other sessions"
    );
    let got_b = rx_b.try_recv().expect("socket B gets its session");
    assert_eq!(
        super::client_message_session_id(&got_b).as_deref(),
        Some("sess-b")
    );
    assert!(
        rx_b.try_recv().is_err(),
        "socket B must not see other sessions"
    );
}

#[test]
fn resume_moves_the_stream_off_the_previous_socket() {
    let (tx_a, mut rx_a) = mpsc::unbounded_channel();
    let (tx_b, mut rx_b) = mpsc::unbounded_channel();
    let relay = std::rc::Rc::new(std::cell::RefCell::new(vec![
        super::RelayConn {
            id: 1,
            tx: tx_a,
            session_id: std::cell::RefCell::new(Some("sess-a".into())),
        },
        super::RelayConn {
            id: 2,
            tx: tx_b,
            session_id: std::cell::RefCell::new(None),
        },
    ]));
    let pending = std::cell::RefCell::new(Some("9".into()));
    super::apply_inbound_bind(
        &relay,
        2,
        &pending,
        r#"{"id":3,"method":"session/load","params":{"sessionId":"sess-a"}}"#,
    );
    assert_eq!(bound_session(&relay, 1), None);
    assert_eq!(bound_session(&relay, 2).as_deref(), Some("sess-a"));

    super::route_client_message(&relay, notification("sess-a"));
    assert!(rx_a.try_recv().is_err());
    assert!(rx_b.try_recv().is_ok());
}

#[test]
fn new_session_binds_only_after_its_create_response() {
    let (tx, rx) = mpsc::unbounded_channel();
    let relay = std::rc::Rc::new(std::cell::RefCell::new(vec![super::RelayConn {
        id: 1,
        tx,
        session_id: std::cell::RefCell::new(None),
    }]));
    let pending = std::cell::RefCell::new(None);
    super::apply_inbound_bind(
        &relay,
        1,
        &pending,
        r#"{"id":7,"method":"session/load","params":{"sessionId":"old"}}"#,
    );
    super::apply_inbound_bind(
        &relay,
        1,
        &pending,
        r#"{"id":8,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}"#,
    );
    assert_eq!(bound_session(&relay, 1), None);
    super::route_client_message(&relay, notification("brand-new"));
    assert!(rx.try_recv().is_err(), "the socket is unbound until the create response");
    super::apply_outbound_bind(
        &relay,
        1,
        &pending,
        r#"{"id":8,"result":{"sessionId":"brand-new"}}"#,
    );
    assert_eq!(bound_session(&relay, 1).as_deref(), Some("brand-new"));
    // A list response must not steal the binding.
    super::apply_outbound_bind(&relay, 1, &pending, r#"{"id":2,"result":{"sessions":[]}}"#);
    assert_eq!(bound_session(&relay, 1).as_deref(), Some("brand-new"));
}
