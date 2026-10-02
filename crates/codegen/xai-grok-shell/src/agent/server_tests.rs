//! Slot-lifecycle tests for the persistent agent boot path.

use std::sync::Arc;
use std::time::Duration;

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
    assert!(
        html.contains("[hidden] { display: none !important; }"),
        "author display rules must not keep #gate or #app visible when hidden"
    );

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
