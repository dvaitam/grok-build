//! WebSocket server for remote agent connections.
//!
//! Remote TUI clients connect here to a grok agent running on a different machine.
//!
//! The agent persists across WebSocket reconnections: a single MvpAgent instance is created on first connection and reused for all later ones.
//! Session actors (and any in-flight prompts) therefore survive client disconnects.
//! Each socket receives notifications only for the session it loaded, resumed, or created, so one browser tab cannot see another conversation.
//! Loading a session that another socket still holds moves that stream to the socket that just loaded it.

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use axum::{
    Json, Router,
    extract::{
        ConnectInfo, Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, simplex};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{info, warn};

use agent_client_protocol as acp;
use xai_acp_lib::{
    AcpAgentGatewayReceiver as GatewayReceiver, AcpAgentGatewaySender as GatewaySender,
    AcpClientMessage, LineBufferedRead,
};

use crate::agent::config::{Config as AgentConfig, ModelEntry};
use crate::agent::mvp_agent::MvpAgent;
use crate::agent::remote_config::{ModelFetchAuth, prefetch_models_blocking};

use indexmap::IndexMap;

/// One browser or TUI socket, bound to at most one session.
struct RelayConn {
    id: u64,
    tx: mpsc::UnboundedSender<AcpClientMessage>,
    session_id: RefCell<Option<String>>,
}

/// Live sockets. The relay delivers each notification to the socket bound to that session.
type RelayTable = Rc<RefCell<Vec<RelayConn>>>;

const MAX_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const KEEPALIVE_INTERVAL_SECS: u64 = 15;

/// Configuration for the agent WebSocket server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind the server to
    pub bind_addr: SocketAddr,
    /// Secret token for client authentication (required)
    pub secret: String,
}

/// Shared state for the WebSocket server.
struct ServerState {
    agent_config: AgentConfig,
    secret: String,
    /// Persistent agent slot.
    /// Lazily initialised on first connection; protected by a tokio Mutex so the axum handler (which is `Send`) can acquire it.
    agent_slot: tokio::sync::Mutex<AgentSlot>,
    /// Monotonic id for each boot attempt.
    /// Reclaim/fail/drop must match it or a stale waiter can clobber a newer `Booting` and spawn a second agent.
    boot_gen: AtomicU64,
}

/// Lifecycle of the persistent agent OS thread.
enum AgentSlot {
    Down,
    /// In-flight spawn. `watch` wakes waiters when the slot leaves this state.
    Booting {
        boot_id: u64,
        rx: tokio::sync::watch::Receiver<()>,
    },
    Up(mpsc::UnboundedSender<NewConnectionChannels>),
}

fn is_boot_gen(slot: &AgentSlot, boot_id: u64) -> bool {
    matches!(slot, AgentSlot::Booting { boot_id: id, .. } if *id == boot_id)
}

/// Channels bridging a single WebSocket connection to the agent thread.
struct NewConnectionChannels {
    from_ws_rx: mpsc::UnboundedReceiver<String>,
    to_ws_tx: mpsc::UnboundedSender<String>,
}

#[derive(Debug, serde::Deserialize, Default)]
pub(crate) struct WsQueryParams {
    #[serde(rename = "server-key")]
    pub server_key: Option<String>,
}

/// Validate the bearer token from request headers or query parameters.
fn validate_auth(headers: &HeaderMap, query: &WsQueryParams, expected_secret: &str) -> bool {
    if let Some(token) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        return token == expected_secret;
    }

    if let Some(ref key) = query.server_key {
        return key == expected_secret;
    }

    false
}

/// WebSocket upgrade handler with authentication.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<WsQueryParams>,
) -> Response {
    if !validate_auth(&headers, &query, &state.secret) {
        warn!("Unauthorized connection attempt from {}", addr);
        return (
            StatusCode::UNAUTHORIZED,
            "Invalid or missing authorization token",
        )
            .into_response();
    }

    info!("Authenticated WebSocket connection from {}", addr);
    ws.on_upgrade(move |socket| handle_connection(socket, state, addr))
}

/// Start the persistent agent if needed and return its connection sender.
///
/// Ready covers runtime build only. Waiters do not hold the slot lock.
async fn ensure_persistent_agent(
    state: &ServerState,
) -> Option<mpsc::UnboundedSender<NewConnectionChannels>> {
    loop {
        let mut slot = state.agent_slot.lock().await;
        match &*slot {
            AgentSlot::Up(tx) if !tx.is_closed() => return Some(tx.clone()),
            AgentSlot::Up(_) => {
                warn!("Persistent agent thread died — will respawn");
                *slot = AgentSlot::Down;
            }
            AgentSlot::Booting { boot_id, rx } => {
                let boot_id = *boot_id;
                let rx = rx.clone();
                drop(slot);
                reclaim_abandoned_boot(&state.agent_slot, rx, boot_id).await;
            }
            AgentSlot::Down => {
                let (conn_tx, conn_rx) = mpsc::unbounded_channel();
                let (ready_tx, ready_rx) =
                    tokio::sync::oneshot::channel::<Result<(), std::io::ErrorKind>>();
                let (boot_tx, boot_rx) = tokio::sync::watch::channel(());
                let boot_id = state.boot_gen.fetch_add(1, Ordering::Relaxed) + 1;
                *slot = AgentSlot::Booting {
                    boot_id,
                    rx: boot_rx,
                };
                let agent_config = state.agent_config.clone();
                drop(slot);
                // Drop of this future (client gone mid-ready) must leave the slot, or later callers spin forever on a dead watch
                let mut boot = BootSlotGuard::new(&state.agent_slot, boot_tx, boot_id);
                if let Err(e) = thread::Builder::new()
                    .name("agent-persistent".into())
                    .spawn(move || persistent_agent_thread(agent_config, conn_rx, ready_tx))
                {
                    warn!(error = %e, "Failed to spawn persistent agent thread");
                    return fail_boot(&state.agent_slot, boot_id).await;
                }
                match ready_rx.await {
                    Ok(Ok(())) => {
                        let mut slot = state.agent_slot.lock().await;
                        if is_boot_gen(&slot, boot_id) {
                            *slot = AgentSlot::Up(conn_tx.clone());
                            drop(slot);
                            boot.notify_waiters();
                            info!("Persistent agent thread spawned");
                            return Some(conn_tx);
                        }
                        // Another attempt owns the slot; drop conn_tx so this thread's receiver closes instead of going live
                        drop(slot);
                        boot.notify_waiters();
                    }
                    Ok(Err(kind)) => {
                        warn!(?kind, "Persistent agent runtime failed");
                        return fail_boot(&state.agent_slot, boot_id).await;
                    }
                    Err(_) => {
                        warn!("Persistent agent thread died during startup");
                        return fail_boot(&state.agent_slot, boot_id).await;
                    }
                }
            }
        }
    }
}

/// Resets a `Booting` slot whose watch sender vanished (cancel / panic).
///
/// `changed()` then returns immediately; without reclaim, waiters loop on `Booting` forever and the agent can never start again.
async fn reclaim_abandoned_boot(
    slot: &tokio::sync::Mutex<AgentSlot>,
    mut rx: tokio::sync::watch::Receiver<()>,
    boot_id: u64,
) {
    if rx.changed().await.is_err() {
        let mut slot = slot.lock().await;
        if is_boot_gen(&slot, boot_id) {
            *slot = AgentSlot::Down;
        }
    }
}

/// Best-effort revert of `Booting` if `ensure_persistent_agent` is dropped before it stores `Up` or `Down`.
/// `try_lock` is enough: a waiter that holds the mutex will see the dropped sender and reclaim.
#[must_use]
struct BootSlotGuard<'a> {
    slot: &'a tokio::sync::Mutex<AgentSlot>,
    boot_tx: Option<tokio::sync::watch::Sender<()>>,
    boot_id: u64,
}

impl<'a> BootSlotGuard<'a> {
    fn new(
        slot: &'a tokio::sync::Mutex<AgentSlot>,
        boot_tx: tokio::sync::watch::Sender<()>,
        boot_id: u64,
    ) -> Self {
        Self {
            slot,
            boot_tx: Some(boot_tx),
            boot_id,
        }
    }

    fn notify_waiters(&mut self) {
        if let Some(tx) = self.boot_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for BootSlotGuard<'_> {
    fn drop(&mut self) {
        let Some(tx) = self.boot_tx.take() else {
            return;
        };
        if let Ok(mut slot) = self.slot.try_lock()
            && is_boot_gen(&slot, self.boot_id)
        {
            *slot = AgentSlot::Down;
        }
        drop(tx);
    }
}

async fn fail_boot(
    slot: &tokio::sync::Mutex<AgentSlot>,
    boot_id: u64,
) -> Option<mpsc::UnboundedSender<NewConnectionChannels>> {
    let mut slot = slot.lock().await;
    if is_boot_gen(&slot, boot_id) {
        *slot = AgentSlot::Down;
    }
    None
}

fn persistent_agent_thread(
    agent_config: AgentConfig,
    conn_rx: mpsc::UnboundedReceiver<NewConnectionChannels>,
    ready_tx: tokio::sync::oneshot::Sender<Result<(), std::io::ErrorKind>>,
) -> std::io::Result<()> {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    let rt = match xai_tty_utils::runtime::build_with_blocking_pool(builder.enable_all()) {
        Ok(rt) => {
            if ready_tx.send(Ok(())).is_err() {
                // The boot was cancelled; drop `rt` so its keep-alive pool does not overlap a respawn's 16-wide pre-warm (EAGAIN)
                return Ok(());
            }
            rt
        }
        Err(e) => {
            warn!(error = %e, "Failed to create runtime for agent");
            let _ = ready_tx.send(Err(e.kind()));
            return Err(e);
        }
    };

    // The boot can still be abandoned after a successful ack: cancel drops `conn_tx`, which closes this receiver
    if conn_rx.is_closed() {
        return Ok(());
    }

    // Prefetch is HTTP; it must not delay the first WS.
    let auth = agent_config.create_auth_manager().current();
    let fetch_auth = ModelFetchAuth::resolve(&agent_config.endpoints, auth.is_some());
    let prefetched_models = if auth.is_some()
        || agent_config.endpoints.has_custom_endpoint()
        || fetch_auth != ModelFetchAuth::Session
    {
        prefetch_models_blocking(&agent_config.endpoints, auth.as_ref(), fetch_auth)
    } else {
        None
    };
    info!("Prefetched models: {:?}", prefetched_models);

    if conn_rx.is_closed() {
        return Ok(());
    }

    // Declared before the `LocalSet` so it is dropped after the `LocalRef` tasks on it, on unwind too; see `LocalRef`.
    let mut keepalive: Option<Rc<MvpAgent>> = None;
    let local_set = tokio::task::LocalSet::new();
    local_set.block_on(&rt, async {
        run_persistent_agent(agent_config, conn_rx, prefetched_models, &mut keepalive).await
    });
    drop(local_set);
    drop(keepalive);

    warn!("Persistent agent thread exiting");
    Ok(())
}

/// Handle an authenticated WebSocket connection.
/// On first connection, spawns a persistent agent thread that owns the MvpAgent.
/// On subsequent connections, the existing agent thread accepts the new socket. Notifications still go only to the socket bound to that session.
async fn handle_connection(ws: WebSocket, state: Arc<ServerState>, peer_addr: SocketAddr) {
    info!("New WebSocket connection from {}", peer_addr);

    let (mut ws_write, mut ws_read) = ws.split();

    let (to_agent_tx, to_agent_rx) = mpsc::unbounded_channel::<String>();
    let (from_agent_tx, mut from_agent_rx) = mpsc::unbounded_channel::<String>();

    let attached = match ensure_persistent_agent(&state).await {
        Some(tx) => {
            let sent = tx
                .send(NewConnectionChannels {
                    from_ws_rx: to_agent_rx,
                    to_ws_tx: from_agent_tx,
                })
                .is_ok();
            if !sent {
                warn!("Failed to send connection channels to agent thread");
                let mut slot = state.agent_slot.lock().await;
                if let AgentSlot::Up(live) = &*slot
                    && live.is_closed()
                {
                    *slot = AgentSlot::Down;
                }
            }
            sent
        }
        None => {
            warn!("Persistent agent is not available");
            false
        }
    };
    if !attached {
        // Do not start the ping loop: the client would see a live socket that never reaches the agent
        let _ = ws_write
            .send(Message::Close(Some(CloseFrame {
                code: close_code::AGAIN,
                reason: "persistent agent unavailable".into(),
            })))
            .await;
        return;
    }

    let read_task = tokio::spawn(async move {
        while let Some(msg) = ws_read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let text_str: &str = text.as_ref();
                    let trimmed = text_str.trim_end_matches(['\r', '\n']);
                    // Skip browser keepalive pings (non-JSON text)
                    if trimmed == "ping" || trimmed.is_empty() {
                        continue;
                    }
                    if to_agent_tx.send(trimmed.to_string()).is_err() {
                        break;
                    }
                }
                Ok(Message::Binary(bin)) => {
                    if let Ok(s) = std::str::from_utf8(&bin) {
                        let trimmed = s.trim_end_matches(['\r', '\n']);
                        if trimmed == "ping" || trimmed.is_empty() {
                            continue;
                        }
                        if to_agent_tx.send(trimmed.to_string()).is_err() {
                            break;
                        }
                    }
                }
                Ok(Message::Close(frame)) => {
                    if let Some(f) = frame {
                        info!(
                            "WebSocket close from {}: {} {}",
                            peer_addr, f.code, f.reason
                        );
                    }
                    break;
                }
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                Err(e) => {
                    warn!("WebSocket read error from {}: {:?}", peer_addr, e);
                    break;
                }
            }
        }
    });

    let write_task = tokio::spawn(async move {
        let mut keepalive = tokio::time::interval(Duration::from_secs(KEEPALIVE_INTERVAL_SECS));

        loop {
            tokio::select! {
                Some(msg) = from_agent_rx.recv() => {
                    if ws_write.send(Message::Text(msg.into())).await.is_err() {
                        break;
                    }
                }
                _ = keepalive.tick() => {
                    if ws_write.send(Message::Ping(vec![].into())).await.is_err() {
                        break;
                    }
                }
                else => break,
            }
        }
    });

    tokio::select! {
        _ = read_task => {}
        _ = write_task => {}
    }

    info!("WebSocket connection ended for {}", peer_addr);
}

/// What an inbound ACP line does to this socket's session binding.
#[derive(Debug, PartialEq, Eq)]
enum InboundBind {
    /// `session/new`: stop delivering the previous conversation. `rpc_id` matches the create response.
    Clear { rpc_id: Option<String> },
    /// This socket becomes the only recipient for `session_id`.
    Exclusive(String),
}

fn json_rpc_id(value: &serde_json::Value) -> Option<String> {
    match value.get("id")? {
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// `sessionId` on this object, or one level down under `params` for a wrapped ext call.
fn json_session_id(value: &serde_json::Value) -> Option<String> {
    let direct = value
        .get("sessionId")
        .or_else(|| value.get("session_id"))
        .and_then(|id| id.as_str())
        .filter(|id| !id.is_empty());
    if let Some(id) = direct {
        return Some(id.to_string());
    }
    value.get("params").and_then(|params| {
        params
            .get("sessionId")
            .or_else(|| params.get("session_id"))
            .and_then(|id| id.as_str())
            .filter(|id| !id.is_empty())
            .map(str::to_string)
    })
}

fn inbound_bind(line: &str) -> Option<InboundBind> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    let method = value.get("method").and_then(|method| method.as_str())?;
    let session_id = value.get("params").and_then(json_session_id);
    match method {
        "session/new" => Some(InboundBind::Clear {
            rpc_id: json_rpc_id(&value),
        }),
        "session/load" | "session/resume" | "session/prompt" => {
            session_id.map(InboundBind::Exclusive)
        }
        _ => None,
    }
}

/// `(rpc id, session id)` when this line is the response that names a session just created.
fn outbound_created_session(line: &str) -> Option<(String, String)> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    let id = json_rpc_id(&value)?;
    let result = value.get("result")?;
    if result.get("sessions").is_some() {
        return None;
    }
    Some((id, json_session_id(result)?))
}

fn session_id_str(id: &acp::SessionId) -> String {
    id.0.to_string()
}

fn raw_session_id(raw: &serde_json::value::RawValue) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw.get()).ok()?;
    json_session_id(&value)
}

fn client_message_session_id(msg: &AcpClientMessage) -> Option<String> {
    Some(match msg {
        AcpClientMessage::RequestPermission(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::ReadTextFile(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::WriteTextFile(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::SessionNotification(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::CreateTerminal(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::TerminalOutput(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::ReleaseTerminal(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::WaitForTerminalExit(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::KillTerminalCommand(args) => session_id_str(&args.request.session_id),
        AcpClientMessage::ExtMethod(args) => return raw_session_id(args.request.params.as_ref()),
        AcpClientMessage::ExtNotification(args) => {
            return raw_session_id(args.request.params.as_ref());
        }
    })
}

fn bind_exclusive(relay: &RelayTable, conn_id: u64, session_id: &str) {
    let relay = relay.borrow();
    for conn in relay.iter() {
        let mut slot = conn.session_id.borrow_mut();
        if conn.id == conn_id {
            *slot = Some(session_id.to_string());
        } else if slot.as_deref() == Some(session_id) {
            *slot = None;
        }
    }
}

fn clear_binding(relay: &RelayTable, conn_id: u64) {
    let relay = relay.borrow();
    for conn in relay.iter() {
        if conn.id == conn_id {
            *conn.session_id.borrow_mut() = None;
        }
    }
}

fn remove_relay_conn(relay: &RelayTable, conn_id: u64) {
    relay.borrow_mut().retain(|conn| conn.id != conn_id);
}

fn apply_inbound_bind(
    relay: &RelayTable,
    conn_id: u64,
    pending_new: &RefCell<Option<String>>,
    line: &str,
) {
    match inbound_bind(line) {
        Some(InboundBind::Clear { rpc_id }) => {
            *pending_new.borrow_mut() = rpc_id;
            clear_binding(relay, conn_id);
        }
        Some(InboundBind::Exclusive(session_id)) => {
            *pending_new.borrow_mut() = None;
            bind_exclusive(relay, conn_id, &session_id);
        }
        None => {}
    }
}

fn apply_outbound_bind(
    relay: &RelayTable,
    conn_id: u64,
    pending_new: &RefCell<Option<String>>,
    line: &str,
) {
    let Some((rpc_id, session_id)) = outbound_created_session(line) else {
        return;
    };
    let matches = pending_new.borrow().as_deref() == Some(rpc_id.as_str());
    if !matches {
        return;
    }
    *pending_new.borrow_mut() = None;
    bind_exclusive(relay, conn_id, &session_id);
}

fn route_client_message(relay: &RelayTable, msg: AcpClientMessage) {
    let Some(session_id) = client_message_session_id(&msg) else {
        return;
    };
    let tx = {
        let relay = relay.borrow();
        relay.iter().find_map(|conn| {
            (conn.session_id.borrow().as_deref() == Some(session_id.as_str()))
                .then(|| conn.tx.clone())
        })
    };
    if let Some(tx) = tx {
        let _ = tx.send(msg);
    }
    relay.borrow_mut().retain(|conn| !conn.tx.is_closed());
}

/// Run the persistent agent on a dedicated thread with LocalSet. The MvpAgent is created **once** and reused across WebSocket reconnections.
/// Session actors hold cloned `GatewaySender` handles onto a persistent gateway channel, so they can always send notifications.
/// A relay task forwards each message to the socket bound to that message's session, and drops it when no socket is bound.
async fn run_persistent_agent(
    mut agent_config: AgentConfig,
    mut connection_rx: mpsc::UnboundedReceiver<NewConnectionChannels>,
    prefetched_models: Option<IndexMap<String, ModelEntry>>,
    keepalive: &mut Option<Rc<MvpAgent>>,
) {
    let (gw_tx, mut gw_rx) = tokio::sync::mpsc::unbounded_channel::<AcpClientMessage>();
    let gateway = GatewaySender::new(gw_tx);

    let auth_manager = Arc::new(agent_config.create_auth_manager());
    let agent_cancel = tokio_util::sync::CancellationToken::new();
    // Covers unwind; the explicit cancel below keeps its teardown ordering.
    let _cancel_on_exit = agent_cancel.clone().drop_guard();
    auth_manager.start_proactive_refresh(agent_cancel.clone());
    xai_grok_cloud_config::managed_config::ensure_managed_policy_present(&auth_manager).await;
    // Current-thread boot: resolve settings before sync bootstrap.
    let boot = match crate::agent::init::resolve_boot_startup_settings(
        &mut agent_config,
        &agent_cancel,
        prefetched_models.is_none(),
        auth_manager.current(),
    )
    .await
    {
        Ok(boot) => boot,
        // A cancelled boot unwinds; only a real config error exits.
        Err(crate::agent::init::BootstrapError::Cancelled) => return,
        Err(err) => crate::agent::init::exit_on_config_error(err),
    };
    crate::agent::app::apply_otel_config(&auth_manager, &agent_config.grok_com_config);
    let agent = Rc::new(
        MvpAgent::new(
            gateway,
            &agent_config,
            auth_manager,
            prefetched_models,
            Some(boot),
        )
        .unwrap_or_else(crate::agent::init::exit_on_config_error),
    );
    // Published before any `LocalRef` task can be spawned, so a panic below cannot free the agent first.
    *keepalive = Some(Rc::clone(&agent));
    agent.models_manager.spawn_background_refresh();

    let relay: RelayTable = Rc::new(RefCell::new(Vec::new()));
    let next_conn_id = Rc::new(RefCell::new(1u64));

    let relay_for_task = relay.clone();
    tokio::task::spawn_local(async move {
        while let Some(msg) = gw_rx.recv().await {
            route_client_message(&relay_for_task, msg);
        }
    });

    while let Some(channels) = connection_rx.recv().await {
        info!("Agent thread: setting up new ACP connection (reconnect)");
        let id = {
            let mut next = next_conn_id.borrow_mut();
            let id = *next;
            *next = next.wrapping_add(1).max(1);
            id
        };
        setup_acp_connection(agent.clone(), channels, relay.clone(), id);
    }

    info!("Agent thread: connection channel closed, exiting");
    agent_cancel.cancel();
}

/// Set up a new ACP connection for a WebSocket connection, reusing the existing MvpAgent.
/// The socket starts unbound. `session/load`, `session/resume`, and `session/prompt` bind it to that session.
/// `session/new` unbinds it until the create response names the new id.
fn setup_acp_connection(
    agent: Rc<MvpAgent>,
    channels: NewConnectionChannels,
    relay: RelayTable,
    conn_id: u64,
) {
    let NewConnectionChannels {
        mut from_ws_rx,
        to_ws_tx,
    } = channels;

    let (agent_read_rx, mut agent_read_tx) = simplex(MAX_BUFFER_SIZE);
    let (agent_write_rx, agent_write_tx) = simplex(MAX_BUFFER_SIZE);

    let incoming = agent_read_rx.compat();
    let outgoing = agent_write_tx.compat_write();

    let (conn_gw_tx, conn_gw_rx) = tokio::sync::mpsc::unbounded_channel::<AcpClientMessage>();

    relay.borrow_mut().push(RelayConn {
        id: conn_id,
        tx: conn_gw_tx,
        session_id: RefCell::new(None),
    });
    let pending_new: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));

    // `Agent` is implemented for `Rc<T: Agent>` so this works.
    let incoming = LineBufferedRead::spawn_local(incoming);
    let (conn, handle_io) = acp::AgentSideConnection::new(agent, outgoing, incoming, |fut| {
        tokio::task::spawn_local(fut);
    });
    tokio::task::spawn_local(
        GatewayReceiver::new(conn_gw_rx, conn)
            .with_on_meta(xai_grok_otel::span_from_meta_traceparent)
            .run(),
    );

    let relay_in = relay.clone();
    let pending_in = pending_new.clone();
    tokio::task::spawn_local(async move {
        while let Some(msg) = from_ws_rx.recv().await {
            // Log messages that lack both `id` and `method`
            // The ACP layer only prints "received message with neither id nor method" without the payload, making debugging impossible
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&msg)
                && v.get("id").is_none()
                && v.get("method").is_none()
            {
                warn!(
                    len = msg.len(),
                    "incoming WS message has neither id nor method"
                );
            }
            // Bind before the agent sees `session/load`, so the history replay reaches this socket.
            apply_inbound_bind(&relay_in, conn_id, &pending_in, &msg);
            if agent_read_tx.write_all(msg.as_bytes()).await.is_err() {
                break;
            }
            if agent_read_tx.write_all(b"\n").await.is_err() {
                break;
            }
        }
        // WS disconnected: the simplex writer is dropped, causing `handle_io` to complete.
        // Dropping this socket's relay sender stops its gateway receiver.
        // The MvpAgent and session actors stay alive for the next connection.
        remove_relay_conn(&relay_in, conn_id);
    });

    let relay_out = relay.clone();
    let pending_out = pending_new;
    tokio::task::spawn_local(async move {
        let mut reader = BufReader::new(agent_write_rx);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let msg = line.trim_end_matches(['\r', '\n']);
                    if msg.is_empty() {
                        continue;
                    }
                    // Bind the created id before the browser can send `session/prompt`.
                    apply_outbound_bind(&relay_out, conn_id, &pending_out, msg);
                    if to_ws_tx.send(msg.to_string()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        remove_relay_conn(&relay_out, conn_id);
    });

    // Run the ACP IO handler fire-and-forget so the connection loop is not blocked
    // It completes when the WS disconnects
    tokio::task::spawn_local(async move {
        let _ = handle_io.await;
        info!("ACP connection IO handler completed");
    });
}

const WEB_UI_HTML: &str = include_str!("web_ui.html");
const FAVICON_SVG: &str = include_str!("favicon.svg");

/// Browser URLs for this bind address.
/// An unspecified address (`0.0.0.0` or `::`) is advertised as loopback plus `lan_hosts`, so another machine on the LAN can open the page.
/// A specific bind address is the only host advertised.
pub fn browser_urls(bind: SocketAddr, lan_hosts: &[std::net::IpAddr], secret: &str) -> Vec<String> {
    use std::net::{IpAddr, Ipv4Addr};
    let key = percent_encode(secret);
    let mut hosts = Vec::new();
    if bind.ip().is_unspecified() {
        hosts.push(IpAddr::V4(Ipv4Addr::LOCALHOST));
        for ip in lan_hosts {
            if ip.is_loopback() || ip.is_unspecified() || hosts.contains(ip) {
                continue;
            }
            hosts.push(*ip);
        }
    } else {
        hosts.push(bind.ip());
    }
    let port = bind.port();
    hosts
        .into_iter()
        .map(|ip| {
            let host = match ip {
                IpAddr::V4(v4) => v4.to_string(),
                IpAddr::V6(v6) => format!("[{v6}]"),
            };
            format!("http://{host}:{port}/?server-key={key}")
        })
        .collect()
}

fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

async fn web_index() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(WEB_UI_HTML))
}

async fn favicon() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        FAVICON_SVG,
    )
}

async fn api_info(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(query): Query<WsQueryParams>,
) -> Response {
    if !validate_auth(&headers, &query, &state.secret) {
        return (
            StatusCode::UNAUTHORIZED,
            "Invalid or missing authorization token",
        )
            .into_response();
    }
    let Ok(cwd) = std::env::current_dir() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not resolve working directory",
        )
            .into_response();
    };
    let Some(cwd) = cwd.to_str() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "working directory is not utf-8",
        )
            .into_response();
    };
    if !std::path::Path::new(cwd).is_absolute() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "working directory is not absolute",
        )
            .into_response();
    }
    Json(serde_json::json!({ "cwd": cwd })).into_response()
}

fn agent_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/", get(web_index))
        .route("/favicon.ico", get(favicon))
        .route("/favicon.svg", get(favicon))
        .route("/api/info", get(api_info))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

/// Run the agent WebSocket server.
/// This starts a WebSocket server that accepts authenticated connections from remote TUI clients and from the browser UI at `/`.
/// A single agent instance is shared across all connections (persisted across reconnections) so in-flight session work survives client disconnects.
/// Each connection is bound to one session at a time, and only that session's updates are written to its socket.
pub async fn run_agent_server(
    config: ServerConfig,
    agent_config: AgentConfig,
) -> anyhow::Result<()> {
    let state = Arc::new(ServerState {
        agent_config,
        secret: config.secret,
        agent_slot: tokio::sync::Mutex::new(AgentSlot::Down),
        boot_gen: AtomicU64::new(0),
    });

    let app = agent_router(state);

    let listener = TcpListener::bind(config.bind_addr).await?;
    info!(
        "Agent server listening on http://{}/ and ws://{}/ws",
        config.bind_addr, config.bind_addr
    );
    info!(
        "Clients should connect with: --remote ws://{}:{}/ws --secret <token>",
        config.bind_addr.ip(),
        config.bind_addr.port()
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod server_tests;
