use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    env,
    io::{self, Error, ErrorKind, Stdout},
    iter,
    sync::Arc,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use message_encoding::MessageEncoding;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
    Terminal,
};
use sequenced_broadcast::SequencedBroadcastSettings;
use sharedstate::{
    new::{
        node_state::{NodeState, PeerState},
        subscribable_state::StateHandle,
        tasks::{
            current_leader::{CurrentLeaderStatus, CurrentLeaderTask, CurrentLeaderTiming, LeaderMode},
            peer_connections::PeerConnections,
            peer_discovery::{PeerDiscoveryTask, PeerDiscoveryTiming},
            rpc_server::RpcServer,
        },
    },
    protocol::messages::{SyncRequest, SyncResponse, PROTOCOL_VERSION},
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::{
        channels::NetIoSettings,
        traits::{SyncConnection, SyncIO, SyncIOListener},
    },
};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};

const COMMAND_HELP: &str =
    "commands: help, status, peers, get <key>, set <key> <value>, delete|del|rm <key>, list|print, quit|exit";
const LOG_LIMIT: usize = 250;
const RENDER_INTERVAL: Duration = Duration::from_millis(100);
const SUBSCRIBE_RETRY_DELAY: Duration = Duration::from_millis(500);

#[derive(Debug)]
struct Args {
    can_lead: bool,
    peers: Vec<u16>,
}

#[derive(Clone)]
struct LocalhostTcpIo {
    address: u16,
    listener: Arc<TcpListener>,
}

impl LocalhostTcpIo {
    async fn bind_ephemeral() -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?.port();
        Ok(Self {
            address,
            listener: Arc::new(listener),
        })
    }
}

impl SyncIO for LocalhostTcpIo {
    type Address = u16;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;

    async fn connect(&self, remote: &Self::Address) -> io::Result<SyncConnection<Self>> {
        let stream = TcpStream::connect(("127.0.0.1", *remote)).await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection {
            remote: *remote,
            read,
            write,
        })
    }
}

impl SyncIOListener for LocalhostTcpIo {
    async fn next_client(&self) -> io::Result<SyncConnection<Self>> {
        let (stream, peer) = self.listener.accept().await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection {
            remote: peer.port(),
            read,
            write,
        })
    }
}

#[derive(Clone, Debug)]
struct KvStore {
    seq: u64,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
enum KvAction {
    Set { key: String, value: String },
    Delete { key: String },
}

impl KvStore {
    fn new() -> Self {
        Self {
            seq: 1,
            values: BTreeMap::new(),
        }
    }
}

impl DeterministicState for KvStore {
    type Action = KvAction;
    type AuthorityAction = KvAction;

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        match action {
            KvAction::Set { key, value } => {
                self.values.insert(key.clone(), value.clone());
            }
            KvAction::Delete { key } => {
                self.values.remove(key);
            }
        }
        self.seq += 1;
    }
}

impl MessageEncoding for KvAction {
    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            KvAction::Set { key, value } => {
                sum += 1u16.write_to(out)?;
                sum += key.write_to(out)?;
                value.write_to(out)?
            }
            KvAction::Delete { key } => {
                sum += 2u16.write_to(out)?;
                key.write_to(out)?
            }
        };
        Ok(sum)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(KvAction::Set {
                key: MessageEncoding::read_from(read)?,
                value: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(KvAction::Delete {
                key: MessageEncoding::read_from(read)?,
            }),
            id => Err(Error::new(ErrorKind::InvalidData, format!("unknown KvAction id {id}"))),
        }
    }
}

impl MessageEncoding for KvStore {
    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        let mut sum = 0;
        sum += self.seq.write_to(out)?;
        sum += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            sum += key.write_to(out)?;
            sum += value.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        let seq = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
        }
        Ok(Self { seq, values })
    }
}

struct App {
    input: String,
    logs: VecDeque<String>,
    should_quit: bool,
}

impl App {
    fn new() -> Self {
        Self {
            input: String::new(),
            logs: VecDeque::new(),
            should_quit: false,
        }
    }

    fn log(&mut self, message: impl Into<String>) {
        self.logs.push_back(message.into());
        while self.logs.len() > LOG_LIMIT {
            self.logs.pop_front();
        }
    }
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let _ = tracing_subscriber::fmt().with_writer(io::stderr).try_init();
    let args = parse_args()?;
    let io = Arc::new(LocalhostTcpIo::bind_ephemeral().await?);
    let local_address = io.address;
    let settings = NetIoSettings::default();

    let initial_peers = args
        .peers
        .iter()
        .copied()
        .filter(|peer| *peer != local_address)
        .map(|peer| {
            (
                peer,
                PeerState {
                    addr: peer,
                    latency: None,
                    can_lead: None,
                    is_connected: false,
                    last_global_connectivity: None,
                    leader_observation: None,
                },
            )
        })
        .collect::<HashMap<_, _>>();

    let state = Arc::new(NodeState {
        my_address: local_address,
        can_lead: args.can_lead,
        peers: Mutex::new(initial_peers),
        state: sharedstate::new::subscribable_state::SubscribableState::new(
            RecoverableState::new(local_address as u64, KvStore::new()),
            SequencedBroadcastSettings::default(),
        )
        .map_err(|error| Error::other(format!("failed to create state broadcast: {error:?}")))?,
        leader_status: Arc::new(CurrentLeaderStatus::new(local_address)),
    });

    let peer_connections = Arc::new(PeerConnections::new(io.clone(), settings.clone(), state.clone()));
    let (actions_tx, actions_rx) = mpsc::channel(512);
    let rpc_server = Arc::new(RpcServer::new(state.clone(), actions_tx.clone()));
    let _server_task = rpc_server.start_listener(io.clone(), settings.clone());

    tokio::spawn(PeerDiscoveryTask::new(state.clone(), peer_connections.clone(), PeerDiscoveryTiming::default()).run());
    tokio::spawn(CurrentLeaderTask::new(state.clone(), CurrentLeaderTiming::default()).run());

    let (log_tx, log_rx) = mpsc::unbounded_channel();
    start_action_router(state.clone(), peer_connections.clone(), actions_rx, log_tx.clone());
    start_follower_subscription(state.clone(), io.clone(), settings.clone(), log_tx.clone());

    let _ = log_tx.send(format!(
        "listening on 127.0.0.1:{local_address} can_lead={} initial_peers={:?}",
        args.can_lead, args.peers
    ));
    let _ = log_tx.send(COMMAND_HELP.to_owned());

    let mut state_handle = state.state.create_handle();
    run_tui(state, actions_tx, log_rx, &mut state_handle).await
}

fn parse_args() -> io::Result<Args> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 && args.len() != 2 {
        return Err(usage_error());
    }

    let can_lead = match args.remove(0).to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => true,
        "false" | "0" | "no" => false,
        _ => return Err(usage_error()),
    };

    let peers = if let Some(raw) = args.pop() {
        if raw.is_empty() {
            return Err(usage_error());
        }
        raw.split(',')
            .map(|part| {
                if part.is_empty() {
                    return Err(usage_error());
                }
                part.parse::<u16>().map_err(|_| usage_error())
            })
            .collect::<io::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(Args { can_lead, peers })
}

fn usage_error() -> io::Error {
    Error::new(ErrorKind::InvalidInput, "usage: cargo run --example kv_tui -- <can_lead:true|false> [peer_ports_csv]")
}

fn start_action_router(
    state: Arc<NodeState<u16, KvStore>>,
    peer_connections: Arc<PeerConnections<LocalhostTcpIo, KvStore>>,
    mut actions_rx: mpsc::Receiver<(u16, KvAction)>,
    log_tx: UnboundedSender<String>,
) {
    tokio::spawn(async move {
        while let Some((source, action)) = actions_rx.recv().await {
            match state.leader_status.leader().await {
                Some(leader) if leader == state.my_address => {
                    state
                        .state
                        .update(iter::once(RecoverableStateAction::StateAction { action }))
                        .await;
                    let _ = log_tx.send(format!("applied action locally from {source}"));
                }
                Some(leader) => {
                    let result = peer_connections
                        .send_rpc(leader, SyncRequest::Action { source, action })
                        .await;
                    match result {
                        Ok(SyncResponse::Ok) => {
                            let _ = log_tx.send(format!("forwarded action from {source} to leader {leader}"));
                        }
                        Ok(response) => {
                            let _ = log_tx.send(format!(
                                "leader {leader} returned unexpected action response {}",
                                response.name()
                            ));
                        }
                        Err(error) => {
                            let _ = log_tx.send(format!("failed to forward action to leader {leader}: {error:?}"));
                        }
                    }
                }
                None => {
                    let _ = log_tx.send("no leader available; action dropped".to_owned());
                }
            }
        }
    });
}

fn start_follower_subscription(
    state: Arc<NodeState<u16, KvStore>>,
    io: Arc<LocalhostTcpIo>,
    settings: NetIoSettings,
    log_tx: UnboundedSender<String>,
) {
    tokio::spawn(async move {
        loop {
            let Some(leader) = state.leader_status.leader().await else {
                tokio::time::sleep(SUBSCRIBE_RETRY_DELAY).await;
                continue;
            };
            if leader == state.my_address {
                tokio::time::sleep(SUBSCRIBE_RETRY_DELAY).await;
                continue;
            }

            let _ = log_tx.send(format!("subscribing fresh from leader {leader}"));
            match subscribe_to_leader(&state, &io, settings.clone(), leader, &log_tx).await {
                Ok(()) => {
                    let _ = log_tx.send(format!("subscription to leader {leader} closed"));
                }
                Err(error) => {
                    let _ = log_tx.send(format!("subscription to leader {leader} failed: {error}"));
                }
            }

            tokio::time::sleep(SUBSCRIBE_RETRY_DELAY).await;
        }
    });
}

async fn subscribe_to_leader(
    state: &Arc<NodeState<u16, KvStore>>,
    io: &Arc<LocalhostTcpIo>,
    settings: NetIoSettings,
    leader: u16,
    log_tx: &UnboundedSender<String>,
) -> io::Result<()> {
    let connection = io.connect(&leader).await?;
    let (_remote, write, mut read) = connection.client_channels::<KvStore>(settings.clone());

    write
        .send(SyncRequest::ProtocolVersion(PROTOCOL_VERSION))
        .await
        .map_err(|_| Error::new(ErrorKind::BrokenPipe, "failed to send protocol version"))?;
    require_ok(read.recv().await, "protocol version")?;

    write
        .send(SyncRequest::MyAddress(state.my_address))
        .await
        .map_err(|_| Error::new(ErrorKind::BrokenPipe, "failed to send local address"))?;
    require_ok(read.recv().await, "my address")?;

    write
        .send(SyncRequest::SubscribeFresh)
        .await
        .map_err(|_| Error::new(ErrorKind::BrokenPipe, "failed to send fresh subscription request"))?;

    match read.recv().await {
        Some(SyncResponse::FreshState(fresh)) => {
            let next_seq = fresh.details().next_seq();
            state.state.reset(fresh).await;
            let _ = log_tx.send(format!("loaded fresh state from leader {leader} next_seq={next_seq}"));
        }
        Some(response) => {
            return Err(Error::new(ErrorKind::InvalidData, format!("expected FreshState, got {}", response.name())));
        }
        None => return Err(Error::new(ErrorKind::UnexpectedEof, "subscription closed before fresh state")),
    }

    while state.leader_status.leader().await == Some(leader) {
        match read.recv().await {
            Some(SyncResponse::AuthorityAction(seq, action)) => {
                state.state.update(iter::once(action)).await;
                let _ = log_tx.send(format!("applied streamed authority action seq={seq} from leader {leader}"));
            }
            Some(SyncResponse::ActionStreamClosed) => break,
            Some(response) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected stream response {}", response.name()),
                ));
            }
            None => return Err(Error::new(ErrorKind::UnexpectedEof, "subscription stream closed")),
        }
    }

    Ok(())
}

fn require_ok(response: Option<SyncResponse<u16, KvStore>>, step: &'static str) -> io::Result<()> {
    match response {
        Some(SyncResponse::Ok) => Ok(()),
        Some(response) => {
            Err(Error::new(ErrorKind::InvalidData, format!("expected Ok during {step}, got {}", response.name())))
        }
        None => Err(Error::new(ErrorKind::UnexpectedEof, format!("connection closed during {step}"))),
    }
}

async fn run_tui(
    state: Arc<NodeState<u16, KvStore>>,
    actions_tx: mpsc::Sender<(u16, KvAction)>,
    mut log_rx: UnboundedReceiver<String>,
    state_handle: &mut StateHandle<KvStore>,
) -> io::Result<()> {
    let mut terminal = TerminalGuard::enter()?;
    let mut app = App::new();
    let mut last_render = Instant::now();

    loop {
        while let Ok(message) = log_rx.try_recv() {
            app.log(message);
        }

        if last_render.elapsed() >= RENDER_INTERVAL {
            let summary = build_summary(&state, state_handle).await;
            terminal.terminal.draw(|frame| render(frame, &app, summary.clone()))?;
            last_render = Instant::now();
        }

        if event::poll(Duration::from_millis(20))? {
            if let Event::Key(key) = event::read()? {
                handle_key(key, &mut app, &state, &actions_tx, state_handle).await;
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

fn render(frame: &mut ratatui::Frame<'_>, app: &App, summary: Vec<String>) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(frame.area());

    let summary_lines = summary.into_iter().map(Line::from).collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(summary_lines)
            .block(Block::default().title("Node").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        chunks[0],
    );

    let shell_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(chunks[1]);
    let log_items = app
        .logs
        .iter()
        .rev()
        .take(shell_chunks[0].height.saturating_sub(2) as usize)
        .rev()
        .map(|line| ListItem::new(Line::from(line.clone())))
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(log_items).block(Block::default().title("Shell").borders(Borders::ALL)),
        shell_chunks[0],
    );

    let prompt = Line::from(vec![
        Span::styled("kv> ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::raw(app.input.clone()),
    ]);
    frame.render_widget(
        Paragraph::new(prompt).block(Block::default().title("Input").borders(Borders::ALL)),
        shell_chunks[1],
    );
}

async fn build_summary(state: &Arc<NodeState<u16, KvStore>>, state_handle: &mut StateHandle<KvStore>) -> Vec<String> {
    let snapshot = state.leader_status.snapshot().await;
    let (seq, item_count, values_preview) = state_handle.read_with(|recoverable| {
        let store = recoverable.state();
        let preview = store
            .values
            .iter()
            .take(6)
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        (store.seq, store.values.len(), preview)
    });
    let peers = state.peers.lock().await;

    let mut lines = vec![
        format!("address: 127.0.0.1:{}  can_lead: {}", state.my_address, state.can_lead),
        format!("leader: {}", leader_mode_line(&snapshot.mode)),
        format!("kv: seq={seq} items={item_count}"),
    ];
    if !values_preview.is_empty() {
        lines.push(format!("kv preview: {}", values_preview.join(", ")));
    }
    lines.push("peers:".to_owned());

    if peers.is_empty() {
        lines.push("  (none)".to_owned());
    } else {
        for peer in peers.values() {
            lines.push(format!(
                "  {} connected={} can_lead={:?} latency_ms={:?} last_global={:?} observed_leader={:?} observed_term={:?}",
                peer.addr,
                peer.is_connected,
                peer.can_lead,
                peer.latency.map(|latency| latency.get()),
                peer.last_global_connectivity.map(|value| value.get()),
                peer.leader_observation.as_ref().and_then(|info| info.leader),
                peer.leader_observation.as_ref().map(|info| info.term),
            ));
        }
    }

    lines
}

fn leader_mode_line(mode: &LeaderMode<u16>) -> String {
    match mode {
        LeaderMode::NoLeader { term } => format!("NoLeader term={term}"),
        LeaderMode::Electing { term } => format!("Electing term={term}"),
        LeaderMode::Leading { term, path } => format!("Leading term={term} path={path:?}"),
        LeaderMode::Following {
            term,
            leader,
            path,
            via,
        } => {
            format!("Following term={term} leader={leader} via={via} path={path:?}")
        }
    }
}

async fn handle_key(
    key: KeyEvent,
    app: &mut App,
    state: &Arc<NodeState<u16, KvStore>>,
    actions_tx: &mpsc::Sender<(u16, KvAction)>,
    state_handle: &mut StateHandle<KvStore>,
) {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true;
        }
        KeyCode::Esc => app.should_quit = true,
        KeyCode::Backspace => {
            app.input.pop();
        }
        KeyCode::Enter => {
            let command = app.input.trim().to_owned();
            app.input.clear();
            if !command.is_empty() {
                app.log(format!("kv> {command}"));
                run_command(command, app, state, actions_tx, state_handle).await;
            }
        }
        KeyCode::Char(ch) => {
            app.input.push(ch);
        }
        _ => {}
    }
}

async fn run_command(
    command: String,
    app: &mut App,
    state: &Arc<NodeState<u16, KvStore>>,
    actions_tx: &mpsc::Sender<(u16, KvAction)>,
    state_handle: &mut StateHandle<KvStore>,
) {
    let mut parts = command.splitn(2, char::is_whitespace);
    let name = parts.next().unwrap_or_default();
    let rest = parts.next().unwrap_or_default().trim();

    match name {
        "help" => app.log(COMMAND_HELP),
        "quit" | "exit" => app.should_quit = true,
        "status" => {
            let snapshot = state.leader_status.snapshot().await;
            let peer_count = state.peers.lock().await.len();
            app.log(format!(
                "address={} can_lead={} leader={} peers={peer_count}",
                state.my_address,
                state.can_lead,
                leader_mode_line(&snapshot.mode)
            ));
        }
        "peers" => {
            let peers = state.peers.lock().await;
            if peers.is_empty() {
                app.log("(no peers)");
            } else {
                for peer in peers.values() {
                    app.log(format!(
                        "{} connected={} can_lead={:?} latency_ms={:?} observed_leader={:?}",
                        peer.addr,
                        peer.is_connected,
                        peer.can_lead,
                        peer.latency.map(|latency| latency.get()),
                        peer.leader_observation.as_ref().and_then(|info| info.leader),
                    ));
                }
            }
        }
        "get" => handle_get(rest, app, state_handle),
        "set" => handle_set(rest, app, state.my_address, actions_tx).await,
        "delete" | "del" | "rm" => handle_delete(rest, app, state.my_address, actions_tx).await,
        "list" | "print" => handle_list(app, state_handle),
        _ => app.log(format!("unknown command: {name}")),
    }
}

fn handle_get(rest: &str, app: &mut App, state_handle: &mut StateHandle<KvStore>) {
    let key = rest.trim();
    if key.is_empty() || key.split_whitespace().nth(1).is_some() {
        app.log("usage: get <key>");
        return;
    }

    let value = state_handle.read_with(|state| state.state().values.get(key).cloned());
    match value {
        Some(value) => app.log(format!("{key}={value}")),
        None => app.log(format!("{key}=(missing)")),
    }
}

async fn handle_set(rest: &str, app: &mut App, source: u16, actions_tx: &mpsc::Sender<(u16, KvAction)>) {
    let mut parts = rest.splitn(2, char::is_whitespace);
    let Some(key) = parts.next().filter(|value| !value.is_empty()) else {
        app.log("usage: set <key> <value>");
        return;
    };
    let Some(value) = parts.next().map(str::trim).filter(|value| !value.is_empty()) else {
        app.log("usage: set <key> <value>");
        return;
    };

    match actions_tx
        .send((
            source,
            KvAction::Set {
                key: key.to_owned(),
                value: value.to_owned(),
            },
        ))
        .await
    {
        Ok(()) => app.log(format!("queued set {key}")),
        Err(error) => app.log(format!("failed to queue set: {error}")),
    }
}

async fn handle_delete(rest: &str, app: &mut App, source: u16, actions_tx: &mpsc::Sender<(u16, KvAction)>) {
    let key = rest.trim();
    if key.is_empty() || key.split_whitespace().nth(1).is_some() {
        app.log("usage: delete <key>");
        return;
    }

    match actions_tx
        .send((source, KvAction::Delete { key: key.to_owned() }))
        .await
    {
        Ok(()) => app.log(format!("queued delete {key}")),
        Err(error) => app.log(format!("failed to queue delete: {error}")),
    }
}

fn handle_list(app: &mut App, state_handle: &mut StateHandle<KvStore>) {
    let values = state_handle.read_with(|state| state.state().values.clone());
    if values.is_empty() {
        app.log("(empty)");
        return;
    }

    for (key, value) in values {
        app.log(format!("{key}={value}"));
    }
}
