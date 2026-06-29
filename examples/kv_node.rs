use std::{
    collections::BTreeMap,
    io::{Error, ErrorKind, Result},
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use message_encoding::MessageEncoding;
use rustyline::{error::ReadlineError, DefaultEditor};
use sharedstate::{
    net::{
        message_channel::NetIoSettings,
        sync_io::{SyncConnection, SyncIO, SyncIOListener},
    },
    shared::node::{NodeActionSender, NodeDebugInfo, NodeState, NodeTiming, SendActionError},
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableState},
};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    port: u16,

    #[arg(long, value_delimiter = ',')]
    peers: Vec<u16>,

    #[arg(long, default_value_t = false)]
    can_lead: bool,

    #[arg(long, default_value_t = 3000)]
    observation_interval_ms: u64,

    #[arg(long, default_value_t = 1000)]
    follow_retry_interval_ms: u64,

    #[arg(long, default_value_t = 15000)]
    observation_stale_ms: u64,

    #[arg(long, default_value_t = 5000)]
    rpc_timeout_ms: u64,
}

#[derive(Clone)]
struct LocalhostIo {
    port: u16,
    listener: Arc<TcpListener>,
}

impl LocalhostIo {
    async fn bind(port: u16) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        Ok(Self {
            port,
            listener: Arc::new(listener),
        })
    }
}

impl SyncIO for LocalhostIo {
    type Address = u16;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;

    async fn connect(&self, remote: &Self::Address) -> Result<SyncConnection<Self>> {
        let stream = TcpStream::connect(("127.0.0.1", *remote)).await?;
        let (read, write) = stream.into_split();

        Ok(SyncConnection {
            remote: *remote,
            read,
            write,
        })
    }
}

impl SyncIOListener for LocalhostIo {
    async fn next_client(&self) -> Result<SyncConnection<Self>> {
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
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Set { key, value } => {
                sum += 1u16.write_to(out)?;
                sum += key.write_to(out)?;
                value.write_to(out)?
            }
            Self::Delete { key } => {
                sum += 2u16.write_to(out)?;
                key.write_to(out)?
            }
        };
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(Self::Set {
                key: MessageEncoding::read_from(read)?,
                value: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(Self::Delete {
                key: MessageEncoding::read_from(read)?,
            }),
            id => Err(Error::new(ErrorKind::InvalidData, format!("unknown KvAction id {id}"))),
        }
    }
}

impl MessageEncoding for KvStore {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
        let mut sum = 0;
        sum += self.seq.write_to(out)?;
        sum += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            sum += key.write_to(out)?;
            sum += value.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
        let seq = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();

        for _ in 0..len {
            values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
        }

        Ok(Self { seq, values })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_writer(std::io::stderr).try_init();
    let args = Args::parse();
    let io = Arc::new(LocalhostIo::bind(args.port).await?);
    let timing = NodeTiming {
        observation_stale_after: Duration::from_millis(args.observation_stale_ms),
        observation_interval: Duration::from_millis(args.observation_interval_ms),
        follow_retry_interval: Duration::from_millis(args.follow_retry_interval_ms),
        rpc_timeout: Duration::from_millis(args.rpc_timeout_ms),
    };
    let node = NodeState::new_with_timing(
        args.port,
        RecoverableState::new(
            args.port as u64,
            KvStore {
                seq: 1,
                values: BTreeMap::new(),
            },
        ),
        args.can_lead,
        NetIoSettings::default(),
        timing,
    )
    .await;

    node.discover_peers(args.peers.iter().copied()).await;
    let _listener = node.start_listener(io.clone()).await;
    let _client = node.start_client(io.clone()).await;

    println!("listening on 127.0.0.1:{} can_lead={} peers={:?}", io.port, args.can_lead, args.peers);
    print_help();

    let actions = node.action_sender();
    let mut handle = node.create_state_handle();
    let mut editor =
        DefaultEditor::new().map_err(|error| Error::other(format!("failed to initialize terminal editor: {error}")))?;

    loop {
        let line = match editor.readline("kv> ") {
            Ok(line) => line,
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(error) => {
                return Err(Error::other(format!("failed to read terminal input: {error}")));
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let _ = editor.add_history_entry(line);

        let mut parts = line.splitn(2, char::is_whitespace);
        let command = parts.next().unwrap_or_default();
        let rest = parts.next().unwrap_or_default().trim();

        match command {
            "set" => handle_set(rest, &actions).await,
            "get" => handle_get(rest, &mut handle),
            "delete" => handle_delete(rest, &actions).await,
            "print" => handle_print(&mut handle),
            "debug" => handle_debug(&node).await,
            "help" => print_help(),
            "quit" | "exit" => break,
            _ => println!("unknown command: {command}"),
        }
    }

    Ok(())
}

async fn handle_set(rest: &str, actions: &NodeActionSender<KvAction, u16>) {
    let mut parts = rest.splitn(2, char::is_whitespace);
    let Some(key) = parts.next().filter(|v| !v.is_empty()) else {
        println!("usage: set <key> <value>");
        return;
    };
    let Some(value) = parts.next().map(str::trim).filter(|v| !v.is_empty()) else {
        println!("usage: set <key> <value>");
        return;
    };

    report_send(
        actions
            .send(KvAction::Set {
                key: key.to_owned(),
                value: value.to_owned(),
            })
            .await,
    );
}

fn handle_get(rest: &str, handle: &mut sharedstate::state::shared_state::SharedStateHandle<RecoverableState<KvStore>>) {
    let key = rest.trim();
    if key.is_empty() || key.split_whitespace().nth(1).is_some() {
        println!("usage: get <key>");
        return;
    }

    let value = {
        let state = handle.read();
        state.state().values.get(key).cloned()
    };
    handle.quiescent();

    match value {
        Some(value) => println!("{value}"),
        None => println!("(missing)"),
    }
}

async fn handle_delete(rest: &str, actions: &NodeActionSender<KvAction, u16>) {
    let key = rest.trim();
    if key.is_empty() || key.split_whitespace().nth(1).is_some() {
        println!("usage: delete <key>");
        return;
    }

    report_send(actions.send(KvAction::Delete { key: key.to_owned() }).await);
}

fn handle_print(handle: &mut sharedstate::state::shared_state::SharedStateHandle<RecoverableState<KvStore>>) {
    let values = {
        let state = handle.read();
        state.state().values.clone()
    };
    handle.quiescent();

    if values.is_empty() {
        println!("(empty)");
        return;
    }

    for (key, value) in values {
        println!("{key}={value}");
    }
}

async fn handle_debug(node: &NodeState<u16, KvStore>) {
    print_debug(node.debug_info().await);
}

fn print_debug(debug: NodeDebugInfo<u16>) {
    println!("node:");
    println!("  address: {}", debug.address);
    println!("  can_lead: {}", debug.can_lead);
    println!("  term: {}", debug.term);
    println!("  leader: {:?}", debug.leader);
    println!("  leader_path: {:?}", debug.leader_path);
    println!("  follow_remote: {:?}", debug.follow_remote);
    println!("  follow_leader_path: {:?}", debug.follow_leader_path);
    println!("  known_can_lead: {:?}", debug.known_can_lead);
    println!("  last_promoted_leader: {:?}", debug.last_promoted_leader);

    println!("peers:");
    if debug.peers.is_empty() {
        println!("  (none)");
    }
    for peer in debug.peers {
        println!(
            "  {} known={} can_lead={:?} connected={:?} latency_ms={:?} fails={:?}",
            peer.address, peer.known, peer.can_lead, peer.connected, peer.latency_ms, peer.repeat_connect_fails
        );
        println!(
            "    activity_ms_ago={:?} global_activity_ms_ago={:?} connect_attempt_ms_ago={:?} connect_fail_ms_ago={:?}",
            peer.last_activity_ms_ago,
            peer.last_global_activity_ms_ago,
            peer.last_connect_attempt_ms_ago,
            peer.last_connect_fail_ms_ago
        );
        println!(
            "    observed_leader={:?} observed_term={:?} observed_path={:?} observed_reachable_can_lead={:?}",
            peer.observed_leader, peer.observed_term, peer.observed_leader_path, peer.observed_reachable_can_lead
        );
    }

    println!("observations:");
    if debug.observations.is_empty() {
        println!("  (none)");
    }
    for observation in debug.observations {
        println!(
            "  observer={} can_lead={} term={} leader={:?} path={:?} reachable_can_lead={:?}",
            observation.observer,
            observation.can_lead,
            observation.term,
            observation.leader,
            observation.leader_path,
            observation.reachable_can_lead
        );
    }
}

fn report_send(result: std::result::Result<(), SendActionError>) {
    match result {
        Ok(()) => println!("queued"),
        Err(SendActionError::NoLeader) => println!("no leader is currently available"),
        Err(SendActionError::Closed) => println!("node action channel is closed"),
    }
}

fn print_help() {
    println!("commands: set <key> <value>, get <key>, delete <key>, print, debug, help, quit");
}
