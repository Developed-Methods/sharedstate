use std::sync::Arc;

use message_encoding::MessageEncoding;
use tokio::task::JoinHandle;

use crate::{
    cluster::{
        election::{ElectionState, NodeTiming},
        inner::{ControlState, FollowConnection, Inner, LeaderInfo, PeerDetails},
        leader::LeaderState,
        messages::{ElectionObservation, LeaderInfoMessage},
        pump::NodeActionSender,
    },
    net::{
        message_channel::NetIoSettings,
        sync_io::{SyncConnection, SyncIO, SyncIOAddress, SyncIOListener},
    },
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::RecoverableState,
        shared_state::{SharedStateHandle, SharedStateReader},
    },
};

use super::{client::ClientWorker, peer::PeerWorker};

use std::collections::{BTreeSet, HashMap};
use tokio::sync::{mpsc, watch, Mutex};

#[derive(Debug)]
pub struct NodeDebugInfo<A: SyncIOAddress> {
    pub address: A,
    pub can_lead: bool,
    pub leader: Option<A>,
    pub leader_path: Option<Vec<A>>,
    pub term: u64,
    pub follow_remote: Option<A>,
    pub follow_leader_path: Option<Vec<A>>,
    pub known_can_lead: Vec<A>,
    pub last_promoted_leader: Option<A>,
    pub observations: Vec<ElectionObservation<A>>,
    pub peers: Vec<PeerDebugInfo<A>>,
}

#[derive(Debug)]
pub struct PeerDebugInfo<A: SyncIOAddress> {
    pub address: A,
    pub known: bool,
    pub can_lead: Option<bool>,
    pub connected: Option<bool>,
    pub latency_ms: Option<u64>,
    pub repeat_connect_fails: Option<u64>,
    pub last_activity_ms_ago: Option<u64>,
    pub last_global_activity_ms_ago: Option<u64>,
    pub last_connect_attempt_ms_ago: Option<u64>,
    pub last_connect_fail_ms_ago: Option<u64>,
    pub observed_leader: Option<A>,
    pub observed_term: Option<u64>,
    pub observed_leader_path: Option<Vec<A>>,
    pub observed_reachable_can_lead: Option<Vec<A>>,
}

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    pub(crate) inner: Arc<Inner<A, D>>,
    io_settings: NetIoSettings,
}

impl<A: SyncIOAddress, D: DeterministicState> NodeState<A, D>
where
    D::Action: Clone,
{
    pub async fn new(address: A, init_state: RecoverableState<D>, can_lead: bool, io_settings: NetIoSettings) -> Self {
        Self::new_with_timing(address, init_state, can_lead, io_settings, NodeTiming::default()).await
    }

    pub async fn new_with_timing(
        address: A,
        init_state: RecoverableState<D>,
        can_lead: bool,
        io_settings: NetIoSettings,
        timing: NodeTiming,
    ) -> Self {
        let (local_actions_tx, local_actions_rx) = mpsc::channel(1024);
        let (leader_updates, _) = watch::channel(LeaderInfoMessage {
            leader: None,
            path: None,
            term: 0,
        });
        let state = LeaderState::new(init_state).await;
        let state_reader = state.state_reader();

        NodeState {
            inner: Arc::new(Inner {
                address,
                can_lead,
                timing,
                control: Mutex::new(ControlState {
                    leader: LeaderInfo {
                        leader: None,
                        path: None,
                        term: 0,
                    },
                    peers: {
                        let mut map = HashMap::new();
                        map.insert(
                            address,
                            Some(PeerDetails {
                                last_activity: None,
                                last_connect_attempt: None,
                                last_connect_fail: None,
                                last_global_activity: None,
                                repeat_connect_fails: 0,
                                latency_ms: Some(0),
                                can_lead,
                                connected: true,
                                active_connections: 0,
                                last_observation: None,
                            }),
                        );
                        map
                    },
                    follow: None::<FollowConnection<A, D>>,
                    election: ElectionState {
                        term: 0,
                        known_can_lead: {
                            let mut set = BTreeSet::new();
                            if can_lead {
                                set.insert(address);
                            }
                            set
                        },
                        observations: HashMap::new(),
                        last_promoted_leader: None,
                    },
                }),
                state: Mutex::new(state),
                state_reader,
                local_actions_tx,
                local_actions_rx: Mutex::new(Some(local_actions_rx)),
                leader_updates,
            }),
            io_settings,
        }
    }

    pub async fn discover_peers(&self, peers: impl Iterator<Item = A>) {
        self.inner.discover_peers(peers).await;
    }

    pub fn state_reader(&self) -> SharedStateReader<RecoverableState<D>> {
        self.inner.state_reader.clone()
    }

    pub fn create_state_handle(&self) -> SharedStateHandle<RecoverableState<D>> {
        self.inner.state_reader.create_handle()
    }

    pub fn action_sender(&self) -> NodeActionSender<D::Action, A> {
        NodeActionSender {
            tx: self.inner.local_actions_tx.clone(),
            leader: self.inner.leader_updates.subscribe(),
        }
    }

    pub async fn debug_info(&self) -> NodeDebugInfo<A> {
        self.inner.debug_info().await
    }

    pub async fn start_client<I>(&self, io: Arc<I>) -> JoinHandle<()>
    where
        I: SyncIO<Address = A>,
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
        self.inner.start_local_action_pump().await;

        tokio::spawn(
            ClientWorker::<I, D> {
                inner: self.inner.clone(),
                io,
                io_settings: self.io_settings.clone(),
            }
            .run(),
        )
    }

    pub async fn start_listener<I>(&self, io: Arc<I>) -> JoinHandle<()>
    where
        I: SyncIOListener<Address = A>,
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
        let inner = self.inner.clone();
        let io_settings = self.io_settings.clone();

        tokio::spawn(async move {
            loop {
                match io.next_client().await {
                    Ok(conn) => {
                        let (transport_addr, write, read) = conn.server_channels(io_settings.clone());
                        tokio::spawn(
                            PeerWorker::<A, D> {
                                transport_addr,
                                node_addr: None,
                                counted_connected: false,
                                inner: inner.clone(),
                                write,
                                read,
                            }
                            .run(),
                        );
                    }
                    Err(error) => {
                        tracing::warn!(?error, "listener stopped accepting clients");
                        break;
                    }
                }
            }
        })
    }

    pub async fn handle_client<I: SyncIO<Address = A>>(&self, conn: SyncConnection<I>) -> JoinHandle<()>
    where
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
        let (transport_addr, write, read) = conn.server_channels(self.io_settings.clone());

        tokio::spawn(
            PeerWorker::<A, D> {
                transport_addr,
                node_addr: None,
                counted_connected: false,
                inner: self.inner.clone(),
                write,
                read,
            }
            .run(),
        )
    }
}
