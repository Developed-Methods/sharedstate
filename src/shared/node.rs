use std::{
    collections::{hash_map, HashMap},
    future::Future,
    num::NonZeroU64,
    panic::Location,
    sync::Arc,
    time::Instant,
};

use message_encoding::MessageEncoding;
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    task::JoinHandle,
};

use crate::{
    net::{
        message_channel::NetIoSettings,
        sync_io::{SyncConnection, SyncIO, SyncIOAddress},
    },
    shared::{
        authorative_state::AuthorativeState,
        messages::{LeaderStatus, SharePeerDetails, SyncRequest, SyncResponse},
    },
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableState},
    utils::now_ms,
};

// pub mod connecting_state;

pub struct NodeState<A: SyncIOAddress, D: DeterministicState> {
    inner: Arc<Inner<A, D>>,
    io_settings: NetIoSettings,
}

struct Inner<A: SyncIOAddress, D: DeterministicState> {
    address: A,
    leader: Mutex<LeaderInfo<A>>,
    peers: Mutex<HashMap<A, Option<PeerDetails>>>,
    state: AuthorativeState<D>,
    actions_tx: broadcast::Sender<(A, D::Action)>,
}

struct LeaderInfo<A: SyncIOAddress> {
    leader: Option<A>,
    path: Option<Vec<A>>,
}

struct PeerDetails {
    last_activity: Option<NonZeroU64>,
    last_global_activity: Option<NonZeroU64>,
    last_connect_fail: Option<NonZeroU64>,
    repeat_connect_fails: u64,
    latency_ms: u64,
    can_lead: bool,
    connected: bool,
}

impl<A: SyncIOAddress, D: DeterministicState> NodeState<A, D>
where
    D::Action: Clone,
{
    pub async fn new(
        address: A,
        init_state: RecoverableState<D>,
        can_lead: bool,
        io_settings: NetIoSettings,
    ) -> Self {
        let (actions_tx, _) = broadcast::channel(2048);

        NodeState {
            inner: Arc::new(Inner {
                address: address.clone(),
                leader: Mutex::new(LeaderInfo {
                    leader: None,
                    path: None,
                }),
                peers: Mutex::new({
                    let mut map = HashMap::new();
                    map.insert(
                        address,
                        Some(PeerDetails {
                            last_activity: None,
                            last_connect_fail: None,
                            last_global_activity: None,
                            repeat_connect_fails: 0,
                            latency_ms: 0,
                            can_lead,
                            connected: true,
                        }),
                    );

                    map
                }),
                state: AuthorativeState::new(init_state).await,
                actions_tx,
            }),
            io_settings,
        }
    }

    pub async fn discover_peers(&self, peers: impl Iterator<Item = A>) {
        self.inner.discover_peers(peers).await;
    }

    pub async fn handle_client<I: SyncIO<Address = A>>(
        &self,
        conn: SyncConnection<I>,
    ) -> JoinHandle<()>
    where
        D: MessageEncoding,
        D::Action: MessageEncoding + Clone,
        D::AuthorityAction: MessageEncoding,
    {
        let (addr, write, read) = conn.server_channels(self.io_settings.clone());

        tokio::spawn(
            PeerWorker::<A, D> {
                addr,
                inner: self.inner.clone(),
                write,
                read,
            }
            .run(),
        )
    }
}

impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
    pub async fn discover_peers(
        &self,
        peers: impl Iterator<Item = impl Into<SharePeerDetails<A>>>,
    ) {
        let mut peers_lock = self.peers.lock().await;

        for peer in peers {
            let details = peer.into();

            /* do not let peer override our own peer details */
            if details.address == self.address {
                continue;
            }

            match peers_lock.entry(details.address) {
                hash_map::Entry::Vacant(v) => {
                    v.insert(details.can_be_leader.map(|can_lead| PeerDetails {
                        last_activity: None,
                        last_connect_fail: None,
                        repeat_connect_fails: 0,
                        latency_ms: 0,
                        can_lead,
                        last_global_activity: None,
                        connected: false,
                    }));
                }
                hash_map::Entry::Occupied(o) => {
                    let value = o.into_mut();

                    if let Some(can_lead) = details.can_be_leader {
                        if let Some(current) = value {
                            current.can_lead = can_lead;
                            continue;
                        }

                        value.replace(PeerDetails {
                            last_activity: None,
                            last_connect_fail: None,
                            last_global_activity: None,
                            repeat_connect_fails: 0,
                            latency_ms: 0,
                            can_lead,
                            connected: false,
                        });
                    }
                }
            }
        }
    }
}

struct PeerWorker<A: SyncIOAddress, D: DeterministicState> {
    addr: A,
    inner: Arc<Inner<A, D>>,
    read: mpsc::Receiver<SyncRequest<A, D>>,
    write: mpsc::Sender<SyncResponse<A, D>>,
}

impl<A: SyncIOAddress, D: DeterministicState> PeerWorker<A, D> {
    async fn run(mut self) {
        while let Some(msg) = self.read.recv().await {
            match msg {
                SyncRequest::Ping(id) => {
                    self.record_activity_ts(true).await;

                    if !self.send(SyncResponse::Pong(id)).await {
                        break;
                    }
                }
                SyncRequest::ProtocolVersion(version) => {
                    if version != 1 {
                        break;
                    }

                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
                SyncRequest::SharePeers(peers) => {
                    self.inner.discover_peers(peers.into_iter()).await;
                    let peers = {
                        let locked = self.inner.peers.lock().await;

                        locked
                            .iter()
                            .map(|(address, details)| SharePeerDetails {
                                address: *address,
                                can_be_leader: details.as_ref().map(|v| v.can_lead),
                                last_global_activity: details
                                    .as_ref()
                                    .and_then(|v| v.last_global_activity),
                            })
                            .collect::<Vec<_>>()
                    };

                    if !self.send(SyncResponse::Peers(peers)).await {
                        break;
                    }
                }
                SyncRequest::SubscribeFresh => {
                    let (state, mut feed) = self.inner.state.subscribe().await;
                    if !self.send(SyncResponse::FreshState(state)).await {
                        break;
                    }

                    let write = self.write.clone();
                    tokio::spawn(async move {
                        while let Ok((seq, action)) = feed.recv().await {
                            if write
                                .send(SyncResponse::AuthorityAction(seq, action))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }

                        let _ = write.send(SyncResponse::ActionStreamClosed).await;
                    });
                }
                SyncRequest::SubscribeRecovery(details) => {
                    let leader_state = self.inner.state.recoverable_state_details().await;

                    if !details.can_recover_follower(&details) {
                        if !self.send(SyncResponse::RecoveryFailed(leader_state)).await {
                            break;
                        }
                        continue;
                    }

                    let Ok(mut feed) = self.inner.state.subscribe_at(details.next_seq()).await
                    else {
                        if !self.send(SyncResponse::RecoveryFailed(leader_state)).await {
                            break;
                        }
                        continue;
                    };

                    if !self.send(SyncResponse::Accepted(feed.next_seq())).await {
                        break;
                    }

                    let write = self.write.clone();
                    tokio::spawn(async move {
                        while let Ok((seq, action)) = feed.recv().await {
                            if write
                                .send(SyncResponse::AuthorityAction(seq, action))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }

                        let _ = write.send(SyncResponse::ActionStreamClosed).await;
                    });
                }
                SyncRequest::MyAddress(addr) => {
                    self.addr = addr;
                    self.record_activity_ts(true).await;
                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
                SyncRequest::ShareLeaderPath => {
                    let path = self.inner.leader.lock().await.path.clone();
                    let resp = match path {
                        None => SyncResponse::NoPathToLeader,
                        Some(path) => SyncResponse::LeaderPath(path),
                    };

                    if !self.send(resp).await {
                        break;
                    }
                }
                SyncRequest::Action { source, action } => {
                    if self.inner.actions_tx.send((source, action)).is_err() {
                        panic!("action tx closed");
                    }
                }
                SyncRequest::LeaderStatus { address, status } => {
                    match status {
                        LeaderStatus::Promoted => {
                            let mut lock = self.inner.leader.lock().await;
                            lock.leader = Some(address);
                        }
                        LeaderStatus::Offline => {}
                        LeaderStatus::Voted => {}
                    }

                    if !self.send(SyncResponse::Ok).await {
                        break;
                    }
                }
            };
        }

        self.record_activity_ts(false).await;
    }

    async fn record_activity_ts(&self, conneted: bool) {
        let now = NonZeroU64::new(now_ms());

        let mut lock = self.inner.peers.lock().await;
        let Some(Some(details)) = lock.get_mut(&self.addr) else {
            return;
        };

        details.last_activity = now;
        details.last_global_activity = now;
        details.connected = conneted;
    }

    #[track_caller]
    fn send(
        &self,
        msg: SyncResponse<A, D>,
    ) -> impl Future<Output = bool> + '_ {
        let caller = Location::caller();

        async {
            if self.write.send(msg).await.is_err() {
                tracing::warn!(
                    "failed to send response to peer {}:{}",
                    caller.file(),
                    caller.line()
                );
                false
            } else {
                true
            }
        }
    }
}
