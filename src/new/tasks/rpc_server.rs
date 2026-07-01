use std::{collections::hash_map, num::NonZeroU64, sync::Arc};

use sequenced_broadcast::SequencedReceiver;
use tokio::sync::{mpsc::Sender, Mutex};

use crate::{
    new::{
        node_state::{NodeState, PeerState},
        subscribable_state::StateHandle,
    },
    protocol::messages::{LeaderWithElectionInfo, SharePeerDetails, SyncRequest, SyncResponse},
    state::{
        determinstic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction},
    },
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

pub struct RpcServer<A: SyncIOAddress, D: DeterministicState> {
    state: Arc<NodeState<A, D>>,
    state_handle: Mutex<StateHandle<D>>,
    actions_tx: Sender<(A, D::Action)>,
}

impl<A: SyncIOAddress, D: DeterministicState> RpcServer<A, D> {
    pub fn new(state: Arc<NodeState<A, D>>, actions_tx: Sender<(A, D::Action)>) -> Self {
        let state_handle = Mutex::new(state.state.create_handle());

        RpcServer {
            state,
            state_handle,
            actions_tx,
        }
    }

    pub async fn handle(&self, peer_addr: A, request: SyncRequest<A, D>) -> ResponseOrFeed<A, D> {
        {
            let mut peers = self.state.peers.lock().await;

            if let Some(state) = peers.get_mut(&peer_addr) {
                state.last_global_connectivity = NonZeroU64::new(now_ms());
            } else {
                tracing::error!("got message from peer but they are not in state");
            }
        }

        let resp = match request {
            SyncRequest::ProtocolVersion(_) => SyncResponse::UnexpectedRequest,
            SyncRequest::MyAddress(_) => SyncResponse::UnexpectedRequest,

            SyncRequest::ShareLeaderPath => {
                let leader = self.state.leader_state.lock().await;
                match &leader.path_to_leader {
                    Some(v) => SyncResponse::LeaderPath(v.clone()),
                    None => SyncResponse::NoPathToLeader,
                }
            }

            SyncRequest::ShareLeaderInfo => {
                let leader = self.state.leader_state.lock().await;
                let peers = self.state.peers.lock().await;
                let recover_details = self.state_handle.lock().await.recover_details();

                SyncResponse::LeaderInfo(LeaderWithElectionInfo {
                    observer: self.state.my_address,
                    term: leader.term,
                    leader: leader.path_to_leader.as_ref().and_then(|v| v.last()).cloned(),
                    leader_path: leader.path_to_leader.clone(),
                    can_lead: self.state.can_lead,
                    reachable_can_lead: peers
                        .iter()
                        .filter_map(|(_, peer)| peer.is_connected.then_some(peer.addr))
                        .collect(),
                    recover_details,
                })
            }
            SyncRequest::Action { source, action } => {
                if self.actions_tx.try_send((source, action)).is_ok() {
                    SyncResponse::Ok
                } else {
                    SyncResponse::FailedToQueueAction { source }
                }
            }
            SyncRequest::LeaderInformation { source, info } => {
                let mut peers = self.state.peers.lock().await;
                match peers.entry(source) {
                    hash_map::Entry::Occupied(o) => {
                        o.into_mut().leader_observation = Some(info);
                    }
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(PeerState {
                            addr: info.observer,
                            latency: None,
                            can_lead: Some(info.can_lead),
                            is_connected: false,
                            last_global_connectivity: NonZeroU64::new(now_ms()),
                            leader_observation: Some(info),
                        });
                    }
                }

                SyncResponse::Ok
            }
            SyncRequest::SubscribeRecovery(details) => match self.state.state.subscribe(details).await {
                Ok(feed) => return ResponseOrFeed::Subscribtion { feed },
                Err(error) => {
                    tracing::warn!(?error, "client recovery failed");
                    SyncResponse::RecoveryFailed
                }
            },
            SyncRequest::SubscribeFresh => {
                let (state, feed) = self.state.state.subscribe_fresh().await;
                return ResponseOrFeed::FreshState { state, feed };
            }
            SyncRequest::Ping(id) => SyncResponse::Pong(id),
            SyncRequest::SharePeers(shared_peers) => {
                let mut peers = self.state.peers.lock().await;

                for peer in shared_peers {
                    match peers.entry(peer.address) {
                        hash_map::Entry::Occupied(o) => {
                            let state = o.into_mut();

                            if let Some(can_lead) = peer.can_be_leader {
                                state.can_lead = Some(can_lead);
                            }

                            state.last_global_connectivity =
                                match (state.last_global_connectivity, peer.last_global_activity) {
                                    (None, Some(a)) | (Some(a), None) => Some(a),
                                    (Some(a), Some(b)) => Some(a.max(b)),
                                    _ => None,
                                };
                        }
                        hash_map::Entry::Vacant(v) => {
                            v.insert(PeerState {
                                addr: peer.address,
                                latency: None,
                                can_lead: peer.can_be_leader,
                                is_connected: false,
                                last_global_connectivity: peer.last_global_activity,
                                leader_observation: None,
                            });
                        }
                    }
                }

                let share_peer_details = peers
                    .iter()
                    .map(|(_, peer)| SharePeerDetails {
                        address: peer.addr,
                        can_be_leader: peer.can_lead,
                        last_global_activity: peer.last_global_connectivity,
                    })
                    .collect::<Vec<_>>();

                SyncResponse::Peers(share_peer_details)
            }
        };

        ResponseOrFeed::Response(resp)
    }
}

enum ResponseOrFeed<A: SyncIOAddress, D: DeterministicState> {
    Response(SyncResponse<A, D>),
    FreshState {
        state: RecoverableState<D>,
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    },
    Subscribtion {
        feed: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    },
}
