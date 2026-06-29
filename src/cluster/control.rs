use std::{
    collections::hash_map::DefaultHasher,
    collections::{hash_map, HashMap},
    hash::Hasher,
    num::NonZeroU64,
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    protocol::messages::{ElectionObservation, LeaderInfoMessage, SharePeerDetails, SyncRequest},
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableStateAction},
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

use super::{
    election::{
        decide_election, valid_remote_leader_path, ElectionDecision, ElectionInput, ElectionState, PeerReachability,
        TimedPeerObservation,
    },
    node::{Inner, NodeDebugInfo, NodeStatus, PeerDebugInfo},
};

pub(crate) struct ControlState<A: SyncIOAddress, D: DeterministicState> {
    pub(crate) leader: LeaderInfo<A>,
    pub(crate) peers: HashMap<A, Option<PeerDetails<A>>>,
    pub(crate) follow: Option<FollowConnection<A, D>>,
    pub(crate) election: ElectionState<A>,
}

#[derive(Clone)]
pub(crate) struct LeaderInfo<A: SyncIOAddress> {
    pub(crate) leader: Option<A>,
    pub(crate) path: Option<Vec<A>>,
    pub(crate) term: u64,
}

#[derive(Clone)]
pub(crate) struct PeerDetails<A: SyncIOAddress> {
    pub(crate) last_activity: Option<NonZeroU64>,
    pub(crate) last_global_activity: Option<NonZeroU64>,
    pub(crate) last_connect_attempt: Option<NonZeroU64>,
    pub(crate) last_connect_fail: Option<NonZeroU64>,
    pub(crate) repeat_connect_fails: u64,
    pub(crate) latency_ms: Option<u64>,
    pub(crate) can_lead: bool,
    pub(crate) connected: bool,
    pub(crate) active_connections: u64,
    pub(crate) last_observation: Option<ElectionObservation<A>>,
}

pub(crate) struct FollowConnection<A: SyncIOAddress, D: DeterministicState> {
    pub(crate) remote: A,
    pub(crate) leader_path: Vec<A>,
    pub(crate) to_peer: mpsc::Sender<SyncRequest<A, D>>,
    pub(crate) cancel: CancellationToken,
}
impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
    pub(crate) fn fail(&self, reason: impl Into<String>) {
        let _ = self.status_updates.send(NodeStatus::Failed { reason: reason.into() });
    }

    pub async fn discover_peers(&self, peers: impl Iterator<Item = impl Into<SharePeerDetails<A>>>) {
        let mut control = self.control.lock().await;

        for peer in peers {
            let details = peer.into();

            if details.address == self.address {
                continue;
            }

            if let Some(can_lead) = details.can_be_leader {
                if can_lead {
                    control.election.known_can_lead.insert(details.address);
                } else {
                    control.election.known_can_lead.remove(&details.address);
                }
            }

            match control.peers.entry(details.address) {
                hash_map::Entry::Vacant(v) => {
                    v.insert(details.can_be_leader.map(|can_lead| PeerDetails {
                        last_activity: None,
                        last_connect_attempt: None,
                        last_connect_fail: None,
                        repeat_connect_fails: 0,
                        latency_ms: None,
                        can_lead,
                        last_global_activity: details.last_global_activity,
                        connected: false,
                        active_connections: 0,
                        last_observation: None,
                    }));
                }
                hash_map::Entry::Occupied(o) => {
                    let value = o.into_mut();

                    if let Some(can_lead) = details.can_be_leader {
                        if let Some(current) = value {
                            current.can_lead = can_lead;
                            if details.last_global_activity > current.last_global_activity {
                                current.last_global_activity = details.last_global_activity;
                            }
                            continue;
                        }

                        value.replace(PeerDetails {
                            last_activity: None,
                            last_connect_attempt: None,
                            last_connect_fail: None,
                            last_global_activity: details.last_global_activity,
                            repeat_connect_fails: 0,
                            latency_ms: None,
                            can_lead,
                            connected: false,
                            active_connections: 0,
                            last_observation: None,
                        });
                    }
                }
            }
        }
    }

    pub(crate) async fn peer_snapshot(&self) -> Vec<SharePeerDetails<A>> {
        let control = self.control.lock().await;

        control
            .peers
            .iter()
            .map(|(address, details)| SharePeerDetails {
                address: *address,
                can_be_leader: details.as_ref().map(|v| v.can_lead),
                last_global_activity: details.as_ref().and_then(|v| v.last_global_activity),
            })
            .collect::<Vec<_>>()
    }

    pub(crate) async fn leader_info_message(&self) -> LeaderInfoMessage<A> {
        let control = self.control.lock().await;
        let leader = control.leader.clone();
        let follow_path = control.follow.as_ref().map(|follow| follow.leader_path.clone());
        let (leader_addr, path) = match leader.leader {
            Some(addr) if addr == self.address => (Some(addr), leader.path),
            Some(addr) => match follow_path {
                Some(path) => (Some(addr), Some(path)),
                None => (None, None),
            },
            None => (None, None),
        };

        LeaderInfoMessage {
            leader: leader_addr,
            path,
            term: leader.term,
        }
    }

    pub(crate) async fn publish_leader_info(&self) {
        let info = self.leader_info_message().await;
        let _ = self.leader_updates.send(info);
    }

    pub(crate) async fn debug_info(&self) -> NodeDebugInfo<A> {
        let now = now_ms();
        let control = self.control.lock().await;
        let leader = control.leader.clone();
        let follow = control
            .follow
            .as_ref()
            .map(|follow| (follow.remote, follow.leader_path.clone()));
        let follow_remote = follow.as_ref().map(|(remote, _)| *remote);

        let mut peer_debug = control
            .peers
            .iter()
            .map(|(address, details)| {
                let observation = details.as_ref().and_then(|details| details.last_observation.clone());
                let connected = details
                    .as_ref()
                    .map(|details| details.connected || Some(*address) == follow_remote);
                PeerDebugInfo {
                    address: *address,
                    known: details.is_some(),
                    can_lead: details.as_ref().map(|details| details.can_lead),
                    connected,
                    latency_ms: details.as_ref().and_then(|details| details.latency_ms),
                    repeat_connect_fails: details.as_ref().map(|details| details.repeat_connect_fails),
                    last_activity_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_activity)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_global_activity_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_global_activity)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_connect_attempt_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_connect_attempt)
                        .map(|ts| now.saturating_sub(ts.get())),
                    last_connect_fail_ms_ago: details
                        .as_ref()
                        .and_then(|details| details.last_connect_fail)
                        .map(|ts| now.saturating_sub(ts.get())),
                    observed_leader: observation.as_ref().and_then(|observation| observation.leader),
                    observed_term: observation.as_ref().map(|observation| observation.term),
                    observed_leader_path: observation
                        .as_ref()
                        .and_then(|observation| observation.leader_path.clone()),
                    observed_reachable_can_lead: observation.map(|observation| observation.reachable_can_lead),
                }
            })
            .collect::<Vec<_>>();
        peer_debug.sort_by_key(|peer| peer.address);

        let mut observations = control.election.observations.values().cloned().collect::<Vec<_>>();
        observations.sort_by_key(|observation| observation.observer);

        NodeDebugInfo {
            address: self.address,
            can_lead: self.can_lead,
            leader: leader.leader,
            leader_path: leader.path,
            term: leader.term,
            follow_remote,
            follow_leader_path: follow.map(|(_, path)| path),
            known_can_lead: control.election.known_can_lead.iter().copied().collect(),
            last_promoted_leader: control.election.last_promoted_leader,
            observations,
            peers: peer_debug,
        }
    }

    pub(crate) async fn local_observation(&self) -> ElectionObservation<A> {
        let (term, leader_addr, leader_path, reachable_can_lead) = {
            let control = self.control.lock().await;
            let leader = control.leader.clone();
            let follow_path = control.follow.as_ref().map(|follow| follow.leader_path.clone());
            let follow_remote = control.follow.as_ref().map(|follow| follow.remote);
            let (leader_addr, leader_path) = match leader.leader {
                Some(addr) if addr == self.address => (Some(addr), leader.path),
                Some(addr) => match follow_path {
                    Some(path) => (Some(addr), Some(path)),
                    None => (None, None),
                },
                None => (None, None),
            };
            let reachable_can_lead = control
                .peers
                .iter()
                .filter_map(|(addr, details)| {
                    let details = details.as_ref()?;
                    if details.can_lead && (details.connected || Some(*addr) == follow_remote || *addr == self.address)
                    {
                        Some(*addr)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            (leader.term, leader_addr, leader_path, reachable_can_lead)
        };
        let state_accept_seq = self.state.lock().await.state_clone().await.state().accept_seq();

        ElectionObservation {
            observer: self.address,
            term,
            leader: leader_addr,
            leader_path,
            can_lead: self.can_lead,
            reachable_can_lead,
            state_accept_seq,
        }
    }

    pub(crate) async fn record_observation(&self, mut observation: ElectionObservation<A>) {
        let mut control = self.control.lock().await;
        if observation.can_lead {
            control.election.known_can_lead.insert(observation.observer);
        }

        if let Some(leader) = observation.leader {
            if leader != observation.observer {
                let has_valid_relay_path = observation
                    .leader_path
                    .as_deref()
                    .map(|path| valid_remote_leader_path(Some(leader), path, observation.observer, self.address))
                    .unwrap_or(false);
                let leader_is_failed = control
                    .peers
                    .get(&leader)
                    .and_then(|details| details.as_ref())
                    .map(|details| {
                        !details.connected && details.last_activity.is_none() && details.last_connect_fail.is_some()
                    })
                    .unwrap_or(false);

                if leader_is_failed && !has_valid_relay_path {
                    observation.leader = None;
                    observation.leader_path = None;
                }
            }
        }

        control.election.term = control.election.term.max(observation.term);
        control
            .election
            .observations
            .insert(observation.observer, observation.clone());

        let details = control
            .peers
            .entry(observation.observer)
            .or_insert_with(|| {
                Some(PeerDetails {
                    last_activity: None,
                    last_connect_attempt: None,
                    last_connect_fail: None,
                    last_global_activity: None,
                    repeat_connect_fails: 0,
                    latency_ms: None,
                    can_lead: observation.can_lead,
                    connected: false,
                    active_connections: 0,
                    last_observation: None,
                })
            })
            .get_or_insert_with(|| PeerDetails {
                last_activity: None,
                last_connect_attempt: None,
                last_connect_fail: None,
                last_global_activity: None,
                repeat_connect_fails: 0,
                latency_ms: None,
                can_lead: observation.can_lead,
                connected: false,
                active_connections: 0,
                last_observation: None,
            });

        details.can_lead = observation.can_lead;
        details.last_activity = NonZeroU64::new(now_ms());
        details.last_observation = Some(observation);
    }

    pub(crate) async fn apply_election(&self) {
        let local_observation = self.local_observation().await;
        let now = now_ms();
        let (known_can_lead, observations, local_term, peer_details) = {
            let control = self.control.lock().await;
            (
                control.election.known_can_lead.clone(),
                control.election.observations.clone(),
                control.election.term,
                control.peers.clone(),
            )
        };
        let max_seen_term = std::iter::once(&local_observation)
            .chain(observations.values())
            .map(|observation| observation.term)
            .max()
            .unwrap_or(local_term);

        self.observe_term(max_seen_term).await;

        let peer_observations = observations
            .into_iter()
            .map(|(observer, observation)| TimedPeerObservation {
                observer,
                last_activity_ms: peer_details
                    .get(&observer)
                    .and_then(|details| details.as_ref())
                    .and_then(|details| details.last_activity)
                    .map(|ts| ts.get()),
                observation,
            })
            .collect::<Vec<_>>();
        let peer_reachability = peer_details
            .iter()
            .map(|(addr, details)| {
                (
                    *addr,
                    PeerReachability {
                        last_activity_ms: details
                            .as_ref()
                            .and_then(|details| details.last_activity)
                            .map(|ts| ts.get()),
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        match decide_election(ElectionInput {
            local_address: self.address,
            can_lead: self.can_lead,
            known_can_lead,
            local_observation,
            peer_observations,
            peer_reachability,
            election_term: local_term,
            now_ms: now,
            stale_after_ms: self.timing.observation_stale_ms(),
        }) {
            ElectionDecision::PromoteSelf { observed_term } => {
                self.promote_if_needed(observed_term).await;
            }
            ElectionDecision::FollowRemote { leader, term, path } => {
                let changed = {
                    let mut control = self.control.lock().await;
                    if control.leader.term < term
                        || control.leader.leader != Some(leader)
                        || control.leader.path != Some(path.clone())
                    {
                        control.leader.term = term;
                        control.leader.leader = Some(leader);
                        control.leader.path = Some(path);
                        true
                    } else {
                        false
                    }
                };
                if changed {
                    self.publish_leader_info().await;
                }
            }
            ElectionDecision::ClearRemoteLeader { leader } => {
                self.clear_remote_leader_if(leader).await;
            }
            ElectionDecision::NoChange => {}
        }
    }

    async fn promote_if_needed(&self, observed_term: u64) {
        {
            let mut control = self.control.lock().await;
            if control.leader.leader == Some(self.address) && control.leader.path.as_deref() == Some(&[self.address]) {
                return;
            }

            let current_leader_term = control.leader.term;
            control.election.term = control
                .election
                .term
                .max(observed_term)
                .max(current_leader_term)
                .saturating_add(1);
            control.election.last_promoted_leader = Some(self.address);
            let new_term = control.election.term;
            control.leader.leader = Some(self.address);
            control.leader.path = Some(vec![self.address]);
            control.leader.term = new_term;
        }
        self.publish_leader_info().await;

        let new_id = generation_id(self.address);
        let mut state = self.state.lock().await;
        if let Err(error) = state
            .apply_authority(RecoverableStateAction::BumpGeneration { new_id })
            .await
        {
            tracing::error!(?error, "failed to bump local authority generation");
            self.fail(format!("failed to bump local authority generation: {error:?}"));
        }
    }

    pub(crate) async fn clear_follow(&self) {
        if let Some(follow) = self.control.lock().await.follow.take() {
            follow.cancel.cancel();
        }
    }

    pub(crate) async fn observe_term(&self, term: u64) {
        let mut control = self.control.lock().await;
        control.election.term = control.election.term.max(term);
    }

    pub(crate) async fn clear_follow_to(&self, remote: A) -> bool {
        let follow = {
            let mut control = self.control.lock().await;
            if control.follow.as_ref().is_some_and(|follow| follow.remote == remote) {
                control.follow.take()
            } else {
                None
            }
        };

        if let Some(follow) = follow {
            follow.cancel.cancel();
            true
        } else {
            false
        }
    }

    async fn clear_remote_leader_if(&self, leader_addr: A) {
        let changed = {
            let mut control = self.control.lock().await;
            if control.leader.leader == Some(leader_addr) && control.leader.leader != Some(self.address) {
                control.leader.leader = None;
                control.leader.path = None;
                true
            } else {
                false
            }
        };
        if changed {
            self.publish_leader_info().await;
        }
    }

    pub(crate) async fn clear_remote_leader(&self) {
        let changed = {
            let mut control = self.control.lock().await;
            if control.leader.leader.is_some() && control.leader.leader != Some(self.address) {
                control.leader.leader = None;
                control.leader.path = None;
                true
            } else {
                false
            }
        };
        if changed {
            self.publish_leader_info().await;
        }
    }

    pub(crate) async fn set_remote_leader_path(
        &self,
        leader_addr: A,
        term: Option<u64>,
        path: Vec<A>,
        follow_remote: Option<A>,
    ) {
        let (changed_leader, changed_follow) = {
            let mut control = self.control.lock().await;
            let leader = &mut control.leader;
            let changed_leader = if let Some(term) = term {
                if leader.term > term {
                    false
                } else {
                    let changed =
                        leader.term != term || leader.leader != Some(leader_addr) || leader.path != Some(path.clone());
                    leader.term = term;
                    leader.leader = Some(leader_addr);
                    leader.path = Some(path.clone());
                    changed
                }
            } else {
                let changed = leader.leader != Some(leader_addr) || leader.path != Some(path.clone());
                leader.leader = Some(leader_addr);
                leader.path = Some(path.clone());
                changed
            };

            let changed_follow = if let Some(follow_remote) = follow_remote {
                if let Some(follow) = control.follow.as_mut().filter(|follow| follow.remote == follow_remote) {
                    if follow.leader_path != path {
                        follow.leader_path = path;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            (changed_leader, changed_follow)
        };

        if changed_leader || changed_follow {
            self.publish_leader_info().await;
        }
        if let Some(term) = term {
            self.observe_term(term).await;
        }
    }
}

static PROMOTION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn generation_id<A: SyncIOAddress>(address: A) -> u64 {
    let mut hasher = DefaultHasher::new();
    address.hash(&mut hasher);
    hasher.finish() ^ now_ms().rotate_left(17) ^ PROMOTION_COUNTER.fetch_add(1, Ordering::SeqCst)
}
