use std::{
    collections::hash_map::DefaultHasher,
    collections::HashMap,
    hash::Hasher,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    protocol::messages::{ElectionObservation, LeaderInfoMessage},
    state::{deterministic::DeterministicState, recoverable::RecoverableStateAction},
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

use super::{
    election::{decide_election, ElectionDecision, ElectionInput, PeerReachability, TimedPeerObservation},
    node::Inner,
};

impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
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
