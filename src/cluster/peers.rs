use std::{collections::hash_map, num::NonZeroU64};

use crate::{
    protocol::messages::{ElectionObservation, SharePeerDetails},
    state::deterministic::DeterministicState,
    transport::traits::SyncIOAddress,
    utils::now_ms,
};

use super::{control::PeerDetails, election::valid_remote_leader_path, node::Inner};

impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
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
}
