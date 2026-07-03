use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use crate::{
    new::node_state::NodeState, protocol::messages::LeaderState, state::determinstic_state::DeterministicState,
    transport::traits::SyncIOAddress,
};

pub use crate::protocol::messages::LeaderMode;

static GENERATION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct CurrentLeaderTask<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    state: Arc<NodeState<A, D>>,
}

impl<A, D> CurrentLeaderTask<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    pub async fn tick(&mut self) {
        if self.state.can_lead {
            self.tick_as_lead().await;
        } else {
            self.tick_as_node().await;
        }
    }

    async fn tick_as_lead(&self) {
        let concensus = self.get_term_concensus().await;
        let current = self.state.leader_state.lock().await.clone();

        if concensus.term < current.term {
            tracing::warn!(
                concensus_term = concensus.term,
                local_term = current.term,
                "peer concensus term is below current, waiting for update"
            );

            return;
        }

        if current.term < concensus.term {
            let new_state = match concensus.mode {
                LeaderMode::Following { leader } if leader == self.state.my_address => {
                    if self.state.can_lead {
                        tracing::warn!("concensus made me leader and I wasn't involved?? Okay, I accept the crown.");

                        LeaderState::<A> {
                            term: concensus.term,
                            mode: LeaderMode::Leading,
                        }
                    } else {
                        tracing::error!(
                            "concensus made me leader, I wasn't involved, and I'm not elligble. Bad cluster."
                        );

                        LeaderState {
                            term: concensus.term + 1,
                            mode: LeaderMode::NoLeader,
                        }
                    }
                }
                mode => LeaderState {
                    term: concensus.term,
                    mode,
                },
            };

            tracing::info!(
                concensus_term = concensus.term,
                local_term = current.term,
                current_state = ?current,
                ?new_state,
                "new election term, progressing"
            );

            *self.state.leader_state.lock().await = new_state;
            return;
        }

        assert_eq!(current.term, concensus.term);

        let leader_observations = {
            let peers = self.state.peers.lock().await;

            let observations = peers
                .iter()
                .filter(|(_, peer)| peer.can_lead.unwrap_or(false))
                .filter_map(|(_, peer)| peer.leader_info.clone().map(|o| (peer.addr, o)))
                .collect::<Vec<_>>();

            observations
        };

        match current.mode.clone() {
            LeaderMode::Electing { vote: None } | LeaderMode::NoLeader => {
                let mut leader_candidates = {
                    let peers = self.state.peers.lock().await;

                    peers
                        .values()
                        .filter(|p| p.connect_status.is_connected() && p.can_lead.unwrap_or(false))
                        .map(|p| p.addr)
                        .collect::<Vec<_>>()
                };

                /* no peers? vote for self */
                if leader_candidates.is_empty() {
                    let mut lock = self.state.leader_state.lock().await;
                    lock.mode = LeaderMode::Electing {
                        vote: Some(self.state.my_address),
                    };
                    return;
                }

                leader_candidates.push(self.state.my_address);

                let node_votes = {
                    let peers = self.state.peers.lock().await;

                    peers.values().fold(HashMap::<A, u32>::new(), |mut map, peer| {
                        let Some(info) = &peer.leader_info else { return map };
                        if !peer.connect_status.is_connected() {
                            return map;
                        }

                        for (addr, connect_status) in &info.leader_connectivity {
                            if !connect_status.is_connected() {
                                continue;
                            }
                            map.entry(*addr).and_modify(|value| *value += 1).or_insert(1);
                        }

                        map
                    })
                };

                let leader_vote = leader_candidates
                    .into_iter()
                    .max_by_key(|addr| {
                        let votes = node_votes.get(addr).cloned().unwrap_or_default();
                        (votes, *addr)
                    })
                    .unwrap();

                let mut lock = self.state.leader_state.lock().await;
                lock.mode = LeaderMode::Electing {
                    vote: Some(leader_vote),
                };
            }
            LeaderMode::Electing { vote: Some(my_vote) } => {
                let leader_candidates = {
                    let peers = self.state.peers.lock().await;

                    peers
                        .values()
                        .filter(|p| p.connect_status.is_connected())
                        .filter_map(|p| p.leader_info.clone().map(|v| (p.addr, v)))
                        .filter(|(_, info)| info.leader_state.term == current.term)
                        .collect::<Vec<_>>()
                };

                let win_threshold = leader_candidates.len() as u32 / 2;

                let mut votes = leader_candidates
                    .iter()
                    .fold(HashMap::<A, u32>::new(), |mut map, (addr, info)| {
                        let (addr, score) = match info.leader_state.mode {
                            LeaderMode::Leading => (*addr, win_threshold),
                            LeaderMode::Following { leader } => (leader, win_threshold),
                            LeaderMode::Electing { vote: Some(vote) } => (vote, 1),
                            _ => return map,
                        };

                        map.entry(addr)
                            .and_modify(|count| *count = count.saturating_add(score))
                            .or_insert(score);

                        map
                    });

                votes
                    .entry(my_vote)
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);

                let (leader, score) = votes.into_iter().max_by_key(|(_, votes)| *votes).unwrap();
                if win_threshold <= score {
                    let mut lock = self.state.leader_state.lock().await;
                    if leader == self.state.my_address {
                        lock.mode = LeaderMode::Leading;
                    } else {
                        lock.mode = LeaderMode::Following { leader };
                    }
                }
            }
            mode => {
                let my_leader_addr = match mode {
                    LeaderMode::Leading => self.state.my_address,
                    LeaderMode::Following { leader } => leader,
                    _ => unreachable!(),
                };

                let mut term_invalid = false;

                /* make sure we don't have any competing leaders */
                for (addr, obv) in &leader_observations {
                    /* if peer has larger term we'll handle on next tick */
                    if obv.leader_state.term != current.term {
                        continue;
                    }

                    match obv.leader_state.mode {
                        LeaderMode::Following { leader } => {
                            if leader != my_leader_addr {
                                tracing::warn!(peer = ?addr, ?leader, ?my_leader_addr, "Peer is following a different leader (not me)");
                                term_invalid = true;
                            }
                        }
                        LeaderMode::Leading if *addr != my_leader_addr => {
                            tracing::warn!(peer = ?addr, ?my_leader_addr, "Peer considers itself as leadering, but I'm leading");
                            term_invalid = true;
                        }
                        LeaderMode::NoLeader => {
                            tracing::warn!(peer = ?addr, "Peer with can_lead in NoLeader state");
                        }
                        _ => {}
                    }
                }

                if term_invalid {
                    tracing::info!("term invalid, bumping term to start new election");
                    let mut lock = self.state.leader_state.lock().await;
                    lock.term += 1;
                    lock.mode = LeaderMode::Electing { vote: None };
                    return;
                }
            }
        }
    }

    async fn tick_as_node(&self) {
        let concensus = self.get_term_concensus().await;
        let current = self.state.leader_state.lock().await.clone();

        if current != concensus {
            tracing::info!(?concensus, ?current, "Found different leader");

            let mut lock = self.state.leader_state.lock().await;
            *lock = concensus;
        }

        // match current.mode.clone() {
        //     LeaderMode::Leading | LeaderMode::Electing { .. } => {
        //         tracing::error!("non leader in leadering / electing mode");
        //         let mut lock = self.state.leader_state.lock().await;
        //         lock.mode = LeaderMode::NoLeader;
        //     }
        //     LeaderMode::NoLeader => {
        //         let LeaderMode::Following { leader } = concensus.mode else {
        //             return;
        //         };

        //         let mut lock = self.state.leader_state.lock().await;
        //         lock.mode = LeaderMode::Following { leader };
        //     }
        //     LeaderMode::Following { leader } => {
        //         if current.mode != concensus.mode {
        //             tracing::error!(local = ?current.mode, peers = ?concensus.mode, "concensus has different state than me");

        //             if self.state.can_lead {
        //                 let mut lock = self.state.leader_state.lock().await;
        //                 lock.term += 1;
        //                 lock.mode = LeaderMode::Electing { vote: None };
        //             }
        //         }
        //     }
        // }
    }

    async fn get_term_concensus(&self) -> LeaderState<A> {
        let peers = self.state.peers.lock().await;

        let observations = peers
            .iter()
            .filter_map(|(_, peer)| peer.leader_info.clone().map(|o| (peer.addr, o)))
            .collect::<Vec<_>>();

        let Some(peak_term) = observations.iter().map(|(_addr, v)| v.leader_state.term).max() else {
            return self.state.leader_state.lock().await.clone();
        };

        let most_common_mode = observations
            .into_iter()
            .filter(|(_addr, o)| o.leader_state.term == peak_term)
            .map(|(addr, o)| match o.leader_state.mode {
                LeaderMode::Leading => LeaderMode::Following { leader: addr },
                other => other,
            })
            .fold(HashMap::<LeaderMode<A>, u32>::new(), |mut map, mode| {
                map.entry(mode).and_modify(|v| *v += 1).or_insert(1);
                map
            })
            .into_iter()
            .max_by_key(|(_, count)| *count);

        LeaderState {
            term: peak_term,
            mode: most_common_mode.map(|v| v.0).unwrap_or(LeaderMode::NoLeader),
        }
    }
}
