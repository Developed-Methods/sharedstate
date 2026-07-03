use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    cluster::node_state::{ConnectStatus, NodeState, PeerState},
    protocol::messages::{ElectionTerm, LeaderState},
    state::{deterministic_state::DeterministicState, recoverable_state::RecoverableStateDetails},
    transport::traits::SyncIOAddress, utils::now_ms,
};

pub use crate::protocol::messages::LeaderMode;

#[derive(Clone, Debug)]
pub struct LeaderTiming {
    pub tick_interval: Duration,
}

impl Default for LeaderTiming {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(250),
        }
    }
}

pub struct LeaderTask<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    state: Arc<NodeState<A, D>>,
    timing: LeaderTiming,
}

impl<A, D> LeaderTask<A, D>
where
    A: SyncIOAddress,
    D: DeterministicState,
{
    pub fn new(state: Arc<NodeState<A, D>>, timing: LeaderTiming) -> Self {
        Self { state, timing }
    }

    pub async fn run(mut self) {
        loop {
            self.tick().await;
            tokio::time::sleep(self.timing.tick_interval).await;
        }
    }

    pub async fn tick(&mut self) {
        let peer_views = {
            let peers = self.state.peers.lock().await;
            let online_peers_with_leader_info = peers.values()
                .filter(|p| p.connect_status.is_connected()).filter_map(|v| v.leader_info.clone());
            
            let (peer_count, connection_table) = online_peers_with_leader_info.fold((0u32, HashMap::<A, u32>::new()), |(count, mut table), peer| {
                for addr in &peer.reachable_voters {
                    table.entry(*addr).and_modify(|v| *v += 1).or_insert(1);
                }
                (count + 1, table)
            });

            let offline_threshold = peer_count / 3;

            peers.values()
                .filter(|peer| {
                    let connection_count = connection_table.get(&peer.addr).cloned().unwrap_or(0);
                    offline_threshold < connection_count
                })
                .map(PeerView::new)
                .collect::<Vec<_>>()
        };

        let mut leader_state = self.state.leader_state.lock().await;

        let next = if self.state.can_lead {
            next_voter_state(self.state.my_address, &leader_state, &peer_views)
        } else {
            next_observer_state(&leader_state, &peer_views)
        };

        if *leader_state != next {
            tracing::info!(previous = ?*leader_state, ?next, "leader state updated");
            *leader_state = next;
        }
    }
}

#[derive(Clone, Debug)]
struct PeerView<A: SyncIOAddress> {
    addr: A,
    is_voter: bool,
    connected: bool,
    unreachable: bool,
    leader_state: Option<LeaderState<A>>,
    recovery_details: Option<RecoverableStateDetails>,
}

impl<A: SyncIOAddress> PeerView<A> {
    fn new(peer: &PeerState<A>) -> Self {
        let unreachable = matches!(peer.connect_status, ConnectStatus::FailedToConnect { .. });
        let known_can_lead = peer
            .can_lead
            .or(peer.leader_info.as_ref().map(|info| info.can_lead));

        Self {
            addr: peer.addr,
            /* until we learn a peer's can_lead, assume it votes so we don't
             * claim leadership before discovery settles; unreachable peers
             * with unknown status are excluded so they can't block elections */
            is_voter: known_can_lead.unwrap_or(!unreachable),
            connected: peer.connect_status.is_connected(),
            unreachable,
            leader_state: peer.leader_info.as_ref().map(|info| info.leader_state.clone()),
            recovery_details: peer.leader_info.as_ref().map(|info| info.recovery_details.clone()),
        }
    }

    fn mode_at_term(&self, term: ElectionTerm) -> Option<&LeaderMode<A>> {
        self.leader_state
            .as_ref()
            .filter(|state| state.term == term)
            .map(|state| &state.mode)
    }
}

/// Decides the next leader state for a node that can lead.
///
/// Convergence comes from three deterministic rules that every voter applies
/// to the same gossiped data:
///  - the election term only moves forward, everyone adopts the highest seen
///  - a voter votes for the lowest address it can reach, so votes can't tie
///  - leadership requires a strict majority of the known voter set, and
///    conflicting claims in one term resolve to the lowest address without
///    bumping the term (term bumps are reserved for leader failure)
fn next_voter_state<A: SyncIOAddress>(me: A, current: &LeaderState<A>, peers: &[PeerView<A>]) -> LeaderState<A> {
    let voters = || peers.iter().filter(|peer| peer.is_voter);

    let term = voters()
        .filter_map(|peer| peer.leader_state.as_ref().map(|state| state.term))
        .chain(std::iter::once(current.term))
        .max_by_key(ElectionTerm::id)
        .unwrap();

    let mode = if term == current.term {
        current.mode.clone()
    } else {
        LeaderMode::Electing { vote: None }
    };

    let voter_count = voters().count() + 1;
    let has_majority = |supporters: usize| voter_count < supporters * 2;

    /* a Leading claim at the current term is authoritative (it required a
     * majority); if partitions merge with two claims, the lowest address wins */
    let peer_claim = voters()
        .filter(|peer| peer.connected)
        .filter(|peer| matches!(peer.mode_at_term(term), Some(LeaderMode::Leading)))
        .map(|peer| (peer.recovery_details.as_ref().unwrap().next_seq(), peer.addr))
        .max();

    match mode {
        LeaderMode::Leading => {
            let reachable_count = voters().filter(|peer| !peer.unreachable).count() + 1;
            if !has_majority(reachable_count) {
                tracing::warn!(
                    %term,
                    voter_count,
                    reachable_count,
                    "lost contact with voter majority, stepping down"
                );
                return LeaderState {
                    term: term.bump(),
                    mode: LeaderMode::Electing { vote: None },
                };
            }

            match peer_claim {
                Some(leader) if leader < me => {
                    tracing::warn!(?leader, %term, "conceding to lower address leading the same term");
                    LeaderState {
                        term,
                        mode: LeaderMode::Following { leader },
                    }
                }
                _ => LeaderState {
                    term,
                    mode: LeaderMode::Leading,
                },
            }
        }
        LeaderMode::Following { leader } => {
            if let Some(claim) = peer_claim {
                return LeaderState {
                    term,
                    mode: LeaderMode::Following { leader: claim },
                };
            }

            let leader_view = peers.iter().find(|peer| peer.addr == leader);

            if leader_view.map(|peer| peer.unreachable).unwrap_or(true) {
                tracing::warn!(?leader, %term, "leader is unreachable, starting a new election");
                return LeaderState {
                    term: term.bump(),
                    mode: LeaderMode::Electing { vote: None },
                };
            }

            match leader_view.and_then(|peer| peer.mode_at_term(term)) {
                /* leader conceded to someone else this term, go with it */
                Some(LeaderMode::Following { leader: new_leader }) => LeaderState {
                    term,
                    mode: LeaderMode::Following { leader: *new_leader },
                },
                /* leader gave up its claim (e.g. restarted and re-joined the
                 * term), rejoin the election so votes can settle */
                Some(LeaderMode::Electing { .. }) | Some(LeaderMode::NoLeader) => LeaderState {
                    term,
                    mode: LeaderMode::Electing { vote: None },
                },
                /* still leading, or we only have stale info from another term */
                Some(LeaderMode::Leading) | None => LeaderState {
                    term,
                    mode: LeaderMode::Following { leader },
                },
            }
        }
        LeaderMode::Electing { .. } | LeaderMode::NoLeader => {
            if let Some(leader) = peer_claim {
                return LeaderState {
                    term,
                    mode: LeaderMode::Following { leader },
                };
            }

            let vote = voters()
                .filter(|peer| peer.connected)
                .map(|peer| peer.addr)
                .chain([me])
                .min()
                .expect("candidates always include self");

            if vote == me {
                let support = 1 + voters()
                    .filter(|peer| peer.connected)
                    .filter(|peer| match peer.mode_at_term(term) {
                        Some(LeaderMode::Electing { vote: Some(vote) }) => *vote == me,
                        Some(LeaderMode::Following { leader }) => *leader == me,
                        _ => false,
                    })
                    .count();

                if has_majority(support) {
                    tracing::info!(term, support, voter_count, "won election with voter majority");
                    return LeaderState {
                        term,
                        mode: LeaderMode::Leading,
                    };
                }
            }

            LeaderState {
                term,
                mode: LeaderMode::Electing { vote: Some(vote) },
            }
        }
    }
}

/// Decides the next leader state for a node that cannot lead. Observers never
/// vote; they mirror what the connected voters report, preferring a direct
/// Leading claim and falling back to the most-followed address.
fn next_observer_state<A: SyncIOAddress>(current: &LeaderState<A>, peers: &[PeerView<A>]) -> LeaderState<A> {
    let voter_states = peers
        .iter()
        .filter(|peer| peer.is_voter && peer.connected)
        .filter_map(|peer| peer.leader_state.as_ref().map(|state| (peer.addr, state)))
        .collect::<Vec<_>>();

    let Some(term) = voter_states.iter().map(|(_, state)| state.term).max() else {
        return current.clone();
    };

    let claimed = voter_states
        .iter()
        .filter(|(_, state)| state.term == term && matches!(state.mode, LeaderMode::Leading))
        .map(|(addr, _)| *addr)
        .min();

    let followed = voter_states
        .iter()
        .filter(|(_, state)| state.term == term)
        .filter_map(|(_, state)| match &state.mode {
            LeaderMode::Following { leader } => Some(*leader),
            _ => None,
        })
        .fold(HashMap::<A, usize>::new(), |mut counts, leader| {
            *counts.entry(leader).or_default() += 1;
            counts
        })
        .into_iter()
        .max_by_key(|(addr, count)| (*count, std::cmp::Reverse(*addr)))
        .map(|(addr, _)| addr);

    let mode = match claimed.or(followed) {
        Some(leader) => LeaderMode::Following { leader },
        None => LeaderMode::NoLeader,
    };

    LeaderState { term, mode }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ls(term: u64, mode: LeaderMode<u16>) -> LeaderState<u16> {
        LeaderState { term, mode }
    }

    fn voter(addr: u16, state: Option<LeaderState<u16>>) -> PeerView<u16> {
        PeerView {
            addr,
            is_voter: true,
            connected: true,
            unreachable: false,
            leader_state: state,
        }
    }

    fn unreachable(mut peer: PeerView<u16>) -> PeerView<u16> {
        peer.connected = false;
        peer.unreachable = true;
        peer
    }

    fn electing(vote: u16) -> LeaderMode<u16> {
        LeaderMode::Electing { vote: Some(vote) }
    }

    fn following(leader: u16) -> LeaderMode<u16> {
        LeaderMode::Following { leader }
    }

    #[test]
    fn lone_voter_becomes_leader() {
        let next = next_voter_state(1, &ls(0, LeaderMode::NoLeader), &[]);
        assert_eq!(next, ls(0, LeaderMode::Leading));
    }

    #[test]
    fn waits_for_reachable_peers_with_unknown_status() {
        let peers = [PeerView {
            addr: 2,
            is_voter: true,
            connected: false,
            unreachable: false,
            leader_state: None,
        }];

        let next = next_voter_state(1, &ls(0, LeaderMode::NoLeader), &peers);
        assert_eq!(next, ls(0, electing(1)));
    }

    #[test]
    fn unreachable_peers_do_not_block_election() {
        let peers = [unreachable(voter(2, None)), unreachable(voter(3, None))];

        /* 2 of 3 voters unreachable: still just electing, no majority */
        let next = next_voter_state(1, &ls(0, LeaderMode::NoLeader), &peers);
        assert_eq!(next, ls(0, electing(1)));

        /* but a peer we never learned anything about is not a voter */
        let mut unknown = unreachable(voter(9, None));
        unknown.is_voter = false;
        let next = next_voter_state(1, &ls(0, LeaderMode::NoLeader), &[unknown]);
        assert_eq!(next, ls(0, LeaderMode::Leading));
    }

    #[test]
    fn votes_for_lowest_reachable_address() {
        let peers = [voter(1, None), voter(3, None)];
        let next = next_voter_state(2, &ls(4, LeaderMode::Electing { vote: None }), &peers);
        assert_eq!(next, ls(4, electing(1)));
    }

    #[test]
    fn skips_unreachable_candidates() {
        let peers = [unreachable(voter(1, None)), voter(3, None)];
        let next = next_voter_state(2, &ls(4, LeaderMode::Electing { vote: None }), &peers);
        assert_eq!(next, ls(4, electing(2)));
    }

    #[test]
    fn wins_election_with_majority() {
        let peers = [voter(2, Some(ls(4, electing(1)))), voter(3, Some(ls(4, electing(3))))];
        let next = next_voter_state(1, &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn does_not_win_without_majority() {
        let peers = [voter(2, Some(ls(4, electing(2)))), voter(3, Some(ls(4, electing(3))))];
        let next = next_voter_state(1, &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, electing(1)));
    }

    #[test]
    fn following_a_winner_counts_as_support() {
        let peers = [voter(2, Some(ls(4, following(1)))), voter(3, Some(ls(4, electing(3))))];
        let next = next_voter_state(1, &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn stale_term_votes_do_not_count() {
        let peers = [voter(2, Some(ls(3, electing(1)))), voter(3, Some(ls(3, following(1))))];
        let next = next_voter_state(1, &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, electing(1)));
    }

    #[test]
    fn adopts_highest_term_and_follows_its_claim() {
        let peers = [voter(2, Some(ls(7, LeaderMode::Leading))), voter(3, Some(ls(6, following(3))))];
        let next = next_voter_state(1, &ls(2, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(7, following(2)));
    }

    #[test]
    fn conflicting_leaders_resolve_to_lowest_address_without_term_bump() {
        let peers = [voter(2, Some(ls(4, LeaderMode::Leading)))];

        /* higher address concedes */
        let next = next_voter_state(3, &ls(4, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(4, following(2)));

        /* lower address keeps the claim */
        let next = next_voter_state(1, &ls(4, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn follower_starts_new_term_when_leader_is_unreachable() {
        let peers = [unreachable(voter(1, Some(ls(4, LeaderMode::Leading)))), voter(3, None)];
        let next = next_voter_state(2, &ls(4, following(1)), &peers);
        assert_eq!(next, ls(5, LeaderMode::Electing { vote: None }));
    }

    #[test]
    fn follower_keeps_leader_with_stale_gossip() {
        /* the leader's last gossip is from an older term but it is still
         * reachable; don't churn */
        let peers = [voter(1, Some(ls(3, LeaderMode::Leading)))];
        let next = next_voter_state(2, &ls(4, following(1)), &peers);
        assert_eq!(next, ls(4, following(1)));
    }

    #[test]
    fn follower_rejoins_election_when_leader_abdicates() {
        let peers = [voter(1, Some(ls(4, LeaderMode::Electing { vote: None })))];
        let next = next_voter_state(2, &ls(4, following(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::Electing { vote: None }));
    }

    #[test]
    fn follower_adopts_leaders_concession() {
        let peers = [voter(2, Some(ls(4, following(1)))), voter(1, None)];
        let next = next_voter_state(3, &ls(4, following(2)), &peers);
        assert_eq!(next, ls(4, following(1)));
    }

    #[test]
    fn leader_steps_down_without_reachable_majority() {
        let peers = [unreachable(voter(2, None)), unreachable(voter(3, None))];
        let next = next_voter_state(1, &ls(4, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(5, LeaderMode::Electing { vote: None }));
    }

    #[test]
    fn leader_keeps_leading_with_reachable_majority() {
        let peers = [voter(2, Some(ls(4, following(1)))), unreachable(voter(3, None))];
        let next = next_voter_state(1, &ls(4, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn split_votes_converge_on_lowest_address() {
        /* every voter recomputes its vote deterministically, so an initial
         * split (everyone voted for itself) resolves to the lowest address */
        let peers = [voter(2, Some(ls(0, electing(2)))), voter(3, Some(ls(0, electing(3))))];
        let next = next_voter_state(1, &ls(0, electing(1)), &peers);
        assert_eq!(next, ls(0, electing(1)));

        let peers = [voter(1, Some(ls(0, electing(1)))), voter(3, Some(ls(0, electing(3))))];
        let next = next_voter_state(2, &ls(0, electing(2)), &peers);
        assert_eq!(next, ls(0, electing(1)));

        /* once the split voters adopt the lowest address, it wins */
        let peers = [voter(2, Some(ls(0, electing(1)))), voter(3, Some(ls(0, electing(1))))];
        let next = next_voter_state(1, &ls(0, electing(1)), &peers);
        assert_eq!(next, ls(0, LeaderMode::Leading));
    }

    #[test]
    fn observer_follows_claimed_leader() {
        let peers = [voter(1, Some(ls(4, LeaderMode::Leading))), voter(2, Some(ls(4, following(1))))];
        let next = next_observer_state(&ls(0, LeaderMode::NoLeader), &peers);
        assert_eq!(next, ls(4, following(1)));
    }

    #[test]
    fn observer_falls_back_to_most_followed_leader() {
        let peers = [
            voter(2, Some(ls(4, following(1)))),
            voter(3, Some(ls(4, following(1)))),
            voter(4, Some(ls(4, following(4)))),
        ];
        let next = next_observer_state(&ls(0, LeaderMode::NoLeader), &peers);
        assert_eq!(next, ls(4, following(1)));
    }

    #[test]
    fn observer_reports_no_leader_during_election() {
        let peers = [voter(1, Some(ls(4, electing(1)))), voter(2, Some(ls(4, electing(1))))];
        let next = next_observer_state(&ls(3, following(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::NoLeader));
    }

    #[test]
    fn observer_keeps_state_without_voter_info() {
        let current = ls(4, following(1));
        assert_eq!(next_observer_state(&current, &[]), current);

        let peers = [unreachable(voter(1, Some(ls(4, LeaderMode::Leading))))];
        assert_eq!(next_observer_state(&current, &peers), current);
    }

    struct SimNode {
        addr: u16,
        can_lead: bool,
        state: LeaderState<u16>,
    }

    /* run synchronized gossip rounds: every node decides against the states
     * all nodes published in the previous round; down nodes keep publishing
     * their last (stale) state, matching how leader_info persists */
    fn run_rounds(nodes: &mut [SimNode], down: &[u16], rounds: usize) {
        for _ in 0..rounds {
            let published = nodes
                .iter()
                .map(|node| (node.addr, node.can_lead, node.state.clone()))
                .collect::<Vec<_>>();

            for node in nodes.iter_mut() {
                if down.contains(&node.addr) {
                    continue;
                }

                let peers = published
                    .iter()
                    .filter(|(addr, _, _)| *addr != node.addr)
                    .map(|(addr, can_lead, state)| PeerView {
                        addr: *addr,
                        is_voter: *can_lead,
                        connected: !down.contains(addr),
                        unreachable: down.contains(addr),
                        leader_state: Some(state.clone()),
                    })
                    .collect::<Vec<_>>();

                node.state = if node.can_lead {
                    next_voter_state(node.addr, &node.state, &peers)
                } else {
                    next_observer_state(&node.state, &peers)
                };
            }
        }
    }

    fn assert_all_agree(nodes: &[SimNode], leader: u16, term: u64, down: &[u16]) {
        for node in nodes {
            if down.contains(&node.addr) {
                continue;
            }
            let expected = if node.addr == leader {
                ls(term, LeaderMode::Leading)
            } else {
                ls(term, following(leader))
            };
            assert_eq!(node.state, expected, "node {} disagrees", node.addr);
        }
    }

    #[test]
    fn cluster_converges_through_leader_failure_and_recovery() {
        let mut nodes = (1..=7)
            .map(|addr| SimNode {
                addr,
                can_lead: addr <= 5,
                state: ls(0, LeaderMode::NoLeader),
            })
            .collect::<Vec<_>>();

        /* cold start: everyone agrees on the lowest address */
        run_rounds(&mut nodes, &[], 5);
        assert_all_agree(&nodes, 1, 0, &[]);

        /* leader dies: survivors elect the next address in a new term */
        run_rounds(&mut nodes, &[1], 6);
        assert_all_agree(&nodes, 2, 1, &[1]);

        /* old leader rejoins with a stale Leading claim and concedes */
        run_rounds(&mut nodes, &[], 3);
        assert_all_agree(&nodes, 2, 1, &[]);
    }

    #[test]
    fn observer_ignores_stale_term_claims() {
        let peers = [voter(1, Some(ls(3, LeaderMode::Leading))), voter(3, Some(ls(4, following(2))))];
        let next = next_observer_state(&ls(3, following(1)), &peers);
        assert_eq!(next, ls(4, following(2)));
    }
}
