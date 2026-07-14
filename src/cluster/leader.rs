use std::{
    cmp::Reverse,
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::Hasher,
    sync::Arc,
    time::Duration,
};

use crate::{
    cluster::node_state::{ConnectStatus, NodeState, PeerState},
    protocol::messages::{ElectionTerm, LeaderState},
    state::{deterministic_state::DeterministicState, recoverable_state::RecoverableStateDetails},
    transport::traits::SyncIOAddress,
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
        let my_recovery = self.state.state.recovery_details().await;

        let (me, peer_views) = {
            let peers = self.state.peers.lock().await;

            /* count, per address, how many connected peers report reaching
             * it; a peer that over a third of reporters can reach is treated
             * as reachable even when our own dial to it failed, so a local
             * connectivity problem doesn't trigger a cluster-wide election */
            let (reporter_count, reach_table) = peers
                .values()
                .filter(|peer| peer.connect_status.is_connected())
                .filter_map(|peer| peer.leader_info.as_ref())
                .fold((0u32, HashMap::<A, u32>::new()), |(count, mut table), info| {
                    for addr in &info.reachable_voters {
                        *table.entry(*addr).or_insert(0) += 1;
                    }
                    (count + 1, table)
                });

            let report_threshold = reporter_count / 3;

            let peer_views = peers
                .values()
                .map(|peer| {
                    let reported_reachable = report_threshold < reach_table.get(&peer.addr).copied().unwrap_or(0);
                    PeerView::new(peer, reported_reachable)
                })
                .collect::<Vec<_>>();

            let me = PeerView {
                addr: self.state.my_address,
                is_voter: self.state.can_lead,
                unreachable: false,
                leader_state: None,
                recovery: Some(my_recovery),
                /* mirrors the reachable_voters list peers receive from us in
                 * LeaderInfo so we score ourselves the way peers score us */
                reachable: peers
                    .values()
                    .filter(|peer| peer.connect_status.is_connected())
                    .map(|peer| peer.addr)
                    .chain([self.state.my_address])
                    .collect(),
            };

            (me, peer_views)
        };

        let mut leader_state = self.state.leader_state.lock().await;

        let next = if self.state.can_lead {
            next_voter_state(&me, &leader_state, &peer_views)
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
    unreachable: bool,
    leader_state: Option<LeaderState<A>>,
    recovery: Option<RecoverableStateDetails>,
    reachable: Vec<A>,
}

impl<A: SyncIOAddress> PeerView<A> {
    fn new(peer: &PeerState<A>, reported_reachable: bool) -> Self {
        /* a peer only counts as unreachable when our own dial failed and the
         * rest of the cluster doesn't vouch for it either */
        let unreachable = matches!(peer.connect_status, ConnectStatus::FailedToConnect { .. }) && !reported_reachable;
        let known_can_lead = peer.can_lead.or(peer.leader_info.as_ref().map(|info| info.can_lead));

        Self {
            addr: peer.addr,
            /* until we learn a peer's can_lead, assume it votes so we don't
             * claim leadership before discovery settles; unreachable peers
             * with unknown status are excluded so they can't block elections */
            is_voter: known_can_lead.unwrap_or(!unreachable),
            unreachable,
            leader_state: peer.leader_info.as_ref().map(|info| info.leader_state.clone()),
            recovery: peer.leader_info.as_ref().map(|info| info.recovery_details.clone()),
            reachable: peer
                .leader_info
                .as_ref()
                .map(|info| info.reachable_voters.clone())
                .unwrap_or_default(),
        }
    }

    fn mode_at_term(&self, term: ElectionTerm) -> Option<&LeaderMode<A>> {
        /* full term identity (number and nonce): claims from a same-numbered
         * term started by a different root never count here */
        self.leader_state
            .as_ref()
            .and_then(|state| (state.term == term).then_some(&state.mode))
    }
}

/// Decides the next leader state for a node that can lead.
///
/// Convergence comes from deterministic rules that every voter applies to the
/// same gossiped data:
///  - the election term only moves forward, everyone adopts the highest seen
///    (ordered by term number, then nonce)
///  - terms with the same number but a different nonce come from different
///    election roots: their claims and votes never carry over, and a leader
///    that observes a conflicting root at its own term number starts a fresh
///    election in a strictly higher term instead of being overridden
///  - a voter votes for the reachable candidate that can recover the most
///    known peer states, then the best connected one, and finally falls back
///    to the lowest address, so votes can't tie
///  - leadership requires a strict majority of the known voter set, and
///    conflicting claims in one term resolve to the lowest address without
///    bumping the term (term bumps are reserved for leader failure and
///    conflicting election roots)
fn next_voter_state<A: SyncIOAddress>(
    me: &PeerView<A>,
    current: &LeaderState<A>,
    peers: &[PeerView<A>],
) -> LeaderState<A> {
    let term_salt = address_salt(me.addr);
    let voters = || peers.iter().filter(|peer| peer.is_voter);
    let seen_terms = || voters().filter_map(|peer| peer.leader_state.as_ref().map(|state| state.term));

    let term = seen_terms().chain(std::iter::once(current.term)).max().unwrap();

    /* our term number was also reached by a different root (same number,
     * different nonce); if we hold a leadership claim it must not be merged
     * away or fought over, so trigger a new election that both roots adopt */
    if matches!(current.mode, LeaderMode::Leading)
        && term.term() == current.term.term()
        && seen_terms().any(|seen| seen.term() == current.term.term() && seen != current.term)
    {
        tracing::warn!(
            term = %current.term,
            "a different election root reached our term number, starting a new election"
        );
        return LeaderState {
            term: term.bump_with_salt(term_salt),
            mode: LeaderMode::Electing { vote: None },
        };
    }

    let mode = if term == current.term {
        current.mode.clone()
    } else {
        /* a new term number, or the same number from a different root:
         * either way our old mode is meaningless there */
        LeaderMode::Electing { vote: None }
    };

    let voter_count = voters().count() + 1;
    let has_majority = |supporters: usize| voter_count < supporters * 2;

    /* a Leading claim at the current term is authoritative (it required a
     * majority); if partitions merge with two claims, the lowest address wins */
    let peer_claim = voters()
        .filter(|peer| !peer.unreachable)
        .filter(|peer| matches!(peer.mode_at_term(term), Some(LeaderMode::Leading)))
        .map(|peer| peer.addr)
        .min();

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
                    term: term.bump_with_salt(term_salt),
                    mode: LeaderMode::Electing { vote: None },
                };
            }

            match peer_claim {
                Some(leader) if leader < me.addr => {
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
                    term: term.bump_with_salt(term_salt),
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

            /* prefer the candidate whose state can recover the most known
             * peers (so the fewest nodes start fresh), then the one with the
             * best connectivity to voters, then the lowest address */
            let known_states = peers
                .iter()
                .chain([me])
                .filter_map(|peer| peer.recovery.as_ref())
                .collect::<Vec<_>>();
            let voter_addrs = voters().map(|peer| peer.addr).chain([me.addr]).collect::<HashSet<_>>();

            let score = |candidate: &PeerView<A>| {
                let recoverable = candidate
                    .recovery
                    .as_ref()
                    .map(|details| {
                        known_states
                            .iter()
                            .filter(|follower| details.can_recover_follower(follower))
                            .count()
                    })
                    .unwrap_or(0);
                let connectivity = candidate
                    .reachable
                    .iter()
                    .filter(|addr| voter_addrs.contains(addr))
                    .count();
                (recoverable, connectivity)
            };

            let vote = voters()
                .filter(|peer| !peer.unreachable)
                .chain([me])
                .max_by_key(|candidate| (score(candidate), Reverse(candidate.addr)))
                .map(|candidate| candidate.addr)
                .expect("candidates always include self");

            if vote == me.addr {
                let support = 1 + voters()
                    .filter(|peer| !peer.unreachable)
                    .filter(|peer| match peer.mode_at_term(term) {
                        Some(LeaderMode::Electing { vote: Some(vote) }) => *vote == me.addr,
                        Some(LeaderMode::Following { leader }) => *leader == me.addr,
                        _ => false,
                    })
                    .count();

                if has_majority(support) {
                    tracing::info!(%term, support, voter_count, "won election with voter majority");
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

fn address_salt<A: SyncIOAddress>(addr: A) -> u32 {
    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    let hash = hasher.finish();
    (hash as u32) ^ ((hash >> 32) as u32)
}

/// Decides the next leader state for a node that cannot lead. Observers never
/// vote; they mirror what the reachable voters report, preferring a direct
/// Leading claim and falling back to the most-followed address.
fn next_observer_state<A: SyncIOAddress>(current: &LeaderState<A>, peers: &[PeerView<A>]) -> LeaderState<A> {
    let voter_states = peers
        .iter()
        .filter(|peer| peer.is_voter && !peer.unreachable)
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
        LeaderState {
            term: ElectionTerm::from_term(term),
            mode,
        }
    }

    fn ls_root(term: u64, nonce: u32, mode: LeaderMode<u16>) -> LeaderState<u16> {
        LeaderState {
            term: ElectionTerm::from_parts(term, nonce),
            mode,
        }
    }

    /* all test voters share the same recoverable state by default so scores
     * tie and the address tiebreak decides, matching a healthy cluster */
    fn shared_recovery() -> RecoverableStateDetails {
        RecoverableStateDetails::new(7, 1)
    }

    fn voter(addr: u16, state: Option<LeaderState<u16>>) -> PeerView<u16> {
        PeerView {
            addr,
            is_voter: true,
            unreachable: false,
            leader_state: state,
            recovery: Some(shared_recovery()),
            reachable: vec![],
        }
    }

    fn me(addr: u16) -> PeerView<u16> {
        voter(addr, None)
    }

    fn unreachable(mut peer: PeerView<u16>) -> PeerView<u16> {
        peer.unreachable = true;
        peer
    }

    fn electing(vote: u16) -> LeaderMode<u16> {
        LeaderMode::Electing { vote: Some(vote) }
    }

    fn following(leader: u16) -> LeaderMode<u16> {
        LeaderMode::Following { leader }
    }

    fn assert_logical_state(actual: LeaderState<u16>, term: u64, mode: LeaderMode<u16>) {
        assert_eq!(actual.term.term(), term);
        assert_eq!(actual.mode, mode);
    }

    #[test]
    fn lone_voter_becomes_leader() {
        let next = next_voter_state(&me(1), &ls(0, LeaderMode::NoLeader), &[]);
        assert_eq!(next, ls(0, LeaderMode::Leading));
    }

    #[test]
    fn waits_for_reachable_peers_with_unknown_status() {
        let peers = [PeerView {
            addr: 2,
            is_voter: true,
            unreachable: false,
            leader_state: None,
            recovery: None,
            reachable: vec![],
        }];

        let next = next_voter_state(&me(1), &ls(0, LeaderMode::NoLeader), &peers);
        assert_eq!(next, ls(0, electing(1)));
    }

    #[test]
    fn unreachable_peers_do_not_block_election() {
        let peers = [unreachable(voter(2, None)), unreachable(voter(3, None))];

        /* 2 of 3 voters unreachable: still just electing, no majority */
        let next = next_voter_state(&me(1), &ls(0, LeaderMode::NoLeader), &peers);
        assert_eq!(next, ls(0, electing(1)));

        /* but a peer we never learned anything about is not a voter */
        let mut unknown = unreachable(voter(9, None));
        unknown.is_voter = false;
        let next = next_voter_state(&me(1), &ls(0, LeaderMode::NoLeader), &[unknown]);
        assert_eq!(next, ls(0, LeaderMode::Leading));
    }

    #[test]
    fn votes_for_lowest_reachable_address() {
        let peers = [voter(1, None), voter(3, None)];
        let next = next_voter_state(&me(2), &ls(4, LeaderMode::Electing { vote: None }), &peers);
        assert_eq!(next, ls(4, electing(1)));
    }

    #[test]
    fn skips_unreachable_candidates() {
        let peers = [unreachable(voter(1, None)), voter(3, None)];
        let next = next_voter_state(&me(2), &ls(4, LeaderMode::Electing { vote: None }), &peers);
        assert_eq!(next, ls(4, electing(2)));
    }

    #[test]
    fn does_not_vote_for_candidate_with_unknown_recovery_state() {
        /* peer 1 is reachable but hasn't shared its recovery details yet, so
         * it can't be preferred over ourselves */
        let mut unknown = voter(1, None);
        unknown.recovery = None;

        let next = next_voter_state(&me(2), &ls(4, LeaderMode::Electing { vote: None }), &[unknown]);
        assert_eq!(next, ls(4, electing(2)));
    }

    #[test]
    fn prefers_candidate_that_can_recover_more_voters() {
        let other_root = RecoverableStateDetails::new(9, 1);

        /* voter 3 shares state with voters 4 and 5; we can only recover
         * ourselves, so 3 wins the vote despite its higher address */
        let mut candidate = voter(3, None);
        candidate.recovery = Some(other_root.clone());
        let mut peer4 = voter(4, None);
        peer4.recovery = Some(other_root.clone());
        let mut peer5 = voter(5, None);
        peer5.recovery = Some(other_root);

        let mut myself = me(1);
        myself.recovery = Some(RecoverableStateDetails::new(5, 1));

        let peers = [candidate, peer4, peer5];
        let next = next_voter_state(&myself, &ls(4, LeaderMode::Electing { vote: None }), &peers);
        assert_eq!(next, ls(4, electing(3)));
    }

    #[test]
    fn prefers_candidate_with_better_voter_connectivity() {
        /* equal recovery scores: voter 2 reaches every voter while voter 1
         * only reaches itself, so 2 wins despite its higher address */
        let mut peer1 = voter(1, None);
        peer1.reachable = vec![1];
        let mut peer2 = voter(2, None);
        peer2.reachable = vec![1, 2, 3];

        let peers = [peer1, peer2];
        let next = next_voter_state(&me(3), &ls(4, LeaderMode::Electing { vote: None }), &peers);
        assert_eq!(next, ls(4, electing(2)));
    }

    #[test]
    fn connectivity_only_counts_voters() {
        /* voter 2's longer reach list is padded with non-voter addresses, so
         * it doesn't beat voter 1 */
        let mut peer1 = voter(1, None);
        peer1.reachable = vec![1, 2];
        let mut peer2 = voter(2, None);
        peer2.reachable = vec![2, 100, 101, 102];

        let peers = [peer1, peer2];
        let next = next_voter_state(&me(3), &ls(4, LeaderMode::Electing { vote: None }), &peers);
        assert_eq!(next, ls(4, electing(1)));
    }

    #[test]
    fn wins_election_with_majority() {
        let peers = [voter(2, Some(ls(4, electing(1)))), voter(3, Some(ls(4, electing(3))))];
        let next = next_voter_state(&me(1), &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn does_not_win_without_majority() {
        let peers = [voter(2, Some(ls(4, electing(2)))), voter(3, Some(ls(4, electing(3))))];
        let next = next_voter_state(&me(1), &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, electing(1)));
    }

    #[test]
    fn following_a_winner_counts_as_support() {
        let peers = [voter(2, Some(ls(4, following(1)))), voter(3, Some(ls(4, electing(3))))];
        let next = next_voter_state(&me(1), &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn stale_term_votes_do_not_count() {
        let peers = [voter(2, Some(ls(3, electing(1)))), voter(3, Some(ls(3, following(1))))];
        let next = next_voter_state(&me(1), &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, electing(1)));
    }

    #[test]
    fn votes_from_a_different_election_root_do_not_count() {
        /* the peers voted for us, but in a same-numbered term from a
         * different root (lower nonce); those votes must not produce a
         * majority in our election */
        let peers = [voter(2, Some(ls(4, electing(1)))), voter(3, Some(ls(4, following(1))))];
        let next = next_voter_state(&me(1), &ls_root(4, 9, electing(1)), &peers);
        assert_eq!(next, ls_root(4, 9, electing(1)));

        /* the same votes at the exact same term do count */
        let next = next_voter_state(&me(1), &ls(4, electing(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn adopts_highest_term_and_follows_its_claim() {
        let peers = [
            voter(2, Some(ls(7, LeaderMode::Leading))),
            voter(3, Some(ls(6, following(3)))),
        ];
        let next = next_voter_state(&me(1), &ls(2, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(7, following(2)));
    }

    #[test]
    fn conflicting_leaders_resolve_to_lowest_address_without_term_bump() {
        let peers = [voter(2, Some(ls(4, LeaderMode::Leading)))];

        /* higher address concedes */
        let next = next_voter_state(&me(3), &ls(4, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(4, following(2)));

        /* lower address keeps the claim */
        let next = next_voter_state(&me(1), &ls(4, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn leader_starts_new_election_when_a_different_root_reaches_its_term() {
        /* the peer's term has our number but a different nonce: it was
         * created by a different root, so instead of conceding by address we
         * bump into a fresh election */
        let peers = [voter(2, Some(ls_root(4, 9, LeaderMode::Leading)))];
        let next = next_voter_state(&me(1), &ls(4, LeaderMode::Leading), &peers);
        assert_logical_state(next, 5, LeaderMode::Electing { vote: None });

        /* also when our nonce is the greater one */
        let peers = [voter(2, Some(ls(4, LeaderMode::Leading)))];
        let next = next_voter_state(&me(1), &ls_root(4, 9, LeaderMode::Leading), &peers);
        assert_logical_state(next, 5, LeaderMode::Electing { vote: None });
    }

    #[test]
    fn adopting_a_conflicting_root_term_restarts_the_election() {
        /* an electing voter that adopts a same-numbered term from a different
         * root drops its old vote and re-elects within the adopted term */
        let peers = [voter(2, Some(ls_root(4, 9, LeaderMode::Electing { vote: None })))];
        let next = next_voter_state(&me(1), &ls(4, following(3)), &peers);
        assert_eq!(next.term, ElectionTerm::from_parts(4, 9));
        assert_eq!(next.mode, electing(1));
    }

    #[test]
    fn follower_starts_new_term_when_leader_is_unreachable() {
        let peers = [unreachable(voter(1, Some(ls(4, LeaderMode::Leading)))), voter(3, None)];
        let next = next_voter_state(&me(2), &ls(4, following(1)), &peers);
        assert_logical_state(next, 5, LeaderMode::Electing { vote: None });
    }

    #[test]
    fn follower_keeps_leader_with_stale_gossip() {
        /* the leader's last gossip is from an older term but it is still
         * reachable; don't churn */
        let peers = [voter(1, Some(ls(3, LeaderMode::Leading)))];
        let next = next_voter_state(&me(2), &ls(4, following(1)), &peers);
        assert_eq!(next, ls(4, following(1)));
    }

    #[test]
    fn follower_rejoins_election_when_leader_abdicates() {
        let peers = [voter(1, Some(ls(4, LeaderMode::Electing { vote: None })))];
        let next = next_voter_state(&me(2), &ls(4, following(1)), &peers);
        assert_eq!(next, ls(4, LeaderMode::Electing { vote: None }));
    }

    #[test]
    fn follower_adopts_leaders_concession() {
        let peers = [voter(2, Some(ls(4, following(1)))), voter(1, None)];
        let next = next_voter_state(&me(3), &ls(4, following(2)), &peers);
        assert_eq!(next, ls(4, following(1)));
    }

    #[test]
    fn leader_steps_down_without_reachable_majority() {
        let peers = [unreachable(voter(2, None)), unreachable(voter(3, None))];
        let next = next_voter_state(&me(1), &ls(4, LeaderMode::Leading), &peers);
        assert_logical_state(next, 5, LeaderMode::Electing { vote: None });
    }

    #[test]
    fn leader_keeps_leading_with_reachable_majority() {
        let peers = [voter(2, Some(ls(4, following(1)))), unreachable(voter(3, None))];
        let next = next_voter_state(&me(1), &ls(4, LeaderMode::Leading), &peers);
        assert_eq!(next, ls(4, LeaderMode::Leading));
    }

    #[test]
    fn split_votes_converge_on_lowest_address() {
        /* every voter recomputes its vote deterministically, so an initial
         * split (everyone voted for itself) resolves to the lowest address */
        let peers = [voter(2, Some(ls(0, electing(2)))), voter(3, Some(ls(0, electing(3))))];
        let next = next_voter_state(&me(1), &ls(0, electing(1)), &peers);
        assert_eq!(next, ls(0, electing(1)));

        let peers = [voter(1, Some(ls(0, electing(1)))), voter(3, Some(ls(0, electing(3))))];
        let next = next_voter_state(&me(2), &ls(0, electing(2)), &peers);
        assert_eq!(next, ls(0, electing(1)));

        /* once the split voters adopt the lowest address, it wins */
        let peers = [voter(2, Some(ls(0, electing(1)))), voter(3, Some(ls(0, electing(1))))];
        let next = next_voter_state(&me(1), &ls(0, electing(1)), &peers);
        assert_eq!(next, ls(0, LeaderMode::Leading));
    }

    #[test]
    fn observer_follows_claimed_leader() {
        let peers = [
            voter(1, Some(ls(4, LeaderMode::Leading))),
            voter(2, Some(ls(4, following(1)))),
        ];
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
        recovery: RecoverableStateDetails,
    }

    fn sim_node(addr: u16, can_lead: bool) -> SimNode {
        SimNode {
            addr,
            can_lead,
            state: ls(0, LeaderMode::NoLeader),
            recovery: shared_recovery(),
        }
    }

    /* run synchronized gossip rounds: every node decides against the states
     * all nodes published in the previous round; down nodes keep publishing
     * their last (stale) state, matching how leader_info persists */
    fn run_rounds(nodes: &mut [SimNode], down: &[u16], rounds: usize) {
        for _ in 0..rounds {
            let published = nodes
                .iter()
                .map(|node| (node.addr, node.can_lead, node.state.clone(), node.recovery.clone()))
                .collect::<Vec<_>>();

            for node in nodes.iter_mut() {
                if down.contains(&node.addr) {
                    continue;
                }

                let peers = published
                    .iter()
                    .filter(|(addr, _, _, _)| *addr != node.addr)
                    .map(|(addr, can_lead, state, recovery)| PeerView {
                        addr: *addr,
                        is_voter: *can_lead,
                        unreachable: down.contains(addr),
                        leader_state: Some(state.clone()),
                        recovery: Some(recovery.clone()),
                        reachable: vec![],
                    })
                    .collect::<Vec<_>>();

                let myself = PeerView {
                    addr: node.addr,
                    is_voter: node.can_lead,
                    unreachable: false,
                    leader_state: None,
                    recovery: Some(node.recovery.clone()),
                    reachable: vec![],
                };

                node.state = if node.can_lead {
                    next_voter_state(&myself, &node.state, &peers)
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
                LeaderMode::Leading
            } else {
                following(leader)
            };
            assert_eq!(node.state.term.term(), term, "node {} disagrees", node.addr);
            assert_eq!(node.state.mode, expected, "node {} disagrees", node.addr);
        }
    }

    #[test]
    fn cluster_converges_through_leader_failure_and_recovery() {
        let mut nodes = (1..=7).map(|addr| sim_node(addr, addr <= 5)).collect::<Vec<_>>();

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
    fn merged_roots_hold_a_fresh_election_and_prefer_recovery() {
        /* two clusters formed independently and both reached term 3; when
         * they merge, neither leader is silently overridden: both bump into
         * a fresh election which the root that can recover the most voters
         * (nodes 3, 4, 5) wins */
        let root_a = RecoverableStateDetails::new(100, 1);
        let root_b = RecoverableStateDetails::new(200, 1);

        let mut nodes = vec![
            SimNode {
                addr: 1,
                can_lead: true,
                state: ls_root(3, 7, LeaderMode::Leading),
                recovery: root_a.clone(),
            },
            SimNode {
                addr: 2,
                can_lead: true,
                state: ls_root(3, 7, following(1)),
                recovery: root_a,
            },
            SimNode {
                addr: 3,
                can_lead: true,
                state: ls_root(3, 9, LeaderMode::Leading),
                recovery: root_b.clone(),
            },
            SimNode {
                addr: 4,
                can_lead: true,
                state: ls_root(3, 9, following(3)),
                recovery: root_b.clone(),
            },
            SimNode {
                addr: 5,
                can_lead: true,
                state: ls_root(3, 9, following(3)),
                recovery: root_b,
            },
        ];

        run_rounds(&mut nodes, &[], 8);
        assert_all_agree(&nodes, 3, 4, &[]);
    }

    #[test]
    fn observer_ignores_stale_term_claims() {
        let peers = [
            voter(1, Some(ls(3, LeaderMode::Leading))),
            voter(3, Some(ls(4, following(2)))),
        ];
        let next = next_observer_state(&ls(3, following(1)), &peers);
        assert_eq!(next, ls(4, following(2)));
    }
}
