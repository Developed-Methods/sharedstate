use std::collections::{BTreeSet, HashMap, HashSet};

use crate::{protocol::messages::LeaderWithElectionInfo, transport::traits::SyncIOAddress};

#[derive(Clone)]
struct CandidateRoute<A: SyncIOAddress> {
    path: Vec<A>,
    via: A,
    recover_next_seq: u64,
}

struct CandidateEvidence<A: SyncIOAddress> {
    leader: A,
    agreement_count: u64,
    max_recover_next_seq: u64,
    best_route: Option<CandidateRoute<A>>,
}

#[derive(Clone, Debug)]
pub struct ElectionInput<A: SyncIOAddress> {
    pub local_address: A,
    pub can_lead: bool,
    pub known_can_lead: BTreeSet<A>,
    pub local_observation: LeaderWithElectionInfo<A>,
    pub peer_observations: Vec<TimedPeerObservation<A>>,
    pub peer_reachability: HashMap<A, PeerReachability>,
    pub current_leader: Option<A>,
    pub election_term: u64,
    pub now_ms: u64,
    pub stale_after_ms: u64,
}

#[derive(Clone, Debug)]
pub struct TimedPeerObservation<A: SyncIOAddress> {
    pub observer: A,
    pub last_activity_ms: Option<u64>,
    pub observation: LeaderWithElectionInfo<A>,
}

#[derive(Clone, Debug)]
pub struct PeerReachability {
    pub last_activity_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ElectionDecision<A: SyncIOAddress> {
    PromoteSelf { term: u64 },
    FollowRemote { leader: A, term: u64, path: Vec<A>, via: A },
    BumpTerm { previous_term: u64, inaccessible_leader: A },
    NoChange,
}

pub fn decide_election<A: SyncIOAddress>(input: ElectionInput<A>) -> ElectionDecision<A> {
    let mut candidates: HashMap<A, CandidateEvidence<A>> = HashMap::new();
    let now_ms = input.now_ms;
    let stale_after_ms = input.stale_after_ms;

    for observation in std::iter::once(input.local_observation.clone()).chain(
        input
            .peer_observations
            .iter()
            .cloned()
            .filter_map(|timed| fresh_observation(now_ms, stale_after_ms, timed)),
    ) {
        if observation.term != input.election_term {
            continue;
        }
        let Some(leader) = observation.leader else {
            continue;
        };
        let Some(path) = &observation.leader_path else {
            continue;
        };

        if observation.observer == input.local_address {
            if !valid_local_leader_path(Some(leader), path, input.local_address) {
                continue;
            }
        } else if !valid_remote_leader_path(Some(leader), path, observation.observer, input.local_address) {
            continue;
        }

        let recover_next_seq = observation.recover_details.next_seq();
        let route = (observation.observer != input.local_address).then(|| CandidateRoute {
            path: append_path(path.clone(), input.local_address),
            via: observation.observer,
            recover_next_seq,
        });

        candidates
            .entry(leader)
            .and_modify(|candidate| {
                candidate.agreement_count += 1;
                candidate.max_recover_next_seq = candidate.max_recover_next_seq.max(recover_next_seq);
                if let Some(route) = route.clone() {
                    let replace_route = candidate.best_route.as_ref().is_none_or(|best_route| {
                        route
                            .recover_next_seq
                            .cmp(&best_route.recover_next_seq)
                            .then_with(|| best_route.path.len().cmp(&route.path.len()))
                            .then_with(|| best_route.via.cmp(&route.via))
                            .is_gt()
                    });
                    if replace_route {
                        candidate.best_route = Some(route);
                    }
                }
            })
            .or_insert_with(|| CandidateEvidence {
                leader,
                agreement_count: 1,
                max_recover_next_seq: recover_next_seq,
                best_route: route,
            });
    }

    let selected = candidates.into_values().max_by(|a, b| {
        a.agreement_count
            .cmp(&b.agreement_count)
            .then(a.max_recover_next_seq.cmp(&b.max_recover_next_seq))
            .then_with(|| b.leader.cmp(&a.leader))
    });

    let reachable_count = input
        .known_can_lead
        .iter()
        .filter(|addr| {
            peer_is_fresh(**addr, input.local_address, &input.peer_reachability, input.now_ms, input.stale_after_ms)
        })
        .count();
    let active_can_lead_count = reachable_count.max(usize::from(input.can_lead));
    let majority = active_can_lead_count / 2 + 1;

    if let Some(candidate) = selected {
        if candidate.leader == input.local_address {
            return if input.can_lead && majority <= reachable_count {
                ElectionDecision::PromoteSelf {
                    term: input.election_term,
                }
            } else {
                ElectionDecision::NoChange
            };
        }

        if let Some(route) = candidate.best_route {
            return ElectionDecision::FollowRemote {
                leader: candidate.leader,
                term: input.election_term,
                path: route.path,
                via: route.via,
            };
        }

        return if input.current_leader == Some(candidate.leader)
            && !peer_is_fresh(
                candidate.leader,
                input.local_address,
                &input.peer_reachability,
                input.now_ms,
                input.stale_after_ms,
            ) {
            ElectionDecision::BumpTerm {
                previous_term: input.election_term,
                inaccessible_leader: candidate.leader,
            }
        } else {
            ElectionDecision::NoChange
        };
    }

    if let Some(inaccessible_leader) = inaccessible_current_leader(&input) {
        return ElectionDecision::BumpTerm {
            previous_term: input.election_term,
            inaccessible_leader,
        };
    }

    if input
        .known_can_lead
        .iter()
        .find(|addr| {
            peer_is_fresh(**addr, input.local_address, &input.peer_reachability, input.now_ms, input.stale_after_ms)
        })
        .is_some_and(|candidate| *candidate == input.local_address)
        && input.can_lead
        && majority <= reachable_count
    {
        return ElectionDecision::PromoteSelf {
            term: input.election_term,
        };
    }

    ElectionDecision::NoChange
}

fn inaccessible_current_leader<A: SyncIOAddress>(input: &ElectionInput<A>) -> Option<A> {
    let current_leader = input.current_leader?;
    (current_leader != input.local_address
        && !peer_is_fresh(
            current_leader,
            input.local_address,
            &input.peer_reachability,
            input.now_ms,
            input.stale_after_ms,
        ))
    .then_some(current_leader)
}

pub fn valid_remote_leader_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], remote: A, local: A) -> bool {
    let Some(leader) = leader else {
        return false;
    };
    if path.is_empty() || path[0] != leader {
        return false;
    }

    let mut seen = HashSet::new();
    for item in path {
        if !seen.insert(*item) {
            return false;
        }
    }

    path.last().copied() == Some(remote)
        && !path
            .iter()
            .enumerate()
            .any(|(idx, step)| *step == local && !(idx == 0 && leader == local))
}

pub fn valid_local_leader_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], local: A) -> bool {
    let Some(leader) = leader else {
        return false;
    };
    if path.is_empty() || path[0] != leader {
        return false;
    }

    let mut seen = HashSet::new();
    for item in path {
        if !seen.insert(*item) {
            return false;
        }
    }

    path.last().copied() == Some(local)
}

pub fn append_path<A: SyncIOAddress>(mut path: Vec<A>, local: A) -> Vec<A> {
    if !path.contains(&local) {
        path.push(local);
    }
    path
}

fn peer_is_fresh<A: SyncIOAddress>(
    addr: A,
    local: A,
    reachability: &HashMap<A, PeerReachability>,
    now_ms: u64,
    stale_after_ms: u64,
) -> bool {
    addr == local
        || reachability
            .get(&addr)
            .and_then(|reachability| reachability.last_activity_ms)
            .map(|ts| now_ms.saturating_sub(ts) <= stale_after_ms)
            .unwrap_or(false)
}

fn fresh_observation<A: SyncIOAddress>(
    now_ms: u64,
    stale_after_ms: u64,
    timed: TimedPeerObservation<A>,
) -> Option<LeaderWithElectionInfo<A>> {
    if timed.observer != timed.observation.observer {
        return None;
    }
    let last_activity = timed.last_activity_ms?;
    if stale_after_ms < now_ms.saturating_sub(last_activity) {
        return None;
    }
    Some(timed.observation)
}

#[cfg(test)]
mod tests {
    use crate::state::recoverable_state::RecoverableStateDetails;

    use super::*;

    fn details(next_seq: u64) -> RecoverableStateDetails {
        let mut details = RecoverableStateDetails::new(1, 1);
        for seq in 1..next_seq {
            details.apply_inner_state_seq(seq);
        }
        details
    }

    fn observation(
        observer: u64,
        term: u64,
        leader: Option<u64>,
        path: Option<Vec<u64>>,
        recover_next_seq: u64,
    ) -> LeaderWithElectionInfo<u64> {
        LeaderWithElectionInfo {
            observer,
            term,
            leader,
            leader_path: path,
            can_lead: observer <= 3,
            reachable_can_lead: vec![],
            recover_details: details(recover_next_seq),
        }
    }

    fn input(local: u64, can_lead: bool, observations: Vec<LeaderWithElectionInfo<u64>>) -> ElectionInput<u64> {
        let local_observation = observation(local, 1, Some(local), Some(vec![local]), 1);
        ElectionInput {
            local_address: local,
            can_lead,
            known_can_lead: [1, 2, 3].into_iter().collect(),
            local_observation,
            peer_observations: observations
                .into_iter()
                .map(|observation| TimedPeerObservation {
                    observer: observation.observer,
                    last_activity_ms: Some(100),
                    observation,
                })
                .collect(),
            peer_reachability: [(1, 100), (2, 100), (3, 100)]
                .into_iter()
                .map(|(addr, ts)| {
                    (
                        addr,
                        PeerReachability {
                            last_activity_ms: Some(ts),
                        },
                    )
                })
                .collect(),
            current_leader: None,
            election_term: 1,
            now_ms: 100,
            stale_after_ms: 50,
        }
    }

    #[test]
    fn remote_leader_path_rejects_cycles_and_local_node() {
        assert!(valid_remote_leader_path(Some(1), &[1, 2], 2, 3));
        assert!(valid_remote_leader_path(Some(1), &[1], 1, 3));
        assert!(!valid_remote_leader_path(Some(1), &[1, 2], 4, 3));
        assert!(!valid_remote_leader_path(Some(1), &[1, 2, 3], 3, 3));
        assert!(!valid_remote_leader_path(Some(1), &[1, 2, 2], 2, 3));
        assert!(!valid_remote_leader_path(Some(2), &[1, 2], 2, 3));
    }

    #[test]
    fn local_leader_path_must_end_at_local_node() {
        assert!(valid_local_leader_path(Some(1), &[1, 2, 3], 3));
        assert!(!valid_local_leader_path(Some(1), &[1, 2], 3));
        assert!(!valid_local_leader_path(Some(1), &[1, 2, 2], 2));
    }

    #[test]
    fn agreement_count_beats_higher_term() {
        let decision = decide_election(input(
            1,
            true,
            vec![
                observation(2, 1, Some(2), Some(vec![2]), 9),
                observation(3, 1, Some(1), Some(vec![1, 3]), 1),
            ],
        ));

        assert_eq!(decision, ElectionDecision::PromoteSelf { term: 1 });
    }

    #[test]
    fn ignores_observations_from_other_terms() {
        let decision = decide_election(input(1, true, vec![observation(2, 2, Some(2), Some(vec![2]), 9)]));

        assert_eq!(decision, ElectionDecision::PromoteSelf { term: 1 });
    }

    #[test]
    fn follows_existing_remote_leader_for_target_term() {
        let decision = decide_election(input(
            3,
            true,
            vec![
                observation(1, 1, Some(1), Some(vec![1]), 1),
                observation(2, 1, Some(2), Some(vec![2]), 9),
            ],
        ));

        assert_eq!(
            decision,
            ElectionDecision::FollowRemote {
                leader: 2,
                term: 1,
                path: vec![2, 3],
                via: 2,
            }
        );
    }

    #[test]
    fn lower_address_breaks_exact_tie() {
        let decision = decide_election(input(
            3,
            true,
            vec![
                observation(1, 1, Some(1), Some(vec![1]), 1),
                observation(2, 1, Some(2), Some(vec![2]), 1),
            ],
        ));

        assert_eq!(
            decision,
            ElectionDecision::FollowRemote {
                leader: 1,
                term: 1,
                path: vec![1, 3],
                via: 1,
            }
        );
    }

    #[test]
    fn stale_observations_are_ignored() {
        let mut input = input(1, true, vec![observation(2, 9, Some(2), Some(vec![2]), 9)]);
        input.peer_observations[0].last_activity_ms = Some(1);

        assert_eq!(decide_election(input), ElectionDecision::PromoteSelf { term: 1 });
    }

    #[test]
    fn invalid_paths_are_ignored() {
        let decision = decide_election(input(1, true, vec![observation(2, 1, Some(2), Some(vec![2, 1]), 9)]));

        assert_eq!(decision, ElectionDecision::PromoteSelf { term: 1 });
    }

    #[test]
    fn no_leader_waits_for_lower_remote_candidate_to_publish() {
        let mut input = input(2, true, vec![observation(1, 1, None, None, 1)]);
        input.local_observation.leader = None;
        input.local_observation.leader_path = None;

        assert_eq!(decide_election(input), ElectionDecision::NoChange);
    }

    #[test]
    fn local_promotion_requires_can_lead() {
        let decision = decide_election(input(4, false, vec![]));

        assert_eq!(decision, ElectionDecision::NoChange);
    }

    #[test]
    fn local_restatement_of_remote_leader_without_route_does_not_clear() {
        let mut input = input(1, false, vec![]);
        input.local_observation = observation(1, 1, Some(2), Some(vec![2, 1]), 1);
        input.current_leader = Some(2);

        assert_eq!(decide_election(input), ElectionDecision::NoChange);
    }

    #[test]
    fn inaccessible_current_leader_requests_term_bump() {
        let mut input = input(1, false, vec![]);
        input.local_observation = observation(1, 1, Some(2), Some(vec![2, 1]), 1);
        input.current_leader = Some(2);
        input.peer_reachability.insert(
            2,
            PeerReachability {
                last_activity_ms: Some(1),
            },
        );

        assert_eq!(
            decide_election(input),
            ElectionDecision::BumpTerm {
                previous_term: 1,
                inaccessible_leader: 2,
            }
        );
    }

    #[test]
    fn no_leader_promotes_local_lowest_fresh_candidate_for_term() {
        let mut input = input(1, true, vec![]);
        input.local_observation.leader = None;
        input.local_observation.leader_path = None;

        assert_eq!(decide_election(input), ElectionDecision::PromoteSelf { term: 1 });
    }

    #[test]
    fn same_term_candidate_tie_uses_agreement_then_recover_then_address() {
        let decision = decide_election(input(
            4,
            true,
            vec![
                observation(1, 1, Some(1), Some(vec![1]), 1),
                observation(2, 1, Some(2), Some(vec![2]), 9),
            ],
        ));

        assert_eq!(
            decision,
            ElectionDecision::FollowRemote {
                leader: 2,
                term: 1,
                path: vec![2, 4],
                via: 2,
            }
        );
    }
}
