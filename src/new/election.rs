use std::collections::{BTreeSet, HashMap, HashSet};

use crate::{protocol::messages::LeaderWithElectionInfo, transport::traits::SyncIOAddress};

struct CandidateEvidence<A: SyncIOAddress> {
    leader: A,
    agreement_count: u64,
    max_term: u64,
    max_recover_next_seq: u64,
    best_path: Option<Vec<A>>,
    best_via: Option<A>,
}

#[derive(Clone, Debug)]
pub struct ElectionInput<A: SyncIOAddress> {
    pub local_address: A,
    pub can_lead: bool,
    pub known_can_lead: BTreeSet<A>,
    pub local_observation: LeaderWithElectionInfo<A>,
    pub peer_observations: Vec<TimedPeerObservation<A>>,
    pub peer_reachability: HashMap<A, PeerReachability>,
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
    PromoteSelf { observed_term: u64 },
    FollowRemote { leader: A, term: u64, path: Vec<A>, via: A },
    ClearRemoteLeader { leader: A },
    NoChange,
}

pub fn decide_election<A: SyncIOAddress>(input: ElectionInput<A>) -> ElectionDecision<A> {
    let mut candidates: HashMap<A, CandidateEvidence<A>> = HashMap::new();
    let now_ms = input.now_ms;
    let stale_after_ms = input.stale_after_ms;

    for observation in std::iter::once(input.local_observation).chain(
        input
            .peer_observations
            .into_iter()
            .filter_map(|timed| fresh_observation(now_ms, stale_after_ms, timed)),
    ) {
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

        let local_path = if observation.observer == input.local_address {
            path.clone()
        } else {
            append_path(path.clone(), input.local_address)
        };
        let recover_next_seq = observation.recover_details.next_seq();
        let via = (observation.observer != input.local_address).then_some(observation.observer);

        candidates
            .entry(leader)
            .and_modify(|candidate| {
                candidate.agreement_count += 1;
                let replace_path = candidate.best_path.as_ref().is_none_or(|best_path| {
                    recover_next_seq
                        .cmp(&candidate.max_recover_next_seq)
                        .then(observation.term.cmp(&candidate.max_term))
                        .then_with(|| best_path.len().cmp(&local_path.len()))
                        .then_with(|| candidate.leader.cmp(&leader))
                        .is_gt()
                });
                candidate.max_term = candidate.max_term.max(observation.term);
                candidate.max_recover_next_seq = candidate.max_recover_next_seq.max(recover_next_seq);
                if replace_path {
                    candidate.best_path = Some(local_path.clone());
                    candidate.best_via = via;
                }
            })
            .or_insert_with(|| CandidateEvidence {
                leader,
                agreement_count: 1,
                max_term: observation.term,
                max_recover_next_seq: recover_next_seq,
                best_path: Some(local_path),
                best_via: via,
            });
    }

    let mut selected = candidates
        .into_values()
        .max_by(|a, b| {
            a.agreement_count
                .cmp(&b.agreement_count)
                .then(a.max_term.cmp(&b.max_term))
                .then(a.max_recover_next_seq.cmp(&b.max_recover_next_seq))
                .then_with(|| b.leader.cmp(&a.leader))
        })
        .map(|candidate| (candidate.max_term, candidate.leader, candidate.best_path, candidate.best_via));

    if selected.is_none() {
        selected = input
            .known_can_lead
            .iter()
            .find(|addr| {
                peer_is_fresh(**addr, input.local_address, &input.peer_reachability, input.now_ms, input.stale_after_ms)
            })
            .map(|leader| (input.election_term, *leader, None, None));
    }

    let Some((term, leader, path, via)) = selected else {
        return ElectionDecision::NoChange;
    };

    let reachable_count = input
        .known_can_lead
        .iter()
        .filter(|addr| {
            peer_is_fresh(**addr, input.local_address, &input.peer_reachability, input.now_ms, input.stale_after_ms)
        })
        .count();
    let active_can_lead_count = reachable_count.max(usize::from(input.can_lead));
    let majority = active_can_lead_count / 2 + 1;

    if leader == input.local_address && input.can_lead && majority <= reachable_count {
        return ElectionDecision::PromoteSelf { observed_term: term };
    }

    if leader != input.local_address {
        return match (path, via) {
            (Some(path), Some(via)) => ElectionDecision::FollowRemote {
                leader,
                term,
                path,
                via,
            },
            _ => ElectionDecision::ClearRemoteLeader { leader },
        };
    }

    ElectionDecision::NoChange
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
                observation(2, 2, Some(2), Some(vec![2]), 1),
                observation(3, 1, Some(1), Some(vec![1, 3]), 1),
            ],
        ));

        assert_eq!(decision, ElectionDecision::PromoteSelf { observed_term: 1 });
    }

    #[test]
    fn term_beats_state_progress_when_agreement_ties() {
        let decision = decide_election(input(
            3,
            true,
            vec![
                observation(1, 2, Some(1), Some(vec![1]), 1),
                observation(2, 1, Some(2), Some(vec![2]), 9),
            ],
        ));

        assert_eq!(
            decision,
            ElectionDecision::FollowRemote {
                leader: 1,
                term: 2,
                path: vec![1, 3],
                via: 1,
            }
        );
    }

    #[test]
    fn state_progress_beats_lower_state_when_agreement_and_term_tie() {
        let decision = decide_election(input(
            3,
            true,
            vec![
                observation(1, 2, Some(1), Some(vec![1]), 1),
                observation(2, 2, Some(2), Some(vec![2]), 9),
            ],
        ));

        assert_eq!(
            decision,
            ElectionDecision::FollowRemote {
                leader: 2,
                term: 2,
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
                observation(1, 2, Some(1), Some(vec![1]), 1),
                observation(2, 2, Some(2), Some(vec![2]), 1),
            ],
        ));

        assert_eq!(
            decision,
            ElectionDecision::FollowRemote {
                leader: 1,
                term: 2,
                path: vec![1, 3],
                via: 1,
            }
        );
    }

    #[test]
    fn stale_observations_are_ignored() {
        let mut input = input(1, true, vec![observation(2, 9, Some(2), Some(vec![2]), 9)]);
        input.peer_observations[0].last_activity_ms = Some(1);

        assert_eq!(decide_election(input), ElectionDecision::PromoteSelf { observed_term: 1 });
    }

    #[test]
    fn invalid_paths_are_ignored() {
        let decision = decide_election(input(1, true, vec![observation(2, 9, Some(2), Some(vec![2, 1]), 9)]));

        assert_eq!(decision, ElectionDecision::PromoteSelf { observed_term: 1 });
    }

    #[test]
    fn no_valid_leader_selects_lowest_reachable_can_lead() {
        let mut input = input(2, true, vec![observation(1, 1, None, None, 1)]);
        input.local_observation.leader = None;
        input.local_observation.leader_path = None;

        assert_eq!(decide_election(input), ElectionDecision::ClearRemoteLeader { leader: 1 });
    }

    #[test]
    fn local_promotion_requires_can_lead() {
        let decision = decide_election(input(4, false, vec![]));

        assert_eq!(decision, ElectionDecision::NoChange);
    }
}
