use std::collections::{BTreeSet, HashMap, HashSet};

use crate::{
    protocol::messages::{LeaderMode, LeaderWithElectionInfo},
    transport::traits::SyncIOAddress,
};

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
pub struct PublishedLeader<A: SyncIOAddress> {
    pub leader: A,
    pub path: Vec<A>,
    pub via: Option<A>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoteTally<A: SyncIOAddress> {
    pub candidate: A,
    pub votes: usize,
    pub majority: usize,
}

#[derive(Clone)]
struct PublishedLeaderEvidence<A: SyncIOAddress> {
    leader: A,
    count: usize,
    max_recover_next_seq: u64,
    best_path: Option<Vec<A>>,
    best_via: Option<A>,
}

pub fn fresh_same_term_observations<A: SyncIOAddress>(input: &ElectionInput<A>) -> Vec<LeaderWithElectionInfo<A>> {
    fresh_same_term_observations_with_observer(input)
        .into_iter()
        .map(|(_, observation)| observation)
        .collect()
}

fn fresh_same_term_observations_with_observer<A: SyncIOAddress>(
    input: &ElectionInput<A>,
) -> Vec<(A, LeaderWithElectionInfo<A>)> {
    let mut observations = Vec::with_capacity(input.peer_observations.len() + 1);

    if input.local_observation.term == input.election_term {
        observations.push((input.local_address, input.local_observation.clone()));
    }

    observations.extend(
        input
            .peer_observations
            .iter()
            .cloned()
            .filter_map(|timed| fresh_observation(input.now_ms, input.stale_after_ms, timed))
            .filter(|(_, observation)| observation.term == input.election_term),
    );

    observations
}

pub fn find_published_leader<A: SyncIOAddress>(input: &ElectionInput<A>) -> Option<PublishedLeader<A>> {
    let mut evidence: HashMap<A, PublishedLeaderEvidence<A>> = HashMap::new();

    for (observer, observation) in fresh_same_term_observations_with_observer(input) {
        let Some((leader, route)) = published_leader_route(observer, &observation.leader, input.local_address) else {
            continue;
        };

        let recover_next_seq = observation.recover_details.next_seq();
        evidence
            .entry(leader)
            .and_modify(|candidate| {
                candidate.count += 1;
                let previous_max_recover_next_seq = candidate.max_recover_next_seq;
                candidate.max_recover_next_seq = candidate.max_recover_next_seq.max(recover_next_seq);
                if let Some((path, via)) = &route {
                    if should_replace_published_route(
                        candidate.best_path.as_ref(),
                        candidate.best_via,
                        previous_max_recover_next_seq,
                        path,
                        *via,
                        recover_next_seq,
                    ) {
                        candidate.best_path = Some(path.clone());
                        candidate.best_via = *via;
                    }
                }
            })
            .or_insert_with(|| {
                let (best_path, best_via) = route.unzip();
                PublishedLeaderEvidence {
                    leader,
                    count: 1,
                    max_recover_next_seq: recover_next_seq,
                    best_path,
                    best_via: best_via.flatten(),
                }
            });
    }

    evidence
        .into_values()
        .filter(|candidate| candidate.leader == input.local_address || candidate.best_via.is_some())
        .max_by(|a, b| {
            a.count
                .cmp(&b.count)
                .then(a.max_recover_next_seq.cmp(&b.max_recover_next_seq))
                .then_with(|| {
                    let a_len = a.best_path.as_ref().map(Vec::len).unwrap_or(usize::MAX);
                    let b_len = b.best_path.as_ref().map(Vec::len).unwrap_or(usize::MAX);
                    b_len.cmp(&a_len)
                })
                .then_with(|| b.leader.cmp(&a.leader))
        })
        .and_then(|candidate| {
            Some(PublishedLeader {
                leader: candidate.leader,
                path: candidate.best_path?,
                via: candidate.best_via,
            })
        })
}

pub fn conflicting_published_leaders<A: SyncIOAddress>(input: &ElectionInput<A>) -> Vec<A> {
    let mut leaders = BTreeSet::new();

    for (observer, observation) in fresh_same_term_observations_with_observer(input) {
        if let Some(leader) = published_leader(observer, &observation.leader) {
            leaders.insert(leader);
        }
    }

    leaders.into_iter().collect()
}

pub fn choose_vote<A: SyncIOAddress>(input: &ElectionInput<A>) -> Option<A> {
    input.known_can_lead.iter().copied().max_by(|a, b| {
        connectivity_score(input, *a)
            .cmp(&connectivity_score(input, *b))
            .then_with(|| b.cmp(a))
    })
}

pub fn tally_votes<A: SyncIOAddress>(input: &ElectionInput<A>) -> Option<VoteTally<A>> {
    let majority = can_lead_majority(&input.known_can_lead);
    let mut votes: HashMap<A, usize> = HashMap::new();

    for (observer, observation) in fresh_same_term_observations_with_observer(input)
        .into_iter()
        .filter(|(_, observation)| observation.can_lead)
    {
        let Some(candidate) = observation.leader.vote(observer) else {
            continue;
        };
        if !input.known_can_lead.contains(&candidate) {
            continue;
        }
        *votes.entry(candidate).or_default() += 1;
    }

    votes
        .into_iter()
        .max_by(|(a_candidate, a_votes), (b_candidate, b_votes)| {
            a_votes.cmp(b_votes).then_with(|| b_candidate.cmp(a_candidate))
        })
        .map(|(candidate, votes)| VoteTally {
            candidate,
            votes,
            majority,
        })
}

pub fn leader_offline_vote_count<A: SyncIOAddress>(input: &ElectionInput<A>, leader: A) -> usize {
    fresh_same_term_observations(input)
        .into_iter()
        .filter(|observation| observation.can_lead)
        .filter(|observation| !observation.reachable_can_lead.contains(&leader))
        .count()
}

pub fn can_lead_majority<A: SyncIOAddress>(known_can_lead: &BTreeSet<A>) -> usize {
    known_can_lead.len() / 2 + 1
}

pub fn peer_is_fresh<A: SyncIOAddress>(
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

fn connectivity_score<A: SyncIOAddress>(input: &ElectionInput<A>, candidate: A) -> usize {
    fresh_same_term_observations(input)
        .into_iter()
        .filter(|observation| observation.can_lead)
        .filter(|observation| observation.reachable_can_lead.contains(&candidate))
        .count()
}

fn published_leader_route<A: SyncIOAddress>(
    observer: A,
    mode: &LeaderMode<A>,
    local: A,
) -> Option<(A, Option<(Vec<A>, Option<A>)>)> {
    match mode {
        LeaderMode::NoLeader | LeaderMode::Electing { .. } => None,
        LeaderMode::Leading => {
            if observer == local {
                Some((observer, Some((vec![local], None))))
            } else {
                Some((observer, Some((vec![observer, local], Some(observer)))))
            }
        }
        LeaderMode::Following { leader } => {
            if *leader == local {
                Some((*leader, Some((vec![local], None))))
            } else if observer == local {
                Some((*leader, None))
            } else {
                Some((*leader, Some((vec![*leader, observer, local], Some(observer)))))
            }
        }
    }
}

fn published_leader<A: SyncIOAddress>(observer: A, mode: &LeaderMode<A>) -> Option<A> {
    match mode {
        LeaderMode::NoLeader | LeaderMode::Electing { .. } => None,
        LeaderMode::Leading => Some(observer),
        LeaderMode::Following { leader } => Some(*leader),
    }
}

fn should_replace_published_route<A: SyncIOAddress>(
    best_path: Option<&Vec<A>>,
    best_via: Option<A>,
    best_recover_next_seq: u64,
    path: &[A],
    via: Option<A>,
    recover_next_seq: u64,
) -> bool {
    let Some(best_path) = best_path else {
        return true;
    };

    recover_next_seq
        .cmp(&best_recover_next_seq)
        .then_with(|| best_path.len().cmp(&path.len()))
        .then_with(|| compare_optional_via(best_via, via))
        .is_gt()
}

fn compare_optional_via<A: SyncIOAddress>(a: Option<A>, b: Option<A>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => b.cmp(&a),
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn fresh_observation<A: SyncIOAddress>(
    now_ms: u64,
    stale_after_ms: u64,
    timed: TimedPeerObservation<A>,
) -> Option<(A, LeaderWithElectionInfo<A>)> {
    let last_activity = timed.last_activity_ms?;
    if stale_after_ms < now_ms.saturating_sub(last_activity) {
        return None;
    }
    Some((timed.observer, timed.observation))
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
        _path: Option<Vec<u64>>,
        vote: Option<u64>,
        can_lead: bool,
        reachable_can_lead: Vec<u64>,
        recover_next_seq: u64,
    ) -> (u64, LeaderWithElectionInfo<u64>) {
        let mode = match leader {
            Some(leader) if leader == observer => LeaderMode::Leading,
            Some(leader) => LeaderMode::Following { leader },
            None => match vote {
                Some(vote) => LeaderMode::Electing { vote: Some(vote) },
                None => LeaderMode::NoLeader,
            },
        };

        (
            observer,
            LeaderWithElectionInfo {
                term,
                leader: mode,
                can_lead,
                reachable_can_lead,
                recover_details: details(recover_next_seq),
            },
        )
    }

    fn input(local: u64, can_lead: bool, observations: Vec<(u64, LeaderWithElectionInfo<u64>)>) -> ElectionInput<u64> {
        let local_observation = observation(local, 1, None, None, None, can_lead, vec![local], 1).1;
        ElectionInput {
            local_address: local,
            can_lead,
            known_can_lead: [1, 2, 3].into_iter().collect(),
            local_observation,
            peer_observations: observations
                .into_iter()
                .map(|(observer, observation)| TimedPeerObservation {
                    observer,
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
    fn fresh_same_term_observations_ignore_stale_and_other_terms() {
        let mut input = input(
            1,
            true,
            vec![
                observation(2, 1, None, None, Some(2), true, vec![2], 1),
                observation(3, 2, None, None, Some(3), true, vec![3], 1),
            ],
        );
        input.peer_observations[0].last_activity_ms = Some(40);

        let observations = fresh_same_term_observations(&input);

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].leader, LeaderMode::NoLeader);
    }

    #[test]
    fn find_published_leader_returns_remote_route() {
        let input = input(
            3,
            true,
            vec![observation(
                2,
                1,
                Some(1),
                Some(vec![1, 2]),
                Some(1),
                true,
                vec![1, 2],
                1,
            )],
        );

        assert_eq!(
            find_published_leader(&input),
            Some(PublishedLeader {
                leader: 1,
                path: vec![1, 2, 3],
                via: Some(2),
            })
        );
    }

    #[test]
    fn find_published_leader_prefers_most_published_observations() {
        let input = input(
            4,
            true,
            vec![
                observation(1, 1, Some(1), Some(vec![1]), Some(1), true, vec![1], 1),
                observation(2, 1, Some(2), Some(vec![2]), Some(2), true, vec![2], 9),
                observation(3, 1, Some(1), Some(vec![1, 3]), Some(1), true, vec![1, 3], 1),
            ],
        );

        assert_eq!(find_published_leader(&input).unwrap().leader, 1);
    }

    #[test]
    fn conflicting_published_leaders_returns_distinct_valid_leaders() {
        let mut input = input(
            3,
            true,
            vec![
                observation(1, 1, Some(1), Some(vec![1]), Some(1), true, vec![1], 1),
                observation(2, 1, Some(2), Some(vec![2]), Some(2), true, vec![2], 1),
                observation(4, 2, Some(4), Some(vec![4]), Some(4), true, vec![4], 1),
                observation(5, 1, None, None, Some(5), true, vec![5], 1),
            ],
        );
        input.local_observation = observation(3, 1, Some(1), Some(vec![1, 3]), Some(1), true, vec![1, 3], 1).1;

        assert_eq!(conflicting_published_leaders(&input), vec![1, 2]);
    }

    #[test]
    fn choose_vote_prefers_candidate_with_most_reachable_can_lead_reports() {
        let input = input(
            1,
            true,
            vec![
                observation(2, 1, None, None, None, true, vec![2, 3], 1),
                observation(3, 1, None, None, None, true, vec![3], 1),
            ],
        );

        assert_eq!(choose_vote(&input), Some(3));
    }

    #[test]
    fn choose_vote_breaks_tie_by_lowest_address() {
        let input = input(
            1,
            true,
            vec![
                observation(2, 1, None, None, None, true, vec![2], 1),
                observation(3, 1, None, None, None, true, vec![3], 1),
            ],
        );

        assert_eq!(choose_vote(&input), Some(1));
    }

    #[test]
    fn tally_votes_counts_only_can_lead_observers() {
        let input = input(
            1,
            true,
            vec![
                observation(2, 1, None, None, Some(2), true, vec![2], 1),
                observation(4, 1, None, None, Some(2), false, vec![2], 1),
            ],
        );

        assert_eq!(
            tally_votes(&input),
            Some(VoteTally {
                candidate: 2,
                votes: 1,
                majority: 2,
            })
        );
    }

    #[test]
    fn tally_votes_detects_majority() {
        let mut input = input(
            1,
            true,
            vec![
                observation(2, 1, None, None, Some(2), true, vec![2], 1),
                observation(3, 1, None, None, Some(2), true, vec![2], 1),
            ],
        );
        input.local_observation.leader = LeaderMode::Electing { vote: Some(1) };

        let tally = tally_votes(&input).unwrap();

        assert_eq!(tally.candidate, 2);
        assert_eq!(tally.votes, 2);
        assert_eq!(tally.majority, 2);
    }

    #[test]
    fn leader_offline_vote_count_counts_can_lead_observers_missing_leader() {
        let input = input(
            1,
            true,
            vec![
                observation(2, 1, None, None, Some(2), true, vec![2], 1),
                observation(3, 1, None, None, Some(3), true, vec![3], 1),
                observation(4, 1, None, None, Some(3), false, vec![3], 1),
            ],
        );

        assert_eq!(leader_offline_vote_count(&input, 1), 2);
    }
}
