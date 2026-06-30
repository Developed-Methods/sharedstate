use crate::net::sync_io::SyncIOAddress;

#[derive(Clone, Debug)]
pub(crate) struct FollowCandidate<A: SyncIOAddress> {
    pub address: A,
    pub connected: bool,
    pub latency_ms: Option<u64>,
    pub repeat_connect_fails: u64,
    pub last_connect_fail_ms: Option<u64>,
    pub failed_without_activity: bool,
    pub can_lead: bool,
    pub observed_leader: Option<A>,
}

pub(crate) fn sort_follow_candidates<A: SyncIOAddress>(
    leader: Option<A>,
    mut candidates: Vec<FollowCandidate<A>>,
) -> Vec<FollowCandidate<A>> {
    candidates.sort_by_key(|candidate| {
        let tier = if Some(candidate.address) == leader {
            if candidate.failed_without_activity {
                5u8
            } else {
                0
            }
        } else if candidate.failed_without_activity {
            5
        } else if leader.is_some() && candidate.observed_leader == leader {
            1
        } else if candidate.can_lead {
            3
        } else {
            4
        };
        (
            tier,
            !candidate.connected,
            candidate.latency_ms.unwrap_or(u64::MAX),
            candidate.repeat_connect_fails,
            candidate.last_connect_fail_ms.unwrap_or(0),
            candidate.address,
        )
    });

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(address: u64) -> FollowCandidate<u64> {
        FollowCandidate {
            address,
            connected: true,
            latency_ms: Some(1),
            repeat_connect_fails: 0,
            last_connect_fail_ms: None,
            failed_without_activity: false,
            can_lead: false,
            observed_leader: None,
        }
    }

    #[test]
    fn direct_leader_before_relay() {
        let mut relay = candidate(3);
        relay.observed_leader = Some(1);

        let sorted = sort_follow_candidates(Some(1), vec![relay, candidate(1)]);
        assert_eq!(sorted.into_iter().map(|c| c.address).collect::<Vec<_>>(), vec![1, 3]);
    }

    #[test]
    fn relay_with_leader_observation_before_can_lead_fallback() {
        let mut relay = candidate(3);
        relay.observed_leader = Some(1);
        let mut can_lead = candidate(2);
        can_lead.can_lead = true;

        let sorted = sort_follow_candidates(Some(1), vec![can_lead, relay]);
        assert_eq!(sorted.into_iter().map(|c| c.address).collect::<Vec<_>>(), vec![3, 2]);
    }

    #[test]
    fn can_lead_before_any_peer() {
        let mut can_lead = candidate(2);
        can_lead.can_lead = true;

        let sorted = sort_follow_candidates(Some(1), vec![candidate(4), can_lead]);
        assert_eq!(sorted.into_iter().map(|c| c.address).collect::<Vec<_>>(), vec![2, 4]);
    }

    #[test]
    fn low_latency_before_high_latency() {
        let mut slow = candidate(2);
        slow.latency_ms = Some(10);
        let fast = candidate(3);

        let sorted = sort_follow_candidates(None, vec![slow, fast]);
        assert_eq!(sorted.into_iter().map(|c| c.address).collect::<Vec<_>>(), vec![3, 2]);
    }

    #[test]
    fn repeat_failures_sort_after_healthier_peer() {
        let mut failed = candidate(2);
        failed.repeat_connect_fails = 2;

        let sorted = sort_follow_candidates(None, vec![failed, candidate(3)]);
        assert_eq!(sorted.into_iter().map(|c| c.address).collect::<Vec<_>>(), vec![3, 2]);
    }

    #[test]
    fn lower_address_breaks_tie() {
        let sorted = sort_follow_candidates(None, vec![candidate(3), candidate(2)]);
        assert_eq!(sorted.into_iter().map(|c| c.address).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn unknown_peer_is_available_as_last_resort() {
        let mut failed_leader = candidate(1);
        failed_leader.failed_without_activity = true;
        let unknown_relay = candidate(3);

        let sorted = sort_follow_candidates(Some(1), vec![failed_leader, unknown_relay]);
        assert_eq!(sorted.into_iter().map(|c| c.address).collect::<Vec<_>>(), vec![3, 1]);
    }
}
