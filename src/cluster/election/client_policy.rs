use std::collections::BTreeSet;

use crate::net::sync_io::SyncIOAddress;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PeerKind {
    Unknown,
    Known { can_lead: bool },
}

#[derive(Clone, Debug)]
pub(crate) struct ObservationTargetInput<A: SyncIOAddress> {
    pub local: A,
    pub can_lead: bool,
    pub leader: Option<A>,
    pub follow_remote: Option<A>,
    pub has_usable_path: bool,
    pub peers: Vec<(A, PeerKind)>,
}

pub(crate) fn observation_targets<A: SyncIOAddress>(input: ObservationTargetInput<A>) -> Vec<A> {
    let mut targets = BTreeSet::new();

    for (addr, kind) in input.peers {
        if addr == input.local {
            continue;
        }

        if input.can_lead {
            targets.insert(addr);
            continue;
        }

        let should_observe = Some(addr) == input.leader && addr != input.local
            || Some(addr) == input.follow_remote
            || matches!(kind, PeerKind::Known { can_lead: true })
            || !input.has_usable_path;

        if should_observe {
            targets.insert(addr);
        }
    }

    targets.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(can_lead: bool) -> ObservationTargetInput<u64> {
        ObservationTargetInput {
            local: 1,
            can_lead,
            leader: Some(2),
            follow_remote: Some(3),
            has_usable_path: true,
            peers: vec![
                (1, PeerKind::Known { can_lead }),
                (2, PeerKind::Known { can_lead: true }),
                (3, PeerKind::Known { can_lead: false }),
                (4, PeerKind::Known { can_lead: false }),
                (5, PeerKind::Unknown),
            ],
        }
    }

    #[test]
    fn leader_capable_observes_all_peers() {
        assert_eq!(observation_targets(input(true)), vec![2, 3, 4, 5]);
    }

    #[test]
    fn follower_with_path_skips_known_non_leader_peers() {
        assert_eq!(observation_targets(input(false)), vec![2, 3]);
    }

    #[test]
    fn follower_without_path_observes_all_known_and_unknown_peers() {
        let mut input = input(false);
        input.leader = None;
        input.follow_remote = None;
        input.has_usable_path = false;

        assert_eq!(observation_targets(input), vec![2, 3, 4, 5]);
    }

    #[test]
    fn follower_observes_current_follow_remote() {
        let mut input = input(false);
        input.leader = None;
        input.follow_remote = Some(4);

        assert_eq!(observation_targets(input), vec![2, 4]);
    }

    #[test]
    fn follower_observes_current_remote_leader() {
        let mut input = input(false);
        input.leader = Some(4);
        input.follow_remote = None;

        assert_eq!(observation_targets(input), vec![2, 4]);
    }
}
