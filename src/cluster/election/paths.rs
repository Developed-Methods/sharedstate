use std::collections::HashSet;

use crate::transport::traits::SyncIOAddress;

pub(crate) fn valid_remote_leader_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], remote: A, local: A) -> bool {
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

pub(crate) fn valid_local_leader_path<A: SyncIOAddress>(leader: Option<A>, path: &[A], local: A) -> bool {
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

pub(crate) fn append_path<A: SyncIOAddress>(mut path: Vec<A>, local: A) -> Vec<A> {
    if !path.contains(&local) {
        path.push(local);
    }
    path
}
