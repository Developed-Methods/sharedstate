use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
    time::UNIX_EPOCH,
};

static STATE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generates an id for a state lineage that is unlikely to collide across
/// nodes, so recovery checks don't accept a follower from a different lineage.
pub fn unique_state_id<A: Hash>(addr: &A) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    addr.hash(&mut hasher);
    now_ms().hash(&mut hasher);
    STATE_ID_COUNTER.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
    hasher.finish()
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn unknown_id_err(id: u16, name: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("unknown id for {}: {}", name, id))
}

pub fn unknown_version_err(version: u16, name: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("unknown version for {}: {}", name, version))
}
