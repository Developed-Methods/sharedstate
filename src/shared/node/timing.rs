use std::time::Duration;

#[cfg(not(test))]
pub(super) fn observation_stale_ms() -> u64 {
    15_000
}

#[cfg(test)]
pub(super) fn observation_stale_ms() -> u64 {
    300
}

#[cfg(not(test))]
pub(super) fn observation_interval() -> Duration {
    Duration::from_secs(3)
}

#[cfg(test)]
pub(super) fn observation_interval() -> Duration {
    Duration::from_millis(50)
}

#[cfg(not(test))]
pub(super) fn follow_retry_interval() -> Duration {
    Duration::from_secs(1)
}

#[cfg(test)]
pub(super) fn follow_retry_interval() -> Duration {
    Duration::from_millis(50)
}

#[cfg(not(test))]
pub(super) fn rpc_timeout() -> Duration {
    Duration::from_secs(5)
}

#[cfg(test)]
pub(super) fn rpc_timeout() -> Duration {
    Duration::from_millis(150)
}
