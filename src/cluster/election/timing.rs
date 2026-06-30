use std::time::Duration;

#[derive(Clone, Debug)]
pub struct NodeTiming {
    pub observation_stale_after: Duration,
    pub observation_interval: Duration,
    pub follow_retry_interval: Duration,
    pub rpc_timeout: Duration,
}

impl Default for NodeTiming {
    fn default() -> Self {
        Self {
            observation_stale_after: default_observation_stale_after(),
            observation_interval: default_observation_interval(),
            follow_retry_interval: default_follow_retry_interval(),
            rpc_timeout: default_rpc_timeout(),
        }
    }
}

impl NodeTiming {
    pub fn observation_stale_ms(&self) -> u64 {
        self.observation_stale_after.as_millis().try_into().unwrap_or(u64::MAX)
    }
}

#[cfg(not(test))]
fn default_observation_stale_after() -> Duration {
    Duration::from_secs(15)
}

#[cfg(test)]
fn default_observation_stale_after() -> Duration {
    Duration::from_millis(300)
}

#[cfg(not(test))]
fn default_observation_interval() -> Duration {
    Duration::from_secs(3)
}

#[cfg(test)]
fn default_observation_interval() -> Duration {
    Duration::from_millis(50)
}

#[cfg(not(test))]
fn default_follow_retry_interval() -> Duration {
    Duration::from_secs(1)
}

#[cfg(test)]
fn default_follow_retry_interval() -> Duration {
    Duration::from_millis(50)
}

#[cfg(not(test))]
fn default_rpc_timeout() -> Duration {
    Duration::from_secs(5)
}

#[cfg(test)]
fn default_rpc_timeout() -> Duration {
    Duration::from_millis(150)
}
