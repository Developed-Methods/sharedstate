//! Instance-scoped metrics for shared-state nodes.

use arc_metrics::{helpers::RegisterableMetric, IntCounter, IntGauge, RegisterAction};

#[derive(Default, Debug)]
pub struct SharedStateMetrics {
    pub recovery_fresh_count: IntCounter,
    pub recovery_recover_count: IntCounter,
    pub peer_connect_success_count: IntCounter,
    pub peer_connect_failure_count: IntCounter,
    pub peer_connect_timeout_count: IntCounter,
    pub peer_subscription_success_count: IntCounter,
    pub peer_subscription_failure_count: IntCounter,
    pub action_client_count: IntCounter,
    pub action_forwarded_count: IntCounter,
    pub action_leader_count: IntCounter,
    pub action_follower_count: IntCounter,
    pub active_subscription_count: IntGauge,
}

impl SharedStateMetrics {
    pub fn record_fresh_recovery(&self) {
        self.recovery_fresh_count.inc();
    }

    pub fn record_incremental_recovery(&self) {
        self.recovery_recover_count.inc();
    }
}

impl RegisterableMetric for SharedStateMetrics {
    fn register(&'static self, register: &mut RegisterAction) {
        register
            .count("recovery_count", &self.recovery_fresh_count)
            .attr("type", "fresh");
        register
            .count("recovery_count", &self.recovery_recover_count)
            .attr("type", "recover");

        register
            .count("peer_connect_count", &self.peer_connect_success_count)
            .attr("result", "success");
        register
            .count("peer_connect_count", &self.peer_connect_failure_count)
            .attr("result", "failure");
        register
            .count("peer_connect_count", &self.peer_connect_timeout_count)
            .attr("result", "timeout");

        register
            .count("peer_subscription_count", &self.peer_subscription_success_count)
            .attr("result", "success");
        register
            .count("peer_subscription_count", &self.peer_subscription_failure_count)
            .attr("result", "failure");

        register
            .count("action_count", &self.action_client_count)
            .attr("source", "client");
        register
            .count("action_count", &self.action_forwarded_count)
            .attr("source", "forwarded");
        register
            .count("action_count", &self.action_leader_count)
            .attr("source", "leader");
        register
            .count("action_count", &self.action_follower_count)
            .attr("source", "follower");

        register.gauge("active_subscription_count", &self.active_subscription_count);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_metrics::PromMetricRegistry;

    use super::*;

    #[test]
    fn registers_recovery_count_by_type() {
        let metrics = Arc::new(SharedStateMetrics::default());
        let mut registry = PromMetricRegistry::new();
        registry.register(&metrics);

        metrics.record_fresh_recovery();
        metrics.record_incremental_recovery();

        let rendered = registry.to_string();
        assert!(rendered.contains("recovery_count{type=\"fresh\"} 1"));
        assert!(rendered.contains("recovery_count{type=\"recover\"} 1"));
    }
}
