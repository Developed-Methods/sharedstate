//! End-to-end fixed-leader sync tests over real TCP.

mod support;

use support::{init_tracing, TestCluster};

#[tokio::test(flavor = "multi_thread")]
async fn follower_actions_apply_through_fixed_leader_over_tcp() {
    init_tracing();

    let cluster = TestCluster::start(2).await;
    cluster.wait_connected_to_leader(1).await;

    cluster.submit(1, "from-follower", "1").await;
    cluster.wait_all_for_value("from-follower", "1").await;

    cluster.submit(0, "from-leader", "2").await;
    cluster.wait_all_for_value("from-leader", "2").await;
}
