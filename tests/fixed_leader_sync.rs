//! End-to-end fixed-leader sync tests over real TCP.

mod support;

use sharedstate::{FollowerStatus, NodeStatus};
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

#[tokio::test(flavor = "multi_thread")]
async fn debug_info_reports_leadership_and_subscription_peer() {
    init_tracing();

    let cluster = TestCluster::start(2).await;
    cluster.wait_connected_to_leader(1).await;

    let leader = cluster.debug_info(0);
    assert_eq!(leader.my_address, cluster.address(0));
    assert_eq!(leader.leader_address, cluster.address(0));
    assert_eq!(leader.status, NodeStatus::Leader);
    assert!(leader.is_leader());
    assert!(leader.is_connected_to_leader());
    assert!(!leader.is_subscribed_to_leader());
    assert_eq!(leader.connected_peer(), None);
    assert_eq!(leader.available_peers.len(), 2);

    let follower = cluster.debug_info(1);
    assert_eq!(follower.my_address, cluster.address(1));
    assert_eq!(follower.leader_address, cluster.address(0));
    assert_eq!(follower.status, NodeStatus::Follower(FollowerStatus::SubscribedToLeader));
    assert!(!follower.is_leader());
    assert!(follower.is_connected_to_leader());
    assert!(follower.is_subscribed_to_leader());
    assert_eq!(follower.connected_peer(), Some(cluster.address(0)));
    assert_eq!(follower.available_peers.len(), 2);
}
