//! End-to-end leader handoff tests over real TCP.

mod support;

use support::{init_tracing, TestCluster};

#[tokio::test(flavor = "multi_thread")]
async fn node_switches_between_following_and_leading_when_leader_address_changes() {
    init_tracing();

    let cluster = TestCluster::start(2).await;
    cluster.wait_connected_to_leader(1).await;
    assert!(!cluster.is_leader(1));

    cluster.elect(1);
    cluster.wait_leader(1).await;
    cluster.wait_accept_seq(1, 3).await;

    cluster.submit(1, "while-leader", "1").await;
    cluster.wait_value(1, "while-leader", "1").await;

    cluster.elect(0);
    cluster.wait_not_leader(1).await;
    cluster.wait_all_for_value("while-leader", "1").await;

    cluster.submit_until_all_apply(1, "after-step-down", "2").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_share_leader_receiver_and_apply_actions_across_leadership_changes() {
    init_tracing();

    let cluster = TestCluster::start(3).await;
    cluster.wait_all_connected_to_leader().await;

    assert_eq!(cluster.leader_address(), cluster.address(0));
    cluster.submit_until_all_apply(2, "under-node-0", "a").await;

    cluster.elect(1);
    cluster.wait_leader(1).await;
    cluster.wait_all_connected_to_leader().await;
    assert_eq!(cluster.leader_address(), cluster.address(1));
    cluster.submit_until_all_apply(0, "under-node-1", "b").await;

    cluster.elect(2);
    cluster.wait_leader(2).await;
    cluster.wait_all_connected_to_leader().await;
    assert_eq!(cluster.leader_address(), cluster.address(2));
    cluster.submit_until_all_apply(1, "under-node-2", "c").await;

    cluster.elect(0);
    cluster.wait_leader(0).await;
    cluster.wait_all_connected_to_leader().await;
    cluster.submit_until_all_apply(2, "back-to-node-0", "d").await;

    cluster.assert_all_contain_keys(&["under-node-0", "under-node-1", "under-node-2", "back-to-node-0"]);
}
