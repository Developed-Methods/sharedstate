//! End-to-end failover test over real TCP with default settings.

mod common;

use std::{sync::Arc, time::Duration};

use common::{start_node, wait_for_settled_leader, wait_for_value, KvState, LocalhostTcpIo};
use sharedstate::{SharedState, SharedStateConfig, SharedStateSettings};

#[tokio::test(flavor = "multi_thread")]
async fn follower_actions_apply_after_leader_change_over_tcp() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).try_init();

    let io1 = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let io2 = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let io3 = LocalhostTcpIo::bind_ephemeral().await.unwrap();

    let (addr1, addr2, addr3) = (io1.address, io2.address, io3.address);
    let mut order = [addr1, addr2, addr3];
    order.sort();
    let first_leader = order[0];
    let second_leader = order[1];

    /* the first leader runs on its own runtime so it can be killed like a
     * real process: every task dies and its sockets close */
    let mut ios = vec![io1, io2, io3];
    let leader_pos = ios.iter().position(|io| io.address == first_leader).unwrap();
    let leader_io = ios.remove(leader_pos);
    let leader_peers: Vec<u16> = ios.iter().map(|io| io.address).collect();

    let leader_rt = tokio::runtime::Runtime::new().unwrap();
    let leader_node = {
        let _guard = leader_rt.enter();
        SharedState::start(SharedStateConfig {
            my_address: leader_io.address,
            io: Arc::new(leader_io),
            can_lead: true,
            accessible: true,
            voter_gateway: None,
            initial_peers: leader_peers,
            initial_state: KvState::default(),
            settings: SharedStateSettings::default(),
        })
        .unwrap()
    };

    let mut remaining = Vec::new();
    for io in ios {
        let peers: Vec<u16> = [addr1, addr2, addr3]
            .iter()
            .copied()
            .filter(|addr| *addr != io.address)
            .collect();
        remaining.push(start_node(io, true, &peers).await);
    }

    {
        let all_refs: Vec<_> = remaining.iter().chain([&leader_node]).collect();
        wait_for_settled_leader(&all_refs, first_leader, Duration::from_secs(20)).await;
    }

    remaining[0]
        .submit_action(("before".to_owned(), "1".to_owned()))
        .await
        .unwrap();
    for node in &remaining {
        wait_for_value(node, "before", "1", Duration::from_secs(20)).await;
    }

    /* kill the leader like a process exit */
    drop(leader_node);
    leader_rt.shutdown_background();

    let remaining_refs: Vec<_> = remaining.iter().collect();
    wait_for_settled_leader(&remaining_refs, second_leader, Duration::from_secs(60)).await;

    /* the follower that moved to the new leader submits an action */
    let moved_follower = remaining
        .iter()
        .find(|node| node.my_address() != second_leader)
        .unwrap();
    moved_follower
        .submit_action(("after".to_owned(), "2".to_owned()))
        .await
        .unwrap();

    for node in &remaining {
        wait_for_value(node, "after", "2", Duration::from_secs(30)).await;
    }
}
