//! Observers reaching the voter set through a TCP load balancer, over real
//! sockets with default settings.

mod common;

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use common::{start_node, start_node_with_gateway, wait_for_settled_leader, wait_for_value, KvState, LocalhostTcpIo};
use sharedstate::{cluster::node_state::SyncStatus, SharedState, SharedStateConfig, SharedStateSettings};
use tokio::net::{TcpListener, TcpStream};

/// A minimal layer 4 load balancer: each accepted connection is piped to
/// the next backend in round robin order.
async fn start_round_robin_proxy(backends: Vec<u16>) -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let next = Arc::new(AtomicUsize::new(0));

    tokio::spawn(async move {
        loop {
            let Ok((mut client, _)) = listener.accept().await else {
                return;
            };
            let backend = backends[next.fetch_add(1, Ordering::Relaxed) % backends.len()];
            tokio::spawn(async move {
                let Ok(mut upstream) = TcpStream::connect(("127.0.0.1", backend)).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });

    port
}

#[tokio::test(flavor = "multi_thread")]
async fn observer_behind_tcp_proxy_syncs_and_survives_leader_change() {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).try_init();

    let io1 = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let io2 = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let io3 = LocalhostTcpIo::bind_ephemeral().await.unwrap();
    let observer_io = LocalhostTcpIo::bind_ephemeral().await.unwrap();

    let (addr1, addr2, addr3) = (io1.address, io2.address, io3.address);
    let mut order = [addr1, addr2, addr3];
    order.sort();
    let first_leader = order[0];
    let second_leader = order[1];

    let proxy = start_round_robin_proxy(vec![addr1, addr2, addr3]).await;

    /* the first leader runs on its own runtime so it can be killed like a
     * real process */
    let mut ios = vec![io1, io2, io3];
    let leader_pos = ios.iter().position(|io| io.address == first_leader).unwrap();
    let leader_io = ios.remove(leader_pos);
    let leader_peers: Vec<u16> = ios.iter().map(|io| io.address).collect();

    let leader_rt = tokio::runtime::Runtime::new().unwrap();
    let leader_node = {
        let _guard = leader_rt.enter();
        let my_address = leader_io.address;
        SharedState::start(SharedStateConfig {
            io: Arc::new(leader_io),
            my_address,
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

    /* the observer knows nothing but the proxy */
    let observer = start_node_with_gateway(observer_io, false, Some(proxy), &[]).await;
    wait_for_settled_leader(&[&observer], proxy, Duration::from_secs(30)).await;

    remaining[0]
        .submit_action(("before".to_owned(), "1".to_owned()))
        .await
        .unwrap();
    wait_for_value(&observer, "before", "1", Duration::from_secs(30)).await;
    assert_eq!(*observer.node().sync_status.borrow(), SyncStatus::Gateway { gateway: proxy });

    observer
        .submit_action(("from-observer".to_owned(), "1".to_owned()))
        .await
        .unwrap();
    for node in remaining.iter().chain([&leader_node]) {
        wait_for_value(node, "from-observer", "1", Duration::from_secs(30)).await;
    }

    /* kill the leader like a process exit; the observer must come back
     * through the proxy against the new leader */
    drop(leader_node);
    leader_rt.shutdown_background();

    let remaining_refs: Vec<_> = remaining.iter().collect();
    wait_for_settled_leader(&remaining_refs, second_leader, Duration::from_secs(60)).await;

    remaining[0]
        .submit_action(("after".to_owned(), "2".to_owned()))
        .await
        .unwrap();
    wait_for_value(&observer, "after", "2", Duration::from_secs(60)).await;

    observer
        .submit_action(("from-observer".to_owned(), "2".to_owned()))
        .await
        .unwrap();
    for node in &remaining {
        wait_for_value(node, "from-observer", "2", Duration::from_secs(30)).await;
    }

    /* the observer never learned the proxy as a peer */
    let peers = observer.node().peers.lock().await;
    assert!(!peers.contains_key(&proxy));
}
