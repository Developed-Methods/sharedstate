use serde::{Deserialize, Serialize};
use sharedstate::{
    transport::simulated::SimulatedNet,
    v4::{Node, NodeConfig, Operation, OperationId, ReadReplica, ReplicatedState},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Instant;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct State {
    padding: String,
    counter: u64,
}
impl ReplicatedState for State {
    type Command = u64;
    type Result = u64;
    const SCHEMA_VERSION: u32 = 1;
    fn apply(&mut self, amount: u64) -> u64 {
        self.counter += amount;
        self.counter
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let readers: u64 = args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(20);
    let mib: usize = args.get(2).map(|s| s.parse()).transpose()?.unwrap_or(60);
    let writes: u64 = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(1000);
    let voter_count: u64 = args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(3);
    if ![3, 5].contains(&voter_count) || readers > 50 || mib > 100 || writes == 0 {
        return Err("expected 3 or 5 voters, at most 50 readers, at most 100 MiB, and positive writes".into());
    }
    std::fs::create_dir_all("target/benchmark-data")?;
    let directory = tempfile::tempdir_in("target/benchmark-data")?;
    let cluster = uuid::Uuid::new_v4();
    let net = SimulatedNet::new();
    let mut voters = BTreeMap::new();
    let started = Instant::now();
    for id in 1..=voter_count {
        let node = Node::open(
            NodeConfig::new(directory.path().join(format!("{id}.db")), cluster, id),
            net.start_io(id).await,
            State {
                padding: "x".repeat(mib * 1024 * 1024),
                counter: 0,
            },
        )
        .await?;
        voters.insert(id, node);
    }
    voters[&1]
        .bootstrap((1..=voter_count).map(|id| (id, id)).collect())
        .await?;
    let leader = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            for (id, voter) in &voters {
                if voter.status().leader == Some(*id) && voter.status().ready {
                    return *id;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    voters[&leader]
        .checkpoint(Instant::now() + Duration::from_secs(60))
        .await?;
    let revision = voters[&leader]
        .read_snapshot()
        .revision
        .ok_or("missing committed base")?;
    eprintln!("voter startup and checkpoint: {:?}", started.elapsed());
    let mut replicas = Vec::new();
    let mut catch_up_ms = Vec::new();
    // Stage replica joins to keep this single-host measurement within its memory budget.
    for id in 10..10 + readers {
        let started = Instant::now();
        let replica = ReadReplica::open(
            NodeConfig::new(directory.path().join(format!("reader-{id}.db")), cluster, id),
            net.start_io(id).await,
            State::default(),
            BTreeMap::from([(leader, leader)]),
        )
        .await?;
        replica
            .wait_for_revision(revision, Instant::now() + Duration::from_secs(60))
            .await?;
        let elapsed = started.elapsed().as_millis();
        eprintln!("replica {} snapshot catch-up: {elapsed} ms", id - 9);
        catch_up_ms.push(elapsed);
        replicas.push(replica);
    }
    let client_id = uuid::Uuid::new_v4();
    let mut latency_us = Vec::new();
    let started = Instant::now();
    let mut last = revision;
    for sequence in 1..=writes {
        let before = Instant::now();
        let receipt = voters[&leader]
            .submit(
                Operation {
                    id: OperationId { client_id, sequence },
                    command: 1,
                },
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .map_err(|e| format!("{e:?}"))?;
        latency_us.push(before.elapsed().as_micros());
        last = receipt.revision;
    }
    let seconds = started.elapsed().as_secs_f64();
    for replica in &replicas {
        replica
            .wait_for_revision(last, Instant::now() + Duration::from_secs(60))
            .await?;
        assert_eq!(replica.read_snapshot().state.counter, writes);
    }
    latency_us.sort_unstable();
    println!(
        "{}",
        serde_json::json!({"voters": voter_count, "readers": readers, "state_mib": mib, "writes": writes, "writes_per_second": writes as f64 / seconds, "commit_p50_us": latency_us.get(latency_us.len() / 2), "commit_p95_us": latency_us.get(latency_us.len() * 95 / 100), "commit_max_us": latency_us.last(), "snapshot_catch_up_ms": catch_up_ms, "transport": "same-process SimulatedNet, no injected latency", "join_mode": "sequential"})
    );
    for replica in replicas {
        replica.shutdown(Instant::now() + Duration::from_secs(10)).await?;
    }
    for (_, voter) in voters {
        voter.shutdown(Instant::now() + Duration::from_secs(10)).await?;
    }
    Ok(())
}
