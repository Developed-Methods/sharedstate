use serde::{Deserialize, Serialize};
use sharedstate::v4::{Node, NodeConfig, Operation, OperationId, ReplicatedState, TcpTransport};
use std::{collections::BTreeMap, io::Write, net::SocketAddrV4, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    time::Instant,
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Counter(u64);
impl ReplicatedState for Counter {
    type Command = u64;
    type Result = u64;
    const SCHEMA_VERSION: u32 = 1;
    fn apply(&mut self, amount: u64) -> u64 {
        self.0 += amount;
        self.0
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err("usage: v4-counter DATABASE CLUSTER_UUID NODE_ID IPV4:PORT".into());
    }
    let node = Node::open(
        NodeConfig::new(args[1].clone().into(), args[2].parse()?, args[3].parse()?),
        Arc::new(TcpTransport::bind(args[4].parse()?).await?),
        Counter::default(),
    )
    .await?;
    println!("ready");
    std::io::stdout().flush()?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let words: Vec<_> = line.split_whitespace().collect();
        let result = match words.as_slice() {
            ["bootstrap", members @ ..] => {
                let mut voters = BTreeMap::new();
                for member in members {
                    let (id, address) = member.split_once('=').ok_or("expected NODE=IPV4:PORT")?;
                    voters.insert(id.parse()?, TcpTransport::encode(address.parse::<SocketAddrV4>()?));
                }
                serde_json::json!({"ok": node.bootstrap(voters).await.is_ok()})
            }
            ["status"] => {
                let status = node.status();
                serde_json::json!({"leader": status.leader, "ready": status.ready, "value": node.read_snapshot().state.0, "revision": status.revision})
            }
            ["submit", client, sequence, value] => {
                let id = OperationId {
                    client_id: client.parse()?,
                    sequence: sequence.parse()?,
                };
                match node
                    .submit(
                        Operation {
                            id,
                            command: value.parse()?,
                        },
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await
                {
                    Ok(receipt) => {
                        serde_json::json!({"ok": true, "result": receipt.result, "revision": receipt.revision})
                    }
                    Err(error) => serde_json::json!({"ok": false, "error": format!("{error:?}")}),
                }
            }
            ["shutdown"] => break,
            _ => serde_json::json!({"error": "unknown command"}),
        };
        println!("{result}");
        std::io::stdout().flush()?;
    }
    node.shutdown(Instant::now() + Duration::from_secs(5)).await?;
    Ok(())
}
