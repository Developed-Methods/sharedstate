# sharedstate

Replicate deterministic application state with Openraft consensus and durable SQLite storage.
Use three or five voters and nonvoting read replicas for heavily read workloads.

## Application state

Implement `ReplicatedState` with serializable state, commands, and results.
Keep `apply` deterministic and free of external side effects.

```rust
use serde::{Deserialize, Serialize};
use sharedstate::ReplicatedState;

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
```

Create each voter with `Node::open`, its own database, and the same cluster UUID, initial state, schema, and session limit.
Use `NodeConfig::persistent` to create or reopen its stable identity independently from its transport address.
Bootstrap once with an explicit map of three or five voter identities and addresses.
Use `replace_voter` for membership changes.
Replacement voters install a complete checkpoint before promotion.

Submit commands through `Node::submit` with a persisted client UUID and increasing sequence, starting at one.
Successful receipts identify committed, applied operations.
Retry uncertain outcomes with the same operation ID and command.
Allow one outstanding operation per client session.

## Readers

Use `ReadReplica` for readers outside voting membership.
`read_snapshot` returns an immutable `Arc` with its committed revision and publication time.
Local reads may be stale.
Use `wait_for_revision` when a reader must observe a particular receipt.
Retaining a snapshot does not prevent subsequent publication.

Snapshots transfer in verified, resumable chunks.
SQLite stores votes, logs, snapshots, and client-session receipts durably.
Call `shutdown` with a deadline to await cooperative task and storage cleanup.

See [protocol limits](docs/architecture/protocol-contract.md) and [architecture decisions](docs/architecture/decisions.md) for the operating contracts.
See [measurements](docs/measurements.md) for the tested 3/5-voter, 20-reader, 60 MiB workloads.

## Run

Run `bash scripts/validate.sh` for formatting, tests, and Clippy.

Run the workload benchmark:

```sh
cargo run --release --locked --example benchmark -- 20 60 1000 3
```

Arguments specify readers, state size in MiB, operation count, and voter count.

Run the TCP counter tool:

```sh
cargo run --locked --bin sharedstate-counter -- DATABASE CLUSTER_UUID NODE_ID IPV4:PORT
```

Its standard input accepts `bootstrap`, `status`, `submit`, and `shutdown` commands.
The [process test](tests/process.rs) demonstrates a complete three-voter cluster.
