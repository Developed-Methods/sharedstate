# sharedstate

Synchronize heavily read application state across servers.

The default API remains v3. The durable rebuild is available behind `experimental-v4`.
It is not yet approved for production migration.
The [upgrade workflow](docs/upgrade/workflow.md) records implementation status and remaining release gates.

## Experimental durable API

`v4::Node` uses Openraft 0.9.25 with a SQLite WAL/FULL backend.
Bootstrap explicitly with three or five voters.
Use `NodeConfig::persistent` to create or reopen a random node identity independently from its address.
Ordinary `ReadReplica` instances stay outside voting membership.

Implement `ReplicatedState::apply` as a deterministic operation without external side effects.
Submit operations with a persisted client UUID and increasing sequence.
Successful receipts identify committed, applied operations.
Retry uncertain outcomes with the same ID and command.
Use `wait_for_revision` when a particular reader must observe a receipt.

Read handles contain an immutable `Arc`, committed revision, and publication time.
Local reads may be stale. Retaining a handle does not block application or publication.
The publication interval defaults to 100 ms and can be configured for the application's clone cost.
Call `checkpoint` at explicit barriers for state digest comparisons.
Use `verify_voter_checkpoints` after all voters reach the same quiesced barrier.

[Protocol limits](docs/architecture/protocol-contract.md) describe the current resource and timing contracts.

[Migration instructions](docs/migration/v4.md) describe the legacy adapter, export/import, and rollback limits.
[Measurements](docs/upgrade/measurements.md) record the tested workloads and their limits.

## Validate

Run `bash scripts/validate-upgrade.sh`.
The script checks formatting, both feature configurations, integration tests, and Clippy.

Run `cargo run --release --features experimental-v4 --example v4_benchmark -- 20 60 1000` for the measured workload.
Arguments specify reader count, state size in MiB, operation count, and optional voter count (three or five).
This benchmark stages replica joins on one host and uses its workspace filesystem.

Run `cargo run --features experimental-v4 --bin v4-counter -- DATABASE CLUSTER_UUID NODE_ID IPV4:PORT` for the TCP process tool.
Its standard input accepts `bootstrap`, `status`, `submit`, and `shutdown` commands.
The process tests provide a complete three-voter example.
