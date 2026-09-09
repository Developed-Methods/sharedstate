# Local upgrade measurements

Target: three or five voters, 20–50 readers, approximately 60 MB of state.
The production write rate has not been specified.
Tests use 60 MiB of JSON string state, a counter command, and scalar results.
Application-specific mutation and serialization costs may differ substantially.

## Environment

These measurements used one shared host with 8 GB RAM and four Tokio workers.
All voters and readers ran within one process using SimulatedNet.
No RTT, loss, or bandwidth limit was injected.
SQLite databases used the workspace filesystem with WAL and FULL synchronization.
Readers joined sequentially and remained live during writes.
The benchmark waits for every reader to observe the final committed revision.

The initial benchmark used `/tmp`, which is a 3.9 GB tmpfs on this host.
An overlapping run exhausted that filesystem.
Later runs use `target/benchmark-data` on the workspace disk.
Comparisons spanning that storage change are not controlled performance comparisons.

## Development measurements

These runs preceded the final checkpoint-lease and voter-resume changes.
They are development evidence, not final production capacity results.
Raw JSON is preserved in [benchmark-results.json](benchmark-results.json).

| Voters | Readers | State | Writes | Chunk size | Writes/s | Commit p50 | Commit p95 | Reader catch-up |
|---|---|---|---|---|---|---|---|---|
| 3 | 20 | 60 MiB | 1,000 | 64 KiB | 5.64 | 183.95 ms | 346.09 ms | 9.44–12.41 s each |
| 3 | 1 | 60 MiB | 1,000 | 256 KiB | 627.38 | 0.94 ms | 4.14 ms | 4.12 s |

Both runs use 100 ms publication intervals.
The 20-reader write phase lasted approximately 177 seconds.
The one-reader write phase lasted approximately 1.6 seconds and does not establish long-duration throughput.
The disparity exposes shared-host replication and copying costs.
It does not predict distributed production throughput.

## Five-voter workload

A later build includes checkpoint leases and resumable voter transfer.
It precedes the final checkpoint-ordering guard, which was separately regression-tested.
The run used five voters, 20 readers, 60 MiB of state, and 1,000 writes.
Other local validation and compilation overlapped this run on the shared host.

| Measurement | Result |
|---|---|
| Sustained write rate | 3.58 writes/s |
| Write phase duration | Approximately 280 seconds |
| Commit p50 / p95 / maximum | 259.93 / 563.60 / 1,131.86 ms |
| Sequential reader catch-up | 3.62–8.58 seconds each |
| Peak resident memory | 4,163,808 KiB, approximately 3.97 GiB |
| Complete benchmark duration | 388.60 seconds |
| Final convergence | All 20 readers observed the final receipt and counter value |

This is evidence of resource pressure on one shared host.
It is not a distributed capacity estimate or a production admission limit.

## Limits

The 50-reader integration test uses small counter state.
Fifty readers with 60 MiB each were not benchmarked on this host.
No simultaneous full-cluster recovery measurement establishes the plan's 60-second SLO.
RTT, durable-write latency, clone duration, and replay headroom require separate instrumentation and deployment measurements.
The current full-state cloning and JSON snapshots remain resource risks for frequent publication.

Run `cargo run --release --locked --features experimental-v4 --example v4_benchmark -- 20 60 1000 3` to reproduce the workload.
Use a larger environment before running 50 full-state readers together.
