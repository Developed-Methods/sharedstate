# Durable consensus upgrade workflow

Source: [downloaded plan](source-plan.html), baseline `5fc00db95bb96e36b5598425fa8cdd04b3f9931f`.
The plan was downloaded with curl and preserved alongside its [extracted text](source-plan.txt).
Implementation and local validation ran in this worktree.
The new API remains behind `experimental-v4`; production promotion gates are incomplete.

## Deployment

Target three or five voters, 20–50 readers, and approximately 60 MB of application state.
The benchmark uses 60 MiB and accepts either voter count.
The production write rate remains unspecified.
See [measurements](measurements.md) for tested configurations and resource limits.

## Executed stages

“Implemented” describes the experimental code, not approval of every source-plan acceptance gate.

| Stage | Result | Evidence and remaining work |
|---|---|---|
| PR01 | Implemented | Invariants recorded; static framing regressions reproduced before the fix. Acceptance coverage appears below. |
| PR02 | Implemented | Openraft 0.9.25, SQLite WAL/FULL, upstream storage suite, durable three-voter prototype. |
| PR03 | Implemented | Protocol v2 handshake, bounded frames and collections, canonical commands, separate RPC lanes. Fuzz campaigns remain. |
| PR04 | Partial | Owned workers, cancellation, deadlines, admission, control and bulk permits. Saturation and scheduler-starvation campaigns remain. |
| PR05 | Implemented | Stable persisted identities, explicit 3/5-voter bootstrap, learner catch-up before replacement. Rolling replacement preserves imported state and receipts. |
| PR06 | Implemented | Committed application, immutable publication, revision waits, failed-apply fencing, checkpoint comparison. Clone cost still limits throughput. |
| PR07 | Implemented | Durable receipts, deduplication, payload validation, retirement, cancellation ambiguity, one-hop forwarding. Automatic redirect retries remain. |
| PR08 | Implemented | Verified atomic installation, durable chunk resume for voters/readers, checkpoint leases, bounded transfers. Manifest crash-point campaign remains. |
| PR09 | Partial | Nonvoting replicas, committed suffix validation, source rotation, divergence recovery. Process-incarnation wire fencing and relay serving remain. |
| PR10 | Partial | Status watches, critical-task health, checkpoint mismatch detection. Full diagnostics registry and legacy removal remain gated. |
| PR11 | Partial | Local fault tests, real process kills, TCP cleanup, storage suite, 60 MiB measurements. Distributed capacity and expanded fault matrix remain. |
| PR12 | Partial | Legacy adapter, verified export/import, cutover and rollback instructions. Application-specific migration rehearsal and release promotion remain. |

## Repeatable workflow

Run `bash scripts/validate-upgrade.sh` for formatting, default tests, experimental tests, and Clippy.
The [validation record](validation.md) summarizes local results.
The GitHub Actions workflow runs the same script on pushes and pull requests.
Run the benchmark separately; it allocates full application states and writes durable databases.
Use a deployment-sized environment for simultaneous recovery and sustained-write acceptance.
Record the admitted write rate before approving the recovery envelope.
Keep the experimental feature gate until the remaining acceptance cases pass.

## Acceptance coverage

Tests with a source-plan ID cover the described subset, not every variant of that acceptance case.

| Case | Local evidence | Remaining acceptance work |
|---|---|---|
| T01 | Minority blackhole prevents commitment; majority commits and converges | Full workload and shipped-timer recovery measurement |
| T02 | RPC deadlines and stalled-handshake shutdown | Stalled RPCs alongside continuing election traffic |
| T03 | Sequential replica owner invalidates failed source sessions | Continuous obsolete-source traffic under scheduler pressure |
| T04 | Stable identity and exact committed-prefix checks | Same-address reincarnation campaign and explicit incarnation handshake |
| T05 | Explicit bootstrap refuses automatic cluster creation; replicas retry sources | All-seeds recovery after simulated 30-minute partition |
| T06 | Separate bounded control, client, and bulk admission | More than eight dead peers with saturated ingress |
| T07 | Lost response after commit, same-ID retry, leader failover | Continuous randomized write/failover histories |
| T08 | Duplicate, changed payload, sequence gaps, retirement, full restart | Client crash-point campaign |
| T09 | Every member replaced sequentially; imported state and receipts survive leader changes | Distributed full-state replacement rehearsal |
| T10 | Three actual TCP processes killed and restarted; retry deduplicates | Old-replica restart during full-workload recovery |
| T11 | Three-voter partition and five-voter two-loss tests | Directed partitions, reordered messages, repeated election flaps |
| T12 | 60 MiB reader transfers exceed two seconds and complete | Bounded-bandwidth voter and reader tests |
| T13 | Durable offsets survive restart; voter resumes after lost chunk response; old checkpoint leases survive replacement | Process kills around every manifest/compaction boundary |
| T14 | Pinned immutable reader survives updates and election | Full-state clone and retained-history memory envelope |
| T15 | Missing log history selects snapshot recovery | One stalled reader while others sustain production traffic |
| T16 | TCP handshake socket closes during shutdown; cooperative task joins | Full queues, blocked storage, idle subscribers together |
| T17 | Mutating apply panic stops readiness and acknowledgements; SQLite write failure test | Disk-full failpoints during live quorum writes |
| T18 | Oversized frames, trailing data, nesting, corrupt digest, chunk gaps, metadata mismatch | Fuzzing and measured allocation ceilings |
| T19 | Static and dynamic framing regression tests pass | Covered locally |
| T20 | Sustained 3/5-voter, 20-reader/60 MiB measurements; 50-reader small-state functional test | 50-reader full-state distributed run; admitted-rate and replay-rate proof |
| T21 | Divergent reader becomes unready and installs verified state | Multi-voter divergence and operator recovery rehearsal |

## Release gate

The requested production upgrade is not complete until the pending acceptance work passes.
The default v3 entry point remains available, with bounded framing and peer-list fixes.
No package was published and no deployment was changed.
Application-specific deterministic behavior and external effects require migration rehearsal against the actual application.

The first baseline run reused cached artifacts containing probes absent from tracked source.
Subsequent baseline and upgrade checks use `target/upgrade` to avoid that contamination.
