# Issues

Findings from a full review of the library (2026-07-08), evaluated against the project
goals: shared state across 20+ datacenters, low-latency local reads, writes may be slow
or dropped, must tolerate datacenters and peers going offline.

Suggested priority order: **02 → 01 → 04 → 03**, then the medium tier.

## High severity

| # | Issue | Area |
|---|-------|------|
| [01](01-action-forwarding-loops.md) | Forwarded actions can loop forever between relaying followers; idle subscriptions have no progress timeout | state_sync / relay |
| [02](02-no-voter-membership-removal.md) | Voter membership only grows — permanently dead voters count toward quorum forever, bricking elections | membership / quorum |
| [03](03-framing-static-size-header-never-written.md) | Length header never written for statically-sized messages (latent stream corruption) | framing |
| [04](04-unbounded-network-allocations.md) | Unvalidated frame/collection sizes allow pre-auth remote OOM | framing / decoding |

## Medium severity

| # | Issue | Area |
|---|-------|------|
| [05](05-split-brain-and-read-visibility-of-dropped-writes.md) | Quorum uses locally-known voter set; readers can observe writes that later vanish, unobservably | election / read model |
| [06](06-drop-leaks-child-tasks-zombie-node.md) | `Drop` leaks child tasks — dropped node becomes a keepalive-fed zombie that stalls peers | lifecycle |
| [07](07-asymmetric-connectivity-wedges-electing-voter.md) | One-way reachability wedges a voter in `Electing` forever with no state sync | election / liveness |

## Low severity / hygiene

| # | Issue | Area |
|---|-------|------|
| [08](08-dead-progress-recover-and-history-gossip-overhead.md) | Dead `progress_recover`; full generation history gossiped every 3s (~48 KB/msg at cap) | recoverable state / gossip |
| [09](09-election-term-nonce-time-derived.md) | Election-root nonces are purely time-derived; distinct roots can collide | election |
| [10](10-wall-clock-dependence.md) | Wall-clock timestamps compared across machines; non-monotonic time in liveness heuristics | time |
| [11](11-submit-action-blocks-indefinitely.md) | `submit_action` blocks unboundedly on a full queue with no leader; silent action drops | API / backpressure |
| [12](12-write-channel-flush-per-message.md) | Per-message flush on the replication fan-out path | transport / perf |
| [13](13-no-persistence-full-restart-loses-state.md) | No persistence — simultaneous voter restart silently loses all state | durability |
