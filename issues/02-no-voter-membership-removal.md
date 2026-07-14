# Voter membership only grows — dead voters count toward quorum forever

**Severity:** High
**Files:** `src/cluster/node_state.rs` (`peers` map, `merge_peer_details`), `src/cluster/leader.rs:225` (`voter_count`), `src/cluster/leader.rs:148` (`is_voter`)

## Summary

The `peers` map in `NodeState` has no removal path, no expiry, and no membership-change
API; `merge_peer_details` only ever adds entries. A peer with `can_lead = Some(true)`
counts in `voter_count` even when unreachable. That is the correct conservative choice
for *transient* failures, but if half or more of the ever-known voters are permanently
decommissioned or destroyed, no strict majority is achievable again and the cluster can
never elect a leader — it is bricked until the dead addresses come back.

Gossip makes this unrecoverable in practice: even restarting the survivors with clean
`initial_peers` re-learns the dead voters from other nodes' `SharePeers` responses, so
zombie voter records propagate cluster-wide forever. This directly conflicts with the
project goal of surviving datacenters going offline (including permanently). It is also
an unbounded-memory issue when addresses churn (e.g. the TCP example keys peers by
ephemeral port).

## Failure scenario

1. 6-voter cluster across 6 DCs, running for months.
2. Three DCs are permanently decommissioned.
3. Remaining 3 voters compute `voter_count = 6`; majority requires 4 supporters.
4. Election never completes; no leader; no writes accepted, ever.
5. Operators restart survivors with fresh peer lists — gossip from the other survivors
   re-adds the dead voters within one `observation_interval`.

## Recommended fix

Two layers, in order of preference:

1. **Voter expiry policy using data already gossiped.** `last_global_connectivity`
   is already tracked and merged cluster-wide. Add a configurable
   `voter_expiry: Duration` (e.g. 24h; default off for backward compatibility). In
   `LeaderTask::tick`, treat a voter whose `last_global_connectivity` is older than the
   expiry as a non-voter for quorum purposes (both `voter_count` and support counts),
   and prune entries past a second, longer threshold from the `peers` map so
   `SharePeers` stops propagating them. Because the timestamp is monotone-merged via
   gossip, all nodes converge on the same view, keeping the quorum computation
   consistent. Note wall-clock skew caveats (see issue 12) — use generous windows.
2. **Explicit membership operations.** Longer term, add `add_voter` / `remove_voter`
   administrative actions that flow through the replicated log itself (as
   `RecoverableStateAction` variants), so membership changes are ordered by the leader
   and survive via the same replication path — the standard Raft-style approach. This
   also gives operators a deliberate scale-down story instead of relying on expiry.

Also document that `initial_peers` addresses must be stable identities (not ephemeral
ports) since they are permanent map keys.
