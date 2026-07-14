# Asymmetric reachability can wedge a voter in `Electing` forever with no state sync

**Severity:** Medium
**Files:** `src/cluster/leader.rs:230-234` (`peer_claim` requires `connected`), `src/cluster/leader.rs:343-346` (vote candidates require `connected`), `src/cluster/state_sync.rs:122` (`wait_for_leader` performs no sync)

## Summary

Both the adoption of a `Leading` claim and the set of vote candidates are filtered by
`peer.connected`, which means *our own outbound dial succeeded*. A node that can
receive the leader's gossip (the leader dials it and pushes `LeaderInformation`) but
cannot dial the leader back — one-way firewall/NAT/routing breakage, common between
datacenters — never adopts the claim and never votes for the leader.

Because the rest of the cluster vouches the leader reachable, the stuck node's
`unreachable` flag stays false, so it doesn't trigger a new election either. It sits in
`Electing { vote: someone-else }` at the current term indefinitely. Critically,
`StateSyncTask` in `Electing`/`NoLeader` mode does *no state sync at all*
(`wait_for_leader` just sleeps), so the node's reads go stale forever, and its queued
client actions sit in the channel. The only symptom is a leader-state log line.

Observers have the same `connected` requirement in `next_observer_state`
(`leader.rs:383`), with the same consequence.

## Failure scenario

1. Voter A's outbound dials to leader L fail (one-way firewall rule); L→A traffic works.
2. A marks L `FailedToConnect`, but peers vouch L reachable → not `unreachable`.
3. A adopts the current term from gossip, enters `Electing`, votes for some connected
   candidate; no majority forms for it; L keeps its majority elsewhere.
4. A remains `Electing` forever: no sync source, stale reads, stalled writes, no error.

## Recommended fix

1. **Accept gossiped claims from vouched-reachable voters.** In `next_voter_state`'s
   `Electing`/`NoLeader` branch, extend `peer_claim` to include voters that are
   `!unreachable` (not just `connected`). The claim data arrives via gossip regardless
   of our dial direction, and following a leader we can't dial already works — the sync
   task falls back to relays (`state_sync.rs:189`). Apply the same relaxation to
   `next_observer_state`.
2. **Let electing nodes sync.** Independently of (1), make `wait_for_leader` sync
   defensively: if any connected peer reports `Following { leader }` for the max term,
   relay-sync from it (reusing the existing relay path) while the local election state
   settles. This keeps reads fresh through every election, which matters for a
   read-optimized system.
3. Add a simulated-net test: block only the A→L edge direction (would need a
   directional block in `SimulatedNet`, which currently only supports symmetric edge
   blocks — worth adding for this class of bug).
