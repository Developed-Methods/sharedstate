# Election implementation workflow

Source: [downloaded plan](etcd-election-plan.html).

1. Add the etcd adapter, configuration, leased owner record, watch recovery, and conservative session deadlines.
2. Replace voting with observed ownership and bounded state preparation.
3. Fence authority and replication with the full epoch and current lease or observation validity.
4. Update startup, the example, and existing election, replication, recovery, relay, and TCP tests.
5. Run formatting, compilation, unit tests, and integration tests against local etcd.
6. Review failure handling and document deployment requirements and verification limits.

Each acquisition uses a fresh session. The owner remains preparing until state preparation and its generation bump complete.
Only the explicitly designated bootstrap node may initialize a new history. Other fresh nodes synchronize before acquiring ownership.
Restored state remains an application responsibility. Etcd stores election metadata only.

## Progress

- Downloaded the source with curl and inspected the existing implementation.
- Completed the adapter, ownership integration, protocol version 2, and voting removal.
- Added guarded state mutations, promotion recovery, bootstrap restrictions, and epoch checks for direct and relayed streams.
- Updated configuration, the TUI, and [deployment instructions](etcd-election-deployment.md).
- Reviewed cancellation during promotion and preserved completion of admitted mutations after revocation.
- Completed all six workflow steps.

## Verification

- `cargo test --all-targets`: 51 library tests and one TCP integration test passed; the TUI compiled.
- `cargo check --all-targets`: passed.
- `cargo clippy --lib -- -D warnings`: passed.
- Rustfmt checks for changed Rust files and `git diff --check`: passed.

Tests used etcd 3.5.16 and protoc 3.21.12. Each integration fixture uses temporary data and isolated listening ports.
The adapter suite includes a three-member quorum-loss test and a simulated lost response after a real acquisition transaction.
Other cases cover simultaneous contenders, compaction, periodic observation refresh, expired permits, and same-address restarts.
Promotion tests cover lagging state recovery and conflicting histories. Bootstrap tests cover fresh candidates and observers.
Replication, recovery, relay, peer partition, action flood, and TCP failover scenarios passed.

TLS and credential options compile but were not exercised against an authenticated TLS deployment.

## Guarantee boundary

Etcd serializes the owner record. Local permits and epoch checks bound stale activity but do not provide linearizable replication.
Actions retain queue-acceptance semantics. External irreversible effects require destination fencing.
