# Deploy etcd leader election

Etcd stores one leased owner record per cluster. Sharedstate continues storing application state and replicating actions through its peer network.

The owner key is `/sharedstate/{cluster_id}/{election_incarnation}/leader`. Its creation revision and the configured incarnation identify a leadership epoch.

## Configure participants

Use the same endpoints, cluster ID, and incarnation for every participant. Keep cluster IDs distinct between independent applications.

```rust
use sharedstate::{SharedStateSettings, cluster::election::EtcdElectionConfig};

let settings = SharedStateSettings {
    election: EtcdElectionConfig {
        endpoints: vec!["http://127.0.0.1:2379".into()],
        cluster_id: "orders".into(),
        election_incarnation: 1,
        bootstrap: false,
        ..Default::default()
    },
    ..Default::default()
};
```

Defaults request a 15-second lease, renew every five seconds, and reserve a three-second authority safety margin. Requests time out after two seconds.

Startup remains synchronous. Inspect `node().election_status()` for connection errors, owner identity, epoch, readiness, and permit validity.

For production, provision a three-member etcd cluster. Configure `tls_configuration` and scoped `credentials` for access to the application’s owner key.

`initial_peers` still supplies discovery and recovery sources. The advertised owner address must remain reachable directly or through an existing relay.

## Start a new cluster

1. Start etcd.
2. Designate one node containing the intended initial application state.
3. Set that node’s `can_lead` and `election.bootstrap` fields to `true`.
4. Start other nodes with `bootstrap: false`.
5. Wait for a ready owner and synchronized replicas before starting producers.

A fresh candidate cannot acquire ownership until it synchronizes. An observer with `can_lead: false` never acquires ownership, including when bootstrap is enabled.

For local development, run the TUI against a single-node etcd instance:

```sh
etcd --data-dir /tmp/sharedstate-etcd
```

Start the designated bootstrap node:

```sh
cargo run --example kv_tui -- \
  --etcd-endpoints http://127.0.0.1:2379 \
  --cluster-id demo --incarnation 1 --bootstrap
```

Start another node using the bootstrap node’s displayed port:

```sh
cargo run --example kv_tui -- \
  --etcd-endpoints http://127.0.0.1:2379 \
  --cluster-id demo --incarnation 1 --peers 41001
```

Replace `41001` with the displayed port. Add `--observer` for a participant that must never lead.

The TUI displays the current epoch, owner freshness, local authority validity, and election errors. Its application state is in memory.

## Cut over from voting

The new wire protocol is version 2. Legacy voting and etcd ownership must never run together in one logical cluster.

1. Pause action producers.
2. Preserve application state through the existing recovery mechanism.
3. Stop every participant running the old election algorithm.
4. Configure the shared etcd namespace and incarnation.
5. Start the chosen prepared node using `SharedState::start_recoverable` and its saved `RecoverableState`.
6. Start the remaining participants and wait for synchronization.
7. Resume producers after observing one ready owner and converged replicas.

`start_recoverable` makes supplied recovery state eligible for promotion. Use it only with application-approved recovery state.

An existing cluster must not use an empty bootstrap node as a recovery shortcut. Etcd contains no application snapshot or action log.

## Restart, restore, and rollback

After all application nodes stop, restore their application state through the existing recovery mechanism. An owner record cannot reconstruct that state.

If etcd is restored or replaced, provision a new election incarnation. Restart all participating applications together with that incarnation and preserved application state.

Changing the incarnation changes the owner key. Participants using different incarnations can each acquire ownership, so the coordinated restart is required.

To roll back:

1. Pause producers and preserve current application state.
2. Stop every etcd-controlled participant.
3. Restart the legacy version using the preserved recovery state.
4. Resume producers after the legacy cluster converges.

## Failure boundaries

A peer partition does not authorize replacing a live etcd owner. Followers can use relays only for the validated owner and epoch.

Loss of etcd access invalidates authority through failed renewal or the conservative permit deadline. Local application reads remain available.

Dropping a node revokes its local permit immediately. Its etcd lease then expires naturally; failed promotion explicitly revokes the lease.

`submit_action` reports queue acceptance. It does not report durable commitment or guarantee that an accepted action survives failover.

Replication retains its existing asynchronous loss boundary. Etcd serializes owner records but cannot atomically fence in-memory application updates during arbitrary process pauses.

External irreversible effects require fencing at their destination. Lease ownership alone does not make those effects linearizable.

## Run the tests

Install `etcd` and `protoc` before building and running integration tests. Set `ETCD_BIN` if the etcd executable is outside `PATH`.

```sh
cargo test --all-targets
```

Tests start isolated etcd processes with temporary data directories. The adapter tests also exercise quorum loss against a three-member etcd cluster.
