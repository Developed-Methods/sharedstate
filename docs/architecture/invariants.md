# v4 invariants

| ID | Contract |
|---|---|
| I01 | Only Openraft controls voting, terms, membership, and commitment. Discovery cannot change membership. |
| I02 | Every published snapshot is a committed log prefix. |
| I03 | Acknowledged operations survive failover while the required durable quorum remains recoverable. |
| I04 | An operation ID cannot apply twice within its client session contract. |
| I05 | Install only complete, verified snapshots and compatible ordered suffixes. |
| I06 | Reject obsolete session completions before state mutation. |
| I07 | Bound network waits and admission with cancellation, deadlines, and typed outcomes. |
| I08 | Retained readers and slow subscribers cannot block consensus application. |
| I09 | Own every task; make critical failures visible through readiness. |
| I10 | Validate recovery within 60 seconds after a stable majority and functioning routes become available. |

I10 requires measured workload and resource limits. It is not a guarantee for arbitrary state sizes or network capacity.
Disconnected nodes may retain stale leadership information. They must never acknowledge conflicting commits.
Historical committed entries remain valid across leader terms when their log prefix and source session are compatible.
