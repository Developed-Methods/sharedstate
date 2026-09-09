# Validation record

Checks ran on 2026-09-09 with rustc 1.98.0.
All commands use `CARGO_TARGET_DIR=target/upgrade` and the committed dependency lockfile.
The source baseline is `5fc00db95bb96e36b5598425fa8cdd04b3f9931f`.

## Baseline regressions

The clean-source baseline passed 63 library tests and one TCP integration test.
Two new static-message framing regressions failed before the header fix.
The fixed framing suite passes all four static, dynamic, trailing-byte, and oversized-header cases.
Peer-list decoding now rejects oversized counts before allocation.

## Repeatable checks

`bash scripts/validate-upgrade.sh` checks both feature configurations and all targets.
It also checks formatting on touched Rust files and runs Clippy with warnings denied.
Existing untouched files are not uniformly rustfmt-clean, so formatting checks use an explicit file list.

The default configuration passes 64 library tests, four framing regressions, and one TCP integration test.
The experimental configuration passes 94 tests: 76 library tests and 18 integration tests.
Experimental tests include the complete upstream Openraft storage suite.
The final Clippy run passed with warnings denied.
Formatting and local documentation links also passed their checks.
They also exercise receipts, partitions, voter replacement, 50 small-state readers, snapshot recovery, and actual process kills.

The rolling replacement test replaces all three original voters, including the leader.
It preserves imported application state and duplicate-operation receipts.
The snapshot transport test drops a response after persisting a chunk, restarts receiver storage, and verifies resumed transmission.
A separate regression prevents late checkpoint completion from replacing a newer installed manifest.
The failed-apply test mutates state before panicking and confirms that partial state never publishes.

## Interpretation

Passing these tests does not establish the complete production fault matrix or recovery SLO.
The [acceptance table](workflow.md) records the remaining cases.
The [measurements](measurements.md) record workload size, topology, and the limits of the local capacity evidence.
