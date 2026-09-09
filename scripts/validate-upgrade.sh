#!/usr/bin/env bash
set -euo pipefail
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target/upgrade}"
rustfmt --edition 2024 --config skip_children=true --check src/lib.rs src/service.rs src/protocol/framing.rs src/protocol/messages.rs src/v4/*.rs tests/framing_regressions.rs tests/v4_*.rs tools/v4_counter.rs examples/v4_benchmark.rs
cargo test --locked --all-targets
cargo test --locked --all-targets --features experimental-v4
cargo clippy --locked --all-targets --features experimental-v4 -- -D warnings
