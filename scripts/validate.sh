#!/usr/bin/env bash
set -euo pipefail
cargo fmt --all -- --check
cargo test --locked --all-targets
cargo test --locked --doc
cargo clippy --locked --all-targets -- -D warnings
