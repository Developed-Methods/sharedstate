# `ElectionTerm` nonces are purely time-derived — distinct election roots can collide

**Severity:** Low
**Files:** `src/protocol/messages.rs:62-66` (`bump`), `src/protocol/messages.rs:43-48` (`Default`)

## Summary

The nonce that distinguishes "different election roots" at the same term number is
derived only from the wall clock:

```rust
pub fn bump(self) -> Self {
    let term = self.term();
    let epoch = now_ms();
    ElectionTerm((term.saturating_add(1) << 32) | (epoch as u32 ^ (epoch >> 32) as u32) as u64)
}
```

Two nodes bumping within the same millisecond mint the *same* nonce. For the common
case — several followers reacting to the same leader failure within one tick — merging
their elections into one root is arguably desirable (votes count across, convergence is
faster). But the nonce mechanism exists to protect *independently formed* clusters and
partitions from having their claims and votes conflated, and that guarantee silently
fails whenever clocks align at millisecond granularity (same-ms cold starts via
`Default`, or NTP-synced DCs bumping simultaneously on both sides of a partition).
`leader_starts_new_election_when_a_different_root_reaches_its_term` protection then
never triggers, and votes cast in one partition's election can count toward a majority
in the other's.

Term numbers also occupy only the high 32 bits (`term << 32`), so `term()` wraps after
2^32 bumps — not a practical concern, but worth a debug assertion.

## Recommended fix

Salt the nonce with node identity in addition to time, e.g.:

```rust
pub fn bump_with_salt(self, salt: u32) -> Self {
    let epoch = now_ms();
    let nonce = (epoch as u32 ^ (epoch >> 32) as u32) ^ salt;
    ElectionTerm((self.term().saturating_add(1) << 32) | nonce as u64)
}
```

where `salt` is a hash of the node address (available at every `bump` call site in
`leader.rs`, which has `me.addr` / `self.state.my_address` in scope). This keeps the
deliberate "simultaneous bumps in one connected cluster merge" behavior *only* when the
same node re-bumps, and makes cross-partition collision probability negligible. If the
merge-on-simultaneous-failure behavior is considered load-bearing, document it
explicitly instead and accept the cross-cluster collision risk consciously.
