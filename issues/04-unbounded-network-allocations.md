# Network-controlled allocations are unbounded — pre-auth memory DoS

**Severity:** High
**Files:** `src/protocol/framing.rs:70-72` (`read_message_to_vec`), `src/protocol/messages.rs:404-411` (`read_vec`), `src/state/recoverable_state.rs:230-239` (`RecoverableStateDetails::read_from`)

## Summary

Frame and collection sizes read off the wire are trusted without any cap:

- `read_message_to_vec` does `buffer.resize(msg_len, 0)` for any length up to
  `u32::MAX - 1` (~4 GiB) — and this happens *before* any handshake validation, so any
  peer that can open a TCP connection to a listener can trigger it with a 4-byte header.
- `read_vec` does `Vec::with_capacity(count)` from a raw network `u64` before reading a
  single element, so a small message claiming a huge count pre-allocates
  `count × size_of::<T>()` bytes.
- `RecoverableStateDetails::read_from` does the same for its `history` VecDeque.

For a system whose listeners are exposed across WAN links between 20+ datacenters, this
is a trivially exploitable remote OOM. Grown buffers are also never shrunk, so even
legitimate large messages permanently pin memory per connection.

## Failure scenario

1. Attacker (or a corrupted/desynced peer — see issue 03) connects to any node's listener.
2. Sends header bytes `FF FF FF FE`.
3. Node allocates ~4 GiB and waits `process_timeout` for the body; a handful of parallel
   connections OOM-kills the process. No authentication step precedes this.

## Recommended fix

1. **Max frame size on read.** Add `max_message_size: u32` to `NetIoSettings`
   (default e.g. 64 MiB, configurable because `FreshState` snapshots scale with user
   state). In `read_message_to_vec`, reject frames above it with a dedicated
   `ReadMessageError::FrameTooLarge` and close the connection. Mirror the check in
   `send_message` so a node never emits a frame its peers would reject.
2. **Bound collection preallocation.** In `read_vec` (and the `history` decode), cap the
   initial `with_capacity` at a sane constant (e.g.
   `count.min(4096)`) and let `push` grow beyond it; the frame-size cap from (1) then
   bounds the true total. Alternatively validate `count × T::STATIC_SIZE` against the
   remaining frame length when the element size is known.
3. **Shrink or drop oversized buffers** after use in `ReadChannel` (e.g.
   `buffer.shrink_to(2048)` after handling a large frame) so one big snapshot doesn't pin
   memory on every long-lived connection.
