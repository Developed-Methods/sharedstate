# Framing never writes the length header for statically-sized messages

**Severity:** High (latent — no current wire message triggers it)
**Files:** `src/protocol/framing.rs:171-178` (`send_message`)

## Summary

`send_message` seeds the buffer with a zeroed 4-byte length header, encodes the message,
and then only backfills the header in the `else` branch:

```rust
/* if static size is known, we've already written size with MAX_SIZE var */
if let Some(size) = M::STATIC_SIZE {
    debug_assert_eq!(size, bytes_written, ...);
}
/* write size to start of buffer */
else {
    buffer[..MESSAGE_HEADER_SIZE].copy_from_slice(&(bytes_written as MessageSizeHeader).to_be_bytes());
}
```

When `M::STATIC_SIZE` is `Some`, the header is left as zeros — and a zero-length frame
is the KEEPALIVE marker (`KEEPALIVE_FRAME_SIZE = 0`). The receiver reads a keepalive,
then misinterprets the message body bytes as the next frame header, permanently
desyncing the stream (or worse, treating 4 payload bytes as a huge length and stalling
or over-allocating). The comment describes behavior that does not exist anywhere.

This is latent today only because `SyncRequest`/`SyncResponse` use the message-encoding
default `STATIC_SIZE = None`. But primitives are statically sized in that crate
(`u64::STATIC_SIZE = Some(8)`), so anyone who frames a primitive directly or adds a
fixed-size protocol message hits silent stream corruption. The existing framing unit
test that sends a `u64` only asserts an error path, so nothing catches it.

## Failure scenario

1. A future refactor adds `const STATIC_SIZE` to `SyncResponse` (or a new fixed-size
   message type is sent via `send_message`).
2. Every send emits `[00 00 00 00][payload…]`.
3. The peer's `read_message_to_vec` returns `KeepAlive`, then reads payload bytes as a
   frame header. All subsequent traffic on the connection is garbage; depending on the
   bytes, the peer may attempt a multi-gigabyte allocation (see issue 04).

## Recommended fix

Delete the special case and write the header unconditionally:

```rust
buffer[..MESSAGE_HEADER_SIZE].copy_from_slice(&(bytes_written as MessageSizeHeader).to_be_bytes());
if let Some(size) = M::STATIC_SIZE {
    debug_assert_eq!(size, bytes_written, "M::STATIC_SIZE does not match M::write_to");
}
```

Also guard against `bytes_written == KEEPALIVE_FRAME_SIZE as usize` colliding with the
keepalive marker (a genuinely zero-length message would be framed as a keepalive);
either reject zero-length messages or reserve a distinct marker. Add a round-trip test
that frames a `u64` (static size) through `send_message` → `read_message_opt` and
asserts the value comes back.
