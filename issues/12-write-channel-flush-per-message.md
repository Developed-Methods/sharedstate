# `WriteChannel` flushes per message — replication hot path pays a syscall per action

**Severity:** Low (performance)
**Files:** `src/transport/channels.rs:97-151` (`WriteChannel::start`), `src/protocol/framing.rs:196` (`flush` at end of `send_message`)

## Summary

`WriteChannel::start` pulls one message from its input channel, calls `send_message`
(which ends with `out.flush().await`), and loops. Under load — a leader streaming its
authority feed to many followers, or a burst of forwarded actions — every action pays a
write+flush round trip, and small frames go out as individual packets (with real TCP
transports, this interacts poorly with per-write syscall costs; the streams here are
raw `TcpStream` halves, so there's no `BufWriter` anywhere in the stack either).

For a system whose stated goal is 20+ datacenters with a busy shared feed, per-message
flush on the fan-out path is the first throughput ceiling you'll hit.

## Recommended fix

Batch opportunistically — flush only when idle:

```rust
loop {
    let msg = tokio::select! { ... };            // existing recv/keepalive select
    write_frame_unflushed(&mut buffer, &msg, &mut self.output).await?;
    // drain whatever else is already queued before flushing
    while let Ok(next) = self.input.try_recv() {
        write_frame_unflushed(&mut buffer, &next, &mut self.output).await?;
    }
    self.output.flush().await?;
}
```

i.e. split `send_message` into an unflushed frame write plus an explicit flush, and
flush once per drained batch rather than once per message. Optionally cap the batch
(e.g. 256 messages or 256 KiB) to bound latency and buffer growth. Wrapping
`I::Write` in `tokio::io::BufWriter` inside `WriteChannel` achieves most of the same
win with less restructuring, as long as the flush moves to the batch boundary.

Keepalive and close paths keep their immediate flush.
