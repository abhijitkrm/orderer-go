# orderer-go design

The architecture is orderer-rust's (its `docs/DESIGN.md`); this file covers
what is specific to Go. The port follows orderer-rust's porting checklist
(§8).

| orderer-rust | orderer-go |
|---|---|
| orderer-disruptor | `disruptor/` (generic `RingBuilder[T]`) |
| orderer-core (core.rs, snapshot) | `core.go`, `flat.go` |
| routing.rs | `routing.go` |
| journal.rs + writer.rs | `journal.go` (`ChunkWriter` inside) |
| msg.rs + egress.rs | `egress.go` |
| pipeline.rs | `pipeline.go` |
| recover.rs | `recover.go` |
| harness.rs + bins | `harness.go` + `cmd/*` |

## Memory model

Go's `sync/atomic` operations are sequentially consistent, which is at
least the acquire/release the ring protocol needs and the `SeqCst` the
shutdown handshake needs. Slot contents are plain memory published by the
atomic cursor or availability store, which the race detector understands:
the ring and pipeline suites pass under `go test -race`, and the CHECKED
harness build is a race build.

## Goroutines

The router, each engine and each egress group run on their own goroutine
with `runtime.LockOSThread`, so a busy-spinning stage keeps a thread and the
scheduler does not migrate it. Panics in a stage are recovered and fail the
pipeline (drain, snapshot and shutdown then return an `ErrFailed` error),
as orderer-rust does with thread panics.

## Slots and allocation

Ring slots are values in one preallocated slice, rewritten in place. The
engine is its own `Emitter` and `FifoCore` reuses one tagging sink, so the
hot path adds no closures. What remains is matcher-go's: its book hands each
event to the sink as `*Event` through an interface, so the event escapes.
`TestSteadyStateAllocations` bounds the whole pipeline at under four
allocations per command (it measures about 1.4), and `orderbench --mode
core` reports bytes per command as `alloc_b_op`.

## Journals

`ChunkWriter` encodes records into 64 preallocated 256 KB chunks. Full
chunks, or idle ones after 50 µs, go to the file's I/O goroutine over a
channel. The I/O goroutine writes everything queued, then decides on one
`File.Sync` (group commit; `F_FULLFSYNC` on macOS, like orderer-rust and
orderer-cpp), then publishes the `flushed` and `durable` watermarks.
`Acks` releases events only up to `durable`.
