# Changelog

## 0.2.1 (orderer-spec/1.3)

- Vendors matcher-go 81542be: the price ladder finds the next best price through a
  summary bitmap, and an emptied side resets at once. A book whose side
  emptied used to scan the whole ladder per cancel (W3 at 1M ops: 4-24x
  faster in the matcher bench). Output unchanged.
- Repair (orderer-spec 1.3), for three kinds of damage SIGKILL left that
  `--repair` refused: binary tails of all-zero records (an interrupted
  write on macOS can extend a file with zeros, a whole write buffer of
  them) are cut; a segment created at a checkpoint whose header never
  arrived is deleted; and when the last segment holds no records, the
  torn segment before it is repaired too.
- BusySpin yields the P every 16K idle spins. At P=4 the spinning
  goroutines starved the journal goroutines returning from fsync: P=4
  durable on W6 went from ~4.5M to ~11M commands/s.
- Pipeline goroutines are no longer locked to OS threads (nor are the
  bench's producers). Locking made throughput swing by up to 2x between
  runs; unlocked, P=2 journal-off is ~12M (was 6-9M) and P=4 is steady.
- `orderbench --stats`: fsync count, mean and max, as in orderer-rust.

## 0.2.0 (orderer-spec/1.2)

- Vendors matcher-go 46852c8: emitting events no longer allocates (77 bytes
  per command to zero; core W6 ~7.5M to ~10-13M ops/s untimed on an M1).
- Binary journals are version 2 (CRC-32C per record via `hash/crc32`);
  version 1 still reads.
- `RepairDir` / `orderrecover --repair` truncate a torn final record.
- `Pipeline.Checkpoint` rotates journals onto segments at a clean cut,
  writes the snapshot durably and removes covered segments. Egress plugs
  may implement `Checkpointer`.
- `Pipeline.Stats()` (stats.go): ring depths, per-partition counts,
  watermarks, fsync timings; `PipelineStats.ToPrometheus()`.
- `Builder.CheckpointEvery(d)`: automatic checkpoints from a background
  goroutine (stopped first at shutdown).
- `orderrun --checkpoint-every K` and `--durable`; `scripts/test.sh` runs the
  vendored `spec/conformance.sh`.

## 0.1.0

- First release: the full orderer pipeline in Go, byte-identical to
  orderer-rust 0.1 (`orderer-spec/1.1`).
- Vendors matcher-go `53b222a` (includes the `depth` allocation fix found
  here) and the orderer spec `41019c6`.
- Harness tools (spec/HARNESS.md) and `orderbench` (spec/BENCH.md 1.1).
