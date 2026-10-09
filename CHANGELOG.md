# Changelog

## Unreleased: orderer-spec/1.2

- Vendors matcher-go 46852c8: emitting events no longer allocates (77 bytes
  per command to zero; core W6 ~7.5M to ~10-13M ops/s untimed on an M1).
- Binary journals are version 2 (CRC-32C per record via `hash/crc32`);
  version 1 still reads.
- `RepairDir` / `orderrecover --repair` truncate a torn final record.
- `Pipeline.Checkpoint` rotates journals onto segments at a clean cut,
  writes the snapshot durably and removes covered segments. Egress plugs
  may implement `Checkpointer`.
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
