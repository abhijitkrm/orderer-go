# orderer-go

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

The Go implementation of [orderer](https://github.com/abhijitkrm/orderer):
an LMAX-Disruptor-style, multi-core order-matching engine around the
[matcher](https://github.com/abhijitkrm/matcher) order book. It needs Go
1.21+ and nothing else, and implements `orderer-spec/1.2`. It is a port of
[orderer-rust](https://github.com/abhijitkrm/orderer-rust), and
**byte-identical** to it: listings, per-partition journals (JSONL and
binary), snapshots and exit codes.

```
Handle.Publish ─▶ ingress ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress plugs
 (any goroutine)  (multi-    (iseq,  │              (journal +
                  producer)   route) └─▶ …            apply)
```

## Quick start

```go
events, listing := orderer.Collect(true)
p, err := orderer.NewBuilder(orderer.NewFifoCore).
	Partitions(2).
	Journal(orderer.NewJournalConfig(dir, orderer.Binary)). // durable: fsync every 1024 records
	Egress(events).                                         // or Acks, Metrics, Callback, your own Egress
	Build()
h := p.Handle() // one per goroutine
h.Publish(7, matcher.NewLimit(1, matcher.Ask, 100, 10, matcher.Gtc))
h.Publish(7, matcher.NewLimit(2, matcher.Bid, 100, 4, matcher.Gtc))
p.Drain()                                       // applied and delivered
snap, _ := p.Snapshot()                         // consistent cut: matcher-snap/1 + .meta
snap.Write(filepath.Join(dir, "books.snap"))
p.Shutdown()
```

The runnable version is `examples/quickstart` (`go run ./examples/quickstart`).

## Plug points

| Seam | Type | Built-ins |
|---|---|---|
| Matching core | `MatchingCore` + `CoreFactory` | `NewFifoCore` (matcher-go `OrderBook` per symbol), `NewNoopCore` |
| Egress | `Egress` + `EgressFactory` (one per partition) | `Collect`, `Callback`, `Acks` (durability-gated), `Metrics` |
| Routing | `PartitionMap` | hash (spec/ROUTING.md) + table overrides |
| Journals | `JournalConfig`, `FsyncPolicy` | JSONL or binary, group-commit fsync on I/O goroutines |
| Waiting | `Waits` / `disruptor.WaitStrategy` | BusySpin, Yield, Backoff, Blocking |
| Recovery | `Recover`, `ReadSnapshot`, `Restore`, `RepairDir` | snapshot + journals → cores at any P; torn tails repaired |
| Checkpoints | `Pipeline.Checkpoint` | durable snapshot + journal segment rotation; old segments removed |

## Build, test, harness

```bash
go test ./...                       # ring, golden, pipeline, allocation suites
go test -race ./...
scripts/test.sh                     # all of the above + the vendored spec/conformance.sh
scripts/build-harness.sh            # → harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}
CHECKED=1 scripts/build-harness.sh  # the same, under the race detector
scripts/vendored.sh                 # vendored spec/ + vectors/ untouched
```

Cross-implementation proofs (diffuzz, exhaustive, e2e, snapdiff) run from
the spec repo with this repo checked out next to it.

## Performance

`orderbench` follows spec/BENCH.md. Core mode is matcher-go alone (the
isolated number); pipe mode is the whole engine (the integrated number).
The spec repo's `docs/RESULTS.md` has the rows for every implementation
and the cross-language comparison. Core rows also report `alloc_b_op`:
matcher-go's book passes each event to its sink by pointer through an
interface, so every event escapes to the heap.

See [`docs/DESIGN.md`](docs/DESIGN.md) for the Go-specific design, and
orderer-rust's DESIGN.md for the architecture.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
