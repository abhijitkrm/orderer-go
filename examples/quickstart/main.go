// The README quick start, runnable: go run ./examples/quickstart
package main

import (
	"fmt"
	"os"
	"path/filepath"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/matcher"
)

func main() {
	dir, _ := os.MkdirTemp("", "orderer-quickstart")
	defer os.RemoveAll(dir)
	events, listing := orderer.Collect(true)
	p, err := orderer.NewBuilder(orderer.NewFifoCore).
		Partitions(2).
		Journal(orderer.NewJournalConfig(dir, orderer.Binary)). // durable: fsync every 1024 records
		Egress(events).                                         // or Acks, Metrics, Callback, your own Egress
		Build()
	if err != nil {
		panic(err)
	}
	h := p.Handle() // one per goroutine
	h.Publish(7, matcher.NewLimit(1, matcher.Ask, 100, 10, matcher.Gtc))
	h.Publish(7, matcher.NewLimit(2, matcher.Bid, 100, 4, matcher.Gtc))
	if err := p.Drain(); err != nil { // applied and delivered
		panic(err)
	}
	snap, _ := p.Snapshot() // consistent cut: matcher-snap/1 + .meta
	snap.Write(filepath.Join(dir, "books.snap"))
	if err := p.Shutdown(); err != nil {
		panic(err)
	}
	fmt.Print(listing.Listing(), snap.Body)
}
