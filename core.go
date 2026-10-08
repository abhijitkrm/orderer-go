package orderer

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// The MatchingCore seam (orderer-rust orderer-core/src/core.rs): what a
// partition's engine goroutine needs from a matching core — plus strict
// snapshot parsing and restore validation.

// Emitter receives every event, in match order.
type Emitter interface {
	Emit(sym uint32, seq uint64, ev *matcher.Event)
}

// EmitFunc adapts a function to Emitter.
type EmitFunc func(sym uint32, seq uint64, ev *matcher.Event)

func (f EmitFunc) Emit(sym uint32, seq uint64, ev *matcher.Event) { f(sym, seq, ev) }

// Block is one snapshot book block (matcher-snap/1 text, no header).
type Block struct {
	Symbol uint32
	Text   string
}

// MatchingCore is one partition's books.
type MatchingCore interface {
	Apply(sym uint32, cmd *matcher.Command, emit Emitter)
	// SnapshotBlocks appends this core's book blocks, any order (the pipeline merges by symbol).
	SnapshotBlocks(out []Block) []Block
	// RestoreBook installs one book from a snapshot block.
	RestoreBook(sym uint32, seq uint64, orders []matcher.RestingOrder) error
}

// CoreFactory builds a partition's core.
type CoreFactory func(cfg matcher.BookConfig) MatchingCore

// ValidateBook reports whether orders can be restored as sym's book under cfg.
func ValidateBook(cfg matcher.BookConfig, sym uint32, orders []matcher.RestingOrder) error {
	pre := fmt.Sprintf("snapshot book %d: ", sym)
	if len(orders) > cfg.MaxOrders {
		return fmt.Errorf("%s%d orders exceed max_orders %d", pre, len(orders), cfg.MaxOrders)
	}
	ids := make([]uint64, len(orders))
	for i, o := range orders {
		ids[i] = o.OrderID
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1] {
			return errors.New(pre + "duplicate order_id")
		}
	}
	var haveBid, haveAsk bool
	var bestBid, bestAsk int64
	for _, o := range orders {
		if o.Qty == 0 {
			return fmt.Errorf("%sorder %d has qty 0", pre, o.OrderID)
		}
		ok := o.Price > 0
		if cfg.Index == matcher.IndexLadder {
			ok = o.Price >= cfg.PriceMin && o.Price <= cfg.PriceMax
		}
		if !ok {
			return fmt.Errorf("%sorder %d price out of range", pre, o.OrderID)
		}
		if o.Side == matcher.Bid {
			if !haveBid || o.Price > bestBid {
				bestBid = o.Price
			}
			haveBid = true
		} else {
			if !haveAsk || o.Price < bestAsk {
				bestAsk = o.Price
			}
			haveAsk = true
		}
	}
	if haveBid && haveAsk && bestBid >= bestAsk {
		return errors.New(pre + "crossed book")
	}
	return nil
}

// FifoCore is the spec-proven core: matcher-go's OrderBook, one per symbol,
// with a dense index for small symbols and a reused tagging sink.
type FifoCore struct {
	cfg    matcher.BookConfig
	dense  []*matcher.OrderBook
	sparse map[uint32]*matcher.OrderBook
	tag    tagSink
}

type tagSink struct {
	sym  uint32
	emit Emitter
}

func (t *tagSink) OnEvent(seq uint64, ev *matcher.Event) { t.emit.Emit(t.sym, seq, ev) }

func NewFifoCore(cfg matcher.BookConfig) MatchingCore {
	return &FifoCore{cfg: cfg, dense: make([]*matcher.OrderBook, denseSyms), sparse: map[uint32]*matcher.OrderBook{}}
}

func (c *FifoCore) book(sym uint32) *matcher.OrderBook {
	if sym < denseSyms {
		b := c.dense[sym]
		if b == nil {
			b = matcher.NewOrderBook(c.cfg)
			c.dense[sym] = b
		}
		return b
	}
	b := c.sparse[sym]
	if b == nil {
		b = matcher.NewOrderBook(c.cfg)
		c.sparse[sym] = b
	}
	return b
}

func (c *FifoCore) Apply(sym uint32, cmd *matcher.Command, emit Emitter) {
	c.tag.sym = sym
	c.tag.emit = emit
	c.book(sym).Apply(*cmd, &c.tag)
}

func block(sym uint32, b *matcher.OrderBook) Block {
	var sb strings.Builder
	matcher.WriteBook(b, sym, &sb)
	return Block{sym, sb.String()}
}

func (c *FifoCore) SnapshotBlocks(out []Block) []Block {
	for s, b := range c.dense {
		if b != nil {
			out = append(out, block(uint32(s), b))
		}
	}
	for s, b := range c.sparse {
		out = append(out, block(s, b))
	}
	return out
}

func (c *FifoCore) RestoreBook(sym uint32, seq uint64, orders []matcher.RestingOrder) error {
	if err := ValidateBook(c.cfg, sym, orders); err != nil {
		return err
	}
	b := matcher.Restore(c.cfg, seq, orders)
	if sym < denseSyms {
		c.dense[sym] = b
	} else {
		c.sparse[sym] = b
	}
	return nil
}

// NoopCore is a test core: it echoes each command as one event with a dense per-symbol seq.
type NoopCore struct{ seqs map[uint32]uint64 }

func NewNoopCore(matcher.BookConfig) MatchingCore { return &NoopCore{seqs: map[uint32]uint64{}} }

func (c *NoopCore) Apply(sym uint32, cmd *matcher.Command, emit Emitter) {
	c.seqs[sym]++
	var ev matcher.Event
	switch cmd.Kind {
	case matcher.CmdNew:
		ev = matcher.Event{Kind: matcher.EvAccepted, OrderID: cmd.OrderID, LeavesQty: cmd.Qty}
	case matcher.CmdCancel:
		ev = matcher.Event{Kind: matcher.EvClosed, OrderID: cmd.OrderID, Reason: uint8(matcher.Cancelled)}
	default:
		ev = matcher.Event{Kind: matcher.EvReplaced, OrderID: cmd.OrderID, Price: cmd.Price, Qty: cmd.Qty}
	}
	emit.Emit(sym, c.seqs[sym], &ev)
}

func (c *NoopCore) SnapshotBlocks(out []Block) []Block {
	for s, q := range c.seqs {
		out = append(out, Block{s, fmt.Sprintf("{\"rec\":\"book\",\"symbol\":%d,\"seq\":%d}\n", s, q)})
	}
	return out
}

func (c *NoopCore) RestoreBook(sym uint32, seq uint64, _ []matcher.RestingOrder) error {
	c.seqs[sym] = seq
	return nil
}

// ---- matcher-snap/1, strictly ---------------------------------------------------------

// SnapBook is one parsed book block.
type SnapBook struct {
	Symbol uint32
	Seq    uint64
	Orders []matcher.RestingOrder
}

// ParsedSnapshot is a parsed matcher-snap/1 body.
type ParsedSnapshot struct {
	Cfg   matcher.BookConfig
	Books []SnapBook
}

// SnapshotHeader is the matcher-snap/1 header line for cfg.
func SnapshotHeader(c matcher.BookConfig) string {
	return fmt.Sprintf("{\"format\":\"matcher-snap/1\",\"pmin\":%d,\"pmax\":%d,\"max_orders\":%d,\"index\":\"%s\"}\n",
		c.PriceMin, c.PriceMax, c.MaxOrders, IndexName(c.Index))
}

// ParseSnapshot is the strict parse (orderer-rust try_parse): malformed input is an error.
func ParseSnapshot(text string) (*ParsedSnapshot, error) {
	lines := strings.Split(text, "\n")
	n := len(lines)
	if n > 0 && lines[n-1] == "" {
		n--
	}
	if n == 0 || text == "" {
		return nil, errors.New("snapshot line 1: empty snapshot")
	}
	if f, _ := Get(lines[0], "format"); f != "matcher-snap/1" {
		return nil, errors.New("snapshot line 1: not a matcher-snap/1 header")
	}
	ps := &ParsedSnapshot{Cfg: ParseHeader(lines[0])}
	for i := 1; i < n; i++ {
		line := lines[i]
		if line == "" {
			continue
		}
		pre := fmt.Sprintf("snapshot line %d: ", i+1)
		switch rec, _ := Get(line, "rec"); rec {
		case "book":
			sym, _ := U64(line, "symbol")
			seq, _ := U64(line, "seq")
			ps.Books = append(ps.Books, SnapBook{Symbol: uint32(sym), Seq: seq})
		case "order":
			id, ok := U64(line, "order_id")
			if !ok {
				return nil, errors.New(pre + "bad order_id")
			}
			side, _ := Get(line, "side")
			if side != "bid" && side != "ask" {
				return nil, errors.New(pre + "bad side")
			}
			ts, _ := Get(line, "tif")
			tif, ok := parseTif(ts)
			if !ok {
				return nil, errors.New(pre + "bad tif")
			}
			price, ok := I64(line, "price")
			if !ok {
				return nil, errors.New(pre + "bad price")
			}
			qty, ok := U64(line, "qty")
			if !ok {
				return nil, errors.New(pre + "bad qty")
			}
			if len(ps.Books) == 0 {
				return nil, errors.New(pre + "order line before book block")
			}
			s := matcher.Bid
			if side == "ask" {
				s = matcher.Ask
			}
			b := &ps.Books[len(ps.Books)-1]
			b.Orders = append(b.Orders, matcher.RestingOrder{OrderID: id, Side: s, Price: price, Qty: qty, Tif: tif})
		default:
			return nil, errors.New(pre + "bad rec")
		}
	}
	return ps, nil
}
