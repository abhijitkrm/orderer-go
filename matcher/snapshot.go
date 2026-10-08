package matcher

import (
	"fmt"
	"strings"
)

// Snapshot serialization per spec/JOURNAL.md — flat `{"rec":...}` lines:
//
//	{"format":"matcher-snap/1","pmin":P,"pmax":M,"max_orders":N,"index":"ladder"}
//	{"rec":"book","symbol":S,"seq":N}
//	{"rec":"order","order_id":I,"side":"bid|ask","otype":"limit","tif":"...","price":P,"qty":Q}

// WriteBook serializes one book (book record + resting orders) into out.
func WriteBook(b *OrderBook, sym uint32, out *strings.Builder) {
	fmt.Fprintf(out, "{\"rec\":\"book\",\"symbol\":%d,\"seq\":%d}\n", sym, b.seq)
	for _, o := range b.RestingOrders() {
		fmt.Fprintf(out,
			"{\"rec\":\"order\",\"order_id\":%d,\"side\":\"%s\",\"otype\":\"limit\",\"tif\":\"%s\",\"price\":%d,\"qty\":%d}\n",
			o.OrderID, o.Side, o.Tif, o.Price, o.Qty)
	}
}

// WriteEngine serializes a full engine snapshot (meta + all books in symbol
// order).
func WriteEngine(e *Engine, out *strings.Builder) {
	c := e.defaultCfg
	idx := "ladder"
	if c.Index == IndexTree {
		idx = "tree"
	}
	fmt.Fprintf(out,
		"{\"format\":\"matcher-snap/1\",\"pmin\":%d,\"pmax\":%d,\"max_orders\":%d,\"index\":\"%s\"}\n",
		c.PriceMin, c.PriceMax, c.MaxOrders, idx)
	for _, sym := range e.Symbols() {
		WriteBook(e.books[sym], sym, out)
	}
}

// SnapBook is the parsed state of one book record.
type SnapBook struct {
	Symbol uint32
	Seq    uint64
	Orders []RestingOrder
}

// Snap is a parsed engine snapshot.
type Snap struct {
	Cfg   BookConfig
	Books []SnapBook
}

// ParseSnapshot parses the flat record format written by WriteEngine.
func ParseSnapshot(text string) (*Snap, error) {
	s := &Snap{Cfg: DefaultConfig()}
	var cur *SnapBook
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if line == "" {
			continue
		}
		if strings.Contains(line, `"rec":"order"`) {
			o := RestingOrder{
				OrderID: getU64(line, "order_id"),
				Price:   getI64(line, "price"),
				Qty:     getU64(line, "qty"),
			}
			if getStr(line, "side") == "ask" {
				o.Side = Ask
			}
			o.Tif = parseTif(getStr(line, "tif"))
			if cur == nil {
				return nil, fmt.Errorf("order record before book record")
			}
			cur.Orders = append(cur.Orders, o)
		} else if strings.Contains(line, `"rec":"book"`) {
			s.Books = append(s.Books, SnapBook{
				Symbol: uint32(getU64(line, "symbol")),
				Seq:    getU64(line, "seq"),
			})
			cur = &s.Books[len(s.Books)-1]
		} else {
			// meta record
			s.Cfg.PriceMin = getI64(line, "pmin")
			s.Cfg.PriceMax = getI64(line, "pmax")
			s.Cfg.MaxOrders = int(getU64(line, "max_orders"))
			if getStr(line, "index") == "tree" {
				s.Cfg.Index = IndexTree
			}
		}
	}
	return s, nil
}

// RestoreEngine rebuilds an engine from a parsed snapshot.
func RestoreEngine(s *Snap) *Engine {
	e := NewEngine(s.Cfg)
	for i := range s.Books {
		b := &s.Books[i]
		e.books[b.Symbol] = Restore(s.Cfg, b.Seq, b.Orders)
	}
	return e
}

func parseTif(s string) Tif {
	switch s {
	case "ioc":
		return Ioc
	case "fok":
		return Fok
	case "post_only":
		return PostOnly
	}
	return Gtc
}

// Minimal flat-JSON getters — the snapshot format is canonical (fixed field
// order, no nesting), so substring scan + digit parse suffices and keeps the
// package dependency-free.

func getStr(line, key string) string {
	k := `"` + key + `":"`
	i := strings.Index(line, k)
	if i < 0 {
		return ""
	}
	rest := line[i+len(k):]
	j := strings.IndexByte(rest, '"')
	return rest[:j]
}

func getU64(line, key string) uint64 {
	k := `"` + key + `":`
	i := strings.Index(line, k)
	if i < 0 {
		return 0
	}
	rest := line[i+len(k):]
	j := 0
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	var v uint64
	for _, c := range rest[:j] {
		v = v*10 + uint64(c-'0')
	}
	return v
}

func getI64(line, key string) int64 {
	k := `"` + key + `":`
	i := strings.Index(line, k)
	if i < 0 {
		return 0
	}
	rest := line[i+len(k):]
	neg := strings.HasPrefix(rest, "-")
	if neg {
		rest = rest[1:]
	}
	j := 0
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	var v int64
	for _, c := range rest[:j] {
		v = v*10 + int64(c-'0')
	}
	if neg {
		return -v
	}
	return v
}
