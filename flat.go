// Package orderer is the Go implementation of orderer: an LMAX-Disruptor-style,
// multi-core order-matching pipeline around matcher-go's order book
// (orderer-spec/1.1, byte-identical to orderer-rust).
package orderer

import (
	"strconv"
	"strings"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// Strict flat-JSON field access and canonical command lines.
//
// matcher-go's parsing is lenient; orderer's harnesses must reject malformed
// input exactly as orderer-rust does (spec/HARNESS.md §5), so these mirror
// matcher-rust's jsonflat: a missing or unparsable field is an error.

// Get returns the value of key: the quoted string's contents, or the trimmed token.
func Get(line, key string) (string, bool) {
	pat := `"` + key + `":`
	p := strings.Index(line, pat)
	if p < 0 {
		return "", false
	}
	rest := line[p+len(pat):]
	if strings.HasPrefix(rest, `"`) {
		e := strings.IndexByte(rest[1:], '"')
		if e < 0 {
			return "", false
		}
		return rest[1 : 1+e], true
	}
	e := strings.IndexAny(rest, ",}")
	if e < 0 {
		e = len(rest)
	}
	return strings.TrimSpace(rest[:e]), true
}

// ParseU64 parses an unsigned decimal with an optional '+'.
func ParseU64(s string) (uint64, bool) {
	s = strings.TrimPrefix(s, "+")
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(s, 10, 64)
	return v, err == nil
}

// ParseI64 parses a signed decimal.
func ParseI64(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	i := 0
	if s[0] == '-' || s[0] == '+' {
		i = 1
	}
	if i == len(s) {
		return 0, false
	}
	for j := i; j < len(s); j++ {
		if s[j] < '0' || s[j] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(strings.TrimPrefix(s, "+"), 10, 64)
	return v, err == nil
}

func U64(line, key string) (uint64, bool) {
	s, ok := Get(line, key)
	if !ok {
		return 0, false
	}
	return ParseU64(s)
}

func I64(line, key string) (int64, bool) {
	s, ok := Get(line, key)
	if !ok {
		return 0, false
	}
	return ParseI64(s)
}

// ParseCommand parses one canonical command line; false if any field is missing or invalid.
func ParseCommand(line string) (matcher.Command, bool) {
	var c matcher.Command
	cmd, ok := Get(line, "cmd")
	if !ok {
		return c, false
	}
	switch cmd {
	case "new":
		side, ok1 := Get(line, "side")
		otype, ok2 := Get(line, "otype")
		tif, ok3 := Get(line, "tif")
		id, ok4 := U64(line, "order_id")
		price, ok5 := I64(line, "price")
		qty, ok6 := U64(line, "qty")
		if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) {
			return c, false
		}
		c = matcher.Command{Kind: matcher.CmdNew, OrderID: id, Price: price, Qty: qty}
		switch side {
		case "bid":
			c.Side = matcher.Bid
		case "ask":
			c.Side = matcher.Ask
		default:
			return c, false
		}
		switch otype {
		case "limit":
			c.OType = matcher.Limit
		case "market":
			c.OType = matcher.Market
		default:
			return c, false
		}
		t, ok := parseTif(tif)
		if !ok {
			return c, false
		}
		c.Tif = t
		return c, true
	case "cancel":
		id, ok := U64(line, "order_id")
		return matcher.Cancel(id), ok
	case "replace":
		id, ok1 := U64(line, "order_id")
		price, ok2 := I64(line, "price")
		qty, ok3 := U64(line, "qty")
		return matcher.Replace(id, price, qty), ok1 && ok2 && ok3
	}
	return c, false
}

func parseTif(s string) (matcher.Tif, bool) {
	switch s {
	case "gtc":
		return matcher.Gtc, true
	case "ioc":
		return matcher.Ioc, true
	case "fok":
		return matcher.Fok, true
	case "post_only":
		return matcher.PostOnly, true
	}
	return 0, false
}

// ParseHeader reads a corpus/vector header into a book config (matcher defaults).
func ParseHeader(line string) matcher.BookConfig {
	c := matcher.DefaultConfig()
	if v, ok := I64(line, "pmin"); ok {
		c.PriceMin = v
	}
	if v, ok := I64(line, "pmax"); ok {
		c.PriceMax = v
	}
	if v, ok := U64(line, "max_orders"); ok {
		c.MaxOrders = int(v)
	}
	if s, _ := Get(line, "index"); s == "tree" {
		c.Index = matcher.IndexTree
	}
	return c
}

func IndexName(k matcher.IndexKind) string {
	if k == matcher.IndexTree {
		return "tree"
	}
	return "ladder"
}

func SameBook(a, b matcher.BookConfig) bool { return a == b }

// WriteCommand appends matcher's canonical command line; the engine form
// (with "symbol") when withSym.
func WriteCommand(c *matcher.Command, sym uint32, withSym bool, b []byte) []byte {
	b = append(b, `{"cmd":"`...)
	switch c.Kind {
	case matcher.CmdNew:
		b = append(b, `new"`...)
	case matcher.CmdCancel:
		b = append(b, `cancel"`...)
	default:
		b = append(b, `replace"`...)
	}
	if withSym {
		b = append(b, `,"symbol":`...)
		b = strconv.AppendUint(b, uint64(sym), 10)
	}
	b = append(b, `,"order_id":`...)
	b = strconv.AppendUint(b, c.OrderID, 10)
	switch c.Kind {
	case matcher.CmdNew:
		b = append(b, `,"side":"`...)
		b = append(b, c.Side.String()...)
		b = append(b, `","otype":"`...)
		b = append(b, c.OType.String()...)
		b = append(b, `","price":`...)
		b = strconv.AppendInt(b, c.Price, 10)
		b = append(b, `,"qty":`...)
		b = strconv.AppendUint(b, c.Qty, 10)
		b = append(b, `,"tif":"`...)
		b = append(b, c.Tif.String()...)
		b = append(b, `"}`...)
	case matcher.CmdCancel:
		b = append(b, '}')
	default:
		b = append(b, `,"price":`...)
		b = strconv.AppendInt(b, c.Price, 10)
		b = append(b, `,"qty":`...)
		b = strconv.AppendUint(b, c.Qty, 10)
		b = append(b, '}')
	}
	return b
}
