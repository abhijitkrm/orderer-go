// Package matcher is a deterministic, zero-allocation FIFO limit order book
// and matching engine core — the Go port of the matcher spec (../spec/SPEC.md).
//
// Commands in; a per-book monotonically sequenced event stream out through the
// Sink seam. Semantics are identical to matcher-rust; the golden corpus in
// ../vectors proves byte-identical canonical output.
package matcher

type Side uint8

const (
	Bid Side = iota
	Ask
)

func (s Side) Opposite() Side { return 1 - s }
func (s Side) String() string {
	if s == Bid {
		return "bid"
	}
	return "ask"
}

type OType uint8

const (
	Limit OType = iota
	Market
)

func (t OType) String() string {
	if t == Limit {
		return "limit"
	}
	return "market"
}

type Tif uint8

const (
	Gtc Tif = iota
	Ioc
	Fok
	PostOnly
)

func (t Tif) String() string {
	switch t {
	case Gtc:
		return "gtc"
	case Ioc:
		return "ioc"
	case Fok:
		return "fok"
	default:
		return "post_only"
	}
}

// Command is a flattened command (Go has no tagged unions): Kind selects
// which fields are meaningful — mirrors Command in the spec.
type CmdKind uint8

const (
	CmdNew CmdKind = iota
	CmdCancel
	CmdReplace
)

type Command struct {
	Kind    CmdKind
	OrderID uint64
	Side    Side
	OType   OType
	Price   int64
	Qty     uint64
	Tif     Tif
}

func NewLimit(id uint64, side Side, price int64, qty uint64, tif Tif) Command {
	return Command{Kind: CmdNew, OrderID: id, Side: side, OType: Limit, Price: price, Qty: qty, Tif: tif}
}

func NewMarket(id uint64, side Side, qty uint64) Command {
	return Command{Kind: CmdNew, OrderID: id, Side: side, OType: Market, Qty: qty, Tif: Ioc}
}

func Cancel(id uint64) Command { return Command{Kind: CmdCancel, OrderID: id} }

func Replace(id uint64, price int64, qty uint64) Command {
	return Command{Kind: CmdReplace, OrderID: id, Price: price, Qty: qty}
}

type RejectReason uint8

const (
	InvalidQty RejectReason = iota
	InvalidPrice
	DuplicateOrderID
	UnknownOrderID
	PostOnlyWouldCross
	FokCannotFill
	BookFull
)

func (r RejectReason) String() string {
	switch r {
	case InvalidQty:
		return "invalid_qty"
	case InvalidPrice:
		return "invalid_price"
	case DuplicateOrderID:
		return "duplicate_order_id"
	case UnknownOrderID:
		return "unknown_order_id"
	case PostOnlyWouldCross:
		return "post_only_would_cross"
	case FokCannotFill:
		return "fok_cannot_fill"
	default:
		return "book_full"
	}
}

type CloseReason uint8

const (
	Filled CloseReason = iota
	Cancelled
	Expired
)

func (r CloseReason) String() string {
	switch r {
	case Filled:
		return "filled"
	case Cancelled:
		return "cancelled"
	default:
		return "expired"
	}
}

// Event is the flattened event record delivered to Sink. Kind selects fields:
//
//	Accepted: OrderID, LeavesQty
//	Rejected: OrderID, Reason (RejectReason)
//	Trade:    Maker, Taker, Price, Qty
//	Closed:   OrderID, Reason (CloseReason)
//	Replaced: OrderID, Price, Qty
type EventKind uint8

const (
	EvAccepted EventKind = iota
	EvRejected
	EvTrade
	EvClosed
	EvReplaced
)

type Event struct {
	Kind      EventKind
	OrderID   uint64
	Maker     uint64
	Taker     uint64
	Price     int64
	Qty       uint64
	LeavesQty uint64
	Reason    uint8
}

// WriteCanonical appends the event's canonical JSON line (SCHEMA.md), no newline.
func (e *Event) WriteCanonical(seq uint64, buf *[]byte) { e.write(seq, nil, buf) }

// WriteCanonicalSym emits the `engine:true` line: `"symbol":N` after `ev`.
func (e *Event) WriteCanonicalSym(seq uint64, sym uint32, buf *[]byte) {
	sf := appendUint([]byte(`,"symbol":`), uint64(sym))
	e.write(seq, sf, buf)
}

func (e *Event) write(seq uint64, symField []byte, buf *[]byte) {
	*buf = append(*buf, `{"seq":`...)
	*buf = appendUint(*buf, seq)
	switch e.Kind {
	case EvAccepted:
		*buf = append(*buf, `,"ev":"accepted"`...)
		*buf = append(*buf, symField...)
		*buf = append(*buf, `,"order_id":`...)
		*buf = appendUint(*buf, e.OrderID)
		*buf = append(*buf, `,"leaves_qty":`...)
		*buf = appendUint(*buf, e.LeavesQty)
	case EvRejected:
		*buf = append(*buf, `,"ev":"rejected"`...)
		*buf = append(*buf, symField...)
		*buf = append(*buf, `,"order_id":`...)
		*buf = appendUint(*buf, e.OrderID)
		*buf = append(*buf, `,"reason":"`...)
		*buf = append(*buf, RejectReason(e.Reason).String()...)
		*buf = append(*buf, '"')
	case EvTrade:
		*buf = append(*buf, `,"ev":"trade"`...)
		*buf = append(*buf, symField...)
		*buf = append(*buf, `,"maker":`...)
		*buf = appendUint(*buf, e.Maker)
		*buf = append(*buf, `,"taker":`...)
		*buf = appendUint(*buf, e.Taker)
		*buf = append(*buf, `,"price":`...)
		*buf = appendInt(*buf, e.Price)
		*buf = append(*buf, `,"qty":`...)
		*buf = appendUint(*buf, e.Qty)
	case EvClosed:
		*buf = append(*buf, `,"ev":"closed"`...)
		*buf = append(*buf, symField...)
		*buf = append(*buf, `,"order_id":`...)
		*buf = appendUint(*buf, e.OrderID)
		*buf = append(*buf, `,"reason":"`...)
		*buf = append(*buf, CloseReason(e.Reason).String()...)
		*buf = append(*buf, '"')
	case EvReplaced:
		*buf = append(*buf, `,"ev":"replaced"`...)
		*buf = append(*buf, symField...)
		*buf = append(*buf, `,"order_id":`...)
		*buf = appendUint(*buf, e.OrderID)
		*buf = append(*buf, `,"price":`...)
		*buf = appendInt(*buf, e.Price)
		*buf = append(*buf, `,"qty":`...)
		*buf = appendUint(*buf, e.Qty)
	}
	*buf = append(*buf, '}')
}

// Fold is a cheap content hash so sinks can observe every field (benchmarks).
func (e *Event) Fold() uint64 {
	switch e.Kind {
	case EvTrade:
		return e.Maker*0x9E3779B3 + e.Taker*0x85EBCA6B + uint64(e.Price)*0xC2B2AE35 + e.Qty
	default:
		return e.OrderID*0x9E3779B1 + uint64(e.Kind) + e.Qty + e.LeavesQty + uint64(e.Reason)
	}
}

func appendUint(b []byte, v uint64) []byte {
	var tmp [20]byte
	i := len(tmp)
	for {
		i--
		tmp[i] = byte('0' + v%10)
		if v < 10 {
			break
		}
		v /= 10
	}
	return append(b, tmp[i:]...)
}

func appendInt(b []byte, v int64) []byte {
	if v < 0 {
		b = append(b, '-')
		v = -v
	}
	return appendUint(b, uint64(v))
}

type IndexKind uint8

const (
	IndexLadder IndexKind = iota
	IndexTree
)

// BookConfig constructs an OrderBook. Tree mode treats the price domain as
// unbounded: invalid_price then applies to price <= 0 (SPEC §2).
type BookConfig struct {
	PriceMin, PriceMax int64
	MaxOrders          int
	Index              IndexKind
}

func DefaultConfig() BookConfig {
	return BookConfig{PriceMin: 0, PriceMax: 1_000_000, MaxOrders: 65_536, Index: IndexLadder}
}
