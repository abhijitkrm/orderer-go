package matcher

// OrderBook is a single-writer deterministic FIFO price-time book.
// Mirrors book.rs in matcher-rust field-for-field.
type OrderBook struct {
	pool *pool
	omap map[uint64]uint32 // order_id -> pool index
	bids priceIndex
	asks priceIndex
	seq  uint64
	cfg  BookConfig
}

func NewOrderBook(cfg BookConfig) *OrderBook {
	b := &OrderBook{
		pool: newPool(cfg.MaxOrders),
		omap: make(map[uint64]uint32, cfg.MaxOrders),
		seq:  0,
		cfg:  cfg,
	}
	if cfg.Index == IndexTree {
		b.bids = newTreeIndex(Bid)
		b.asks = newTreeIndex(Ask)
	} else {
		b.bids = newLadderIndex(Bid, cfg.PriceMin, cfg.PriceMax)
		b.asks = newLadderIndex(Ask, cfg.PriceMin, cfg.PriceMax)
	}
	return b
}

// Apply runs one command, emitting its event stream through sink.
func (b *OrderBook) Apply(cmd Command, sink Sink) {
	switch cmd.Kind {
	case CmdNew:
		b.newOrder(cmd, sink)
	case CmdCancel:
		b.cancel(cmd.OrderID, sink)
	case CmdReplace:
		b.replace(cmd.OrderID, cmd.Price, cmd.Qty, sink)
	}
}

func (b *OrderBook) newOrder(cmd Command, sink Sink) {
	oid := cmd.OrderID
	// SPEC §4.3 validation precedence.
	if cmd.Qty == 0 {
		b.reject(sink, oid, InvalidQty)
		return
	}
	if cmd.OType == Limit && !b.priceOk(cmd.Price) {
		b.reject(sink, oid, InvalidPrice)
		return
	}
	if _, dup := b.omap[oid]; dup {
		b.reject(sink, oid, DuplicateOrderID)
		return
	}
	if b.pool.live >= b.cfg.MaxOrders {
		b.reject(sink, oid, BookFull)
		return
	}
	if cmd.OType == Limit {
		switch cmd.Tif {
		case PostOnly:
			if b.wouldCross(cmd.Side, cmd.Price) {
				b.reject(sink, oid, PostOnlyWouldCross)
				return
			}
		case Fok:
			if b.fillable(cmd.Side, cmd.Price) < cmd.Qty {
				b.reject(sink, oid, FokCannotFill)
				return
			}
		}
	}

	remaining := cmd.Qty
	var bound int64
	hasBound := cmd.OType == Limit
	if hasBound {
		bound = cmd.Price
	}
	b.cross(cmd.Side, hasBound, bound, oid, &remaining, sink)

	switch {
	case remaining == 0:
		b.emit(sink, Event{Kind: EvClosed, OrderID: oid, Reason: uint8(Filled)})
	case cmd.OType == Limit && (cmd.Tif == Gtc || cmd.Tif == PostOnly):
		b.rest(oid, cmd.Side, cmd.Price, remaining, cmd.Tif)
		b.emit(sink, Event{Kind: EvAccepted, OrderID: oid, LeavesQty: remaining})
	default:
		b.emit(sink, Event{Kind: EvClosed, OrderID: oid, Reason: uint8(Expired)})
	}
}

func (b *OrderBook) cancel(oid uint64, sink Sink) {
	idx, ok := b.omap[oid]
	if !ok {
		b.reject(sink, oid, UnknownOrderID)
		return
	}
	o := &b.pool.slots[idx]
	price, side := o.price, o.side
	own := b.ownIndex(side)
	if lv := own.levelMut(price); lv != nil {
		b.pool.levelUnlink(lv, idx)
	}
	own.unlinkLevel(price)
	delete(b.omap, oid)
	b.pool.free(idx)
	b.emit(sink, Event{Kind: EvClosed, OrderID: oid, Reason: uint8(Cancelled)})
}

func (b *OrderBook) replace(oid uint64, price int64, qty uint64, sink Sink) {
	idx, ok := b.omap[oid]
	if !ok {
		b.reject(sink, oid, UnknownOrderID)
		return
	}
	if qty == 0 {
		b.reject(sink, oid, InvalidQty)
		return
	}
	if !b.priceOk(price) {
		b.reject(sink, oid, InvalidPrice)
		return
	}
	o := &b.pool.slots[idx]
	oldPrice, oldQty, side := o.price, o.qty, o.side

	if price == oldPrice && qty <= oldQty {
		own := b.ownIndex(side)
		if lv := own.levelMut(price); lv != nil {
			lv.total -= oldQty - qty
		}
		b.pool.slots[idx].qty = qty
		b.emit(sink, Event{Kind: EvReplaced, OrderID: oid, Price: price, Qty: qty})
		return
	}

	// Priority loss: unlink + re-enter aggressive GTC limit path.
	own := b.ownIndex(side)
	if lv := own.levelMut(oldPrice); lv != nil {
		b.pool.levelUnlink(lv, idx)
	}
	own.unlinkLevel(oldPrice)
	o = &b.pool.slots[idx]
	o.price = price
	o.qty = qty

	remaining := qty
	b.cross(side, true, price, oid, &remaining, sink)

	if remaining == 0 {
		delete(b.omap, oid)
		b.pool.free(idx)
		b.emit(sink, Event{Kind: EvClosed, OrderID: oid, Reason: uint8(Filled)})
		return
	}
	b.pool.slots[idx].qty = remaining
	lv := own.levelInsert(price)
	b.pool.levelPush(lv, idx)
	b.emit(sink, Event{Kind: EvReplaced, OrderID: oid, Price: price, Qty: remaining})
}

// cross walks the opposite side, mutating *qty down to what is left.
// hasBound=false means market (match all available depth).
func (b *OrderBook) cross(side Side, hasBound bool, bound int64, taker uint64, qty *uint64, sink Sink) {
	var opp priceIndex
	if side == Bid {
		opp = b.asks
	} else {
		opp = b.bids
	}
	for {
		bp, ok := opp.bestPrice()
		if !ok {
			return
		}
		if hasBound {
			if side == Bid && bp > bound {
				return
			}
			if side == Ask && bp < bound {
				return
			}
		}
		lv := opp.levelMut(bp)
		if lv == nil {
			return
		}
		for {
			mi := lv.head
			if mi == NIL {
				break
			}
			m := &b.pool.slots[mi]
			mid, mqty := m.id, m.qty
			q := *qty
			if mqty < q {
				q = mqty
			}
			b.seq++
			sink.OnEvent(b.seq, &Event{Kind: EvTrade, Maker: mid, Taker: taker, Price: bp, Qty: q})
			lv.total -= q
			m.qty = mqty - q
			*qty -= q
			if mqty == q {
				b.pool.levelUnlink(lv, mi)
				delete(b.omap, mid)
				b.pool.free(mi)
				b.seq++
				sink.OnEvent(b.seq, &Event{Kind: EvClosed, OrderID: mid, Reason: uint8(Filled)})
			}
			if *qty == 0 {
				break
			}
		}
		if lv.empty() {
			opp.unlinkLevel(bp)
		}
		if *qty == 0 {
			return
		}
	}
}

// rest inserts a remainder as a resting order (slot guaranteed by book_full).
func (b *OrderBook) rest(oid uint64, side Side, price int64, qty uint64, tif Tif) {
	idx, ok := b.pool.alloc()
	if !ok {
		// Unreachable: ingest checked live < MaxOrders and matching only frees.
		return
	}
	b.pool.slots[idx] = order{id: oid, side: side, price: price, qty: qty, tif: tif, prev: NIL, next: NIL}
	own := b.ownIndex(side)
	lv := own.levelInsert(price)
	b.pool.levelPush(lv, idx)
	b.omap[oid] = idx
}

func (b *OrderBook) ownIndex(side Side) priceIndex {
	if side == Bid {
		return b.bids
	}
	return b.asks
}

func (b *OrderBook) priceOk(price int64) bool {
	if b.cfg.Index == IndexTree {
		return price > 0
	}
	return price >= b.cfg.PriceMin && price <= b.cfg.PriceMax
}

func (b *OrderBook) wouldCross(side Side, price int64) bool {
	if side == Bid {
		bp, ok := b.asks.bestPrice()
		return ok && price >= bp
	}
	bp, ok := b.bids.bestPrice()
	return ok && price <= bp
}

func (b *OrderBook) fillable(side Side, price int64) uint64 {
	var lo, hi int64
	if b.cfg.Index == IndexTree {
		lo, hi = -1<<62, 1<<62
	} else {
		lo, hi = b.cfg.PriceMin, b.cfg.PriceMax
	}
	if side == Bid {
		return b.asks.sumRange(lo, price)
	}
	return b.bids.sumRange(price, hi)
}

func (b *OrderBook) emit(sink Sink, ev Event) {
	b.seq++
	sink.OnEvent(b.seq, &ev)
}

func (b *OrderBook) reject(sink Sink, oid uint64, reason RejectReason) {
	b.emit(sink, Event{Kind: EvRejected, OrderID: oid, Reason: uint8(reason)})
}

// ---- query API (SPEC §8) ----

type OrderInfo struct {
	OrderID uint64
	Side    Side
	Price   int64
	Qty     uint64
}

func (b *OrderBook) BestBid() (int64, bool) { return b.bids.bestPrice() }
func (b *OrderBook) BestAsk() (int64, bool) { return b.asks.bestPrice() }
func (b *OrderBook) OrderCount() int        { return b.pool.live }
func (b *OrderBook) Seq() uint64            { return b.seq }
func (b *OrderBook) LevelCount(s Side) int  { return b.ownIndex(s).len() }
func (b *OrderBook) Depth(s Side, n int) []levelDepth {
	return b.ownIndex(s).depth(n)
}

func (b *OrderBook) Order(oid uint64) (OrderInfo, bool) {
	idx, ok := b.omap[oid]
	if !ok {
		return OrderInfo{}, false
	}
	o := &b.pool.slots[idx]
	return OrderInfo{OrderID: o.id, Side: o.side, Price: o.price, Qty: o.qty}, true
}

// RestingOrder is one live order for snapshot serialization.
type RestingOrder struct {
	OrderID uint64
	Side    Side
	Price   int64
	Qty     uint64
	Tif     Tif
}

// RestingOrders returns all live orders in book order: bids best→worst then
// asks best→worst, FIFO within each level.
func (b *OrderBook) RestingOrders() []RestingOrder {
	out := make([]RestingOrder, 0, b.pool.live)
	for _, side := range []Side{Bid, Ask} {
		idx := b.ownIndex(side)
		for _, d := range idx.depth(1 << 30) {
			lv := idx.levelMut(d.Price)
			for i := lv.head; i != NIL; i = b.pool.slots[i].next {
				o := &b.pool.slots[i]
				out = append(out, RestingOrder{
					OrderID: o.id, Side: o.side, Price: o.price, Qty: o.qty, Tif: o.tif,
				})
			}
		}
	}
	return out
}

// Restore rebuilds a book from a snapshot: same config, explicit seq, and
// resting orders replayed in snapshot order (bids then asks, FIFO per level).
func Restore(cfg BookConfig, seq uint64, orders []RestingOrder) *OrderBook {
	b := NewOrderBook(cfg)
	b.seq = seq
	for _, o := range orders {
		idx, ok := b.pool.alloc()
		if !ok {
			return b
		}
		b.pool.slots[idx] = order{
			id: o.OrderID, side: o.Side, price: o.Price, qty: o.Qty,
			tif: o.Tif, prev: NIL, next: NIL,
		}
		b.pool.levelPush(b.ownIndex(o.Side).levelInsert(o.Price), idx)
		b.omap[o.OrderID] = idx
	}
	return b
}
