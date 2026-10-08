package matcher

// level is a price level: intrusive FIFO queue of pool orders plus a running
// total (FOK pre-checks, depth queries).
type level struct {
	head  uint32
	tail  uint32
	total uint64
}

func newLevel() level { return level{head: NIL, tail: NIL} }

func (l *level) empty() bool { return l.head == NIL }

// levelPush appends idx at the tail; order qty must already be set.
func (p *pool) levelPush(l *level, idx uint32) {
	qty := p.slots[idx].qty
	p.slots[idx].prev = l.tail
	p.slots[idx].next = NIL
	if l.tail != NIL {
		p.slots[l.tail].next = idx
	} else {
		l.head = idx
	}
	l.tail = idx
	l.total += qty
}

// levelUnlink removes idx from anywhere in l and adjusts the total.
func (p *pool) levelUnlink(l *level, idx uint32) {
	o := &p.slots[idx]
	prev, next, qty := o.prev, o.next, o.qty
	if prev != NIL {
		p.slots[prev].next = next
	} else {
		l.head = next
	}
	if next != NIL {
		p.slots[next].prev = prev
	} else {
		l.tail = prev
	}
	o.prev, o.next = NIL, NIL
	l.total -= qty
}
