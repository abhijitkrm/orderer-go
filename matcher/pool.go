package matcher

const NIL = ^uint32(0) // sentinel link ("null index")

type order struct {
	id    uint64
	side  Side
	price int64
	qty   uint64
	tif   Tif
	prev  uint32
	next  uint32
}

// pool is a slab of order slots with an intrusive free-list. Steady-state
// matching performs zero heap allocation.
type pool struct {
	slots    []order
	freeHead uint32
	live     int
	cap      int
}

func newPool(cap int) *pool {
	prealloc := cap
	if prealloc > 1<<20 {
		prealloc = 1 << 20
	}
	return &pool{
		slots:    make([]order, 0, prealloc),
		freeHead: NIL,
		cap:      cap,
	}
}

func (p *pool) alloc() (uint32, bool) {
	if p.freeHead != NIL {
		idx := p.freeHead
		p.freeHead = p.slots[idx].next
		p.live++
		return idx, true
	}
	if len(p.slots) < p.cap {
		idx := uint32(len(p.slots))
		p.slots = append(p.slots, order{prev: NIL, next: NIL})
		p.live++
		return idx, true
	}
	return 0, false
}

func (p *pool) free(idx uint32) {
	p.slots[idx].next = p.freeHead
	p.slots[idx].prev = NIL
	p.freeHead = idx
	p.live--
}
