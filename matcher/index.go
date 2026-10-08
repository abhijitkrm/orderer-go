package matcher

import (
	"math/bits"
	"sort"
)

// priceIndex abstracts the price -> level index (SPEC §2). Two impls:
//   - ladderIndex: direct-indexed + bitmap + top-of-book cursor (hot path)
//   - treeIndex: sorted-slice fallback for unbounded/sparse domains
//
// Semantics identical; choice is performance/memory only.
type priceIndex interface {
	bestPrice() (int64, bool)
	levelMut(price int64) *level    // existing non-empty level or nil
	levelInsert(price int64) *level // get-or-create
	unlinkLevel(price int64)        // drop bookkeeping if emptied
	sumRange(lo, hi int64) uint64   // FOK fillability
	len() int
	depth(n int) []levelDepth
}

type levelDepth struct {
	Price int64
	Qty   uint64
}

// ---- ladder --------------------------------------------------------------

type ladderIndex struct {
	base   int64
	side   Side
	levels []level
	bits   []uint64
	count  int
	best   uint32 // index of best occupied level; NIL when empty
}

func newLadderIndex(side Side, pmin, pmax int64) *ladderIndex {
	span := int(pmax-pmin) + 1
	if span < 1 {
		span = 1
	}
	li := &ladderIndex{
		base:   pmin,
		side:   side,
		levels: make([]level, span),
		bits:   make([]uint64, (span+63)/64),
		best:   NIL,
	}
	for i := range li.levels {
		li.levels[i] = newLevel() // head/tail must start at NIL, not 0
	}
	return li
}

func (l *ladderIndex) idx(price int64) int { return int(price - l.base) }

func (l *ladderIndex) bestPrice() (int64, bool) {
	if l.best == NIL {
		return 0, false
	}
	return l.base + int64(l.best), true
}

func (l *ladderIndex) levelMut(price int64) *level {
	i := l.idx(price)
	if i < 0 || i >= len(l.levels) {
		return nil
	}
	lv := &l.levels[i]
	if lv.empty() {
		return nil
	}
	return lv
}

func (l *ladderIndex) levelInsert(price int64) *level {
	i := l.idx(price)
	lv := &l.levels[i]
	if lv.empty() {
		l.bits[i/64] |= 1 << (uint(i) % 64)
		l.count++
		if l.best == NIL ||
			(l.side == Bid && uint32(i) > l.best) ||
			(l.side == Ask && uint32(i) < l.best) {
			l.best = uint32(i)
		}
	}
	return lv
}

func (l *ladderIndex) unlinkLevel(price int64) {
	i := l.idx(price)
	lv := &l.levels[i]
	if lv.head != NIL {
		return
	}
	l.bits[i/64] &^= 1 << (uint(i) % 64)
	l.count--
	if uint32(i) == l.best {
		l.best = l.rescan(i)
	}
}

// rescan finds the next occupied index moving inward from `from` (inclusive):
// higher for asks, lower for bids.
func (l *ladderIndex) rescan(from int) uint32 {
	if l.side == Ask {
		for i := from; i < len(l.levels); {
			w := i / 64
			word := l.bits[w] & (^uint64(0) << (uint(i) % 64))
			for word != 0 {
				return uint32(w*64 + bits.TrailingZeros64(word))
			}
			i = w*64 + 64
		}
		return NIL
	}
	for i := from; i >= 0; {
		w := i / 64
		word := l.bits[w] & (^uint64(0) >> (63 - uint(i)%64))
		for word != 0 {
			return uint32(w*64 + 63 - bits.LeadingZeros64(word))
		}
		i = w*64 - 1
	}
	return NIL
}

func (l *ladderIndex) sumRange(lo, hi int64) uint64 {
	loI := l.idx(lo)
	if loI < 0 {
		loI = 0
	}
	hiI := l.idx(hi)
	if hiI > len(l.levels)-1 {
		hiI = len(l.levels) - 1
	}
	if loI > hiI {
		return 0
	}
	var sum uint64
	for i := loI; i <= hiI; {
		w := i / 64
		hiBit := w*64 + 63
		if hiBit > hiI {
			hiBit = hiI
		}
		word := l.bits[w] & (^uint64(0) << (uint(i) % 64)) &
			(^uint64(0) >> (63 - uint(hiBit)%64))
		for word != 0 {
			b := bits.TrailingZeros64(word)
			sum += l.levels[w*64+b].total
			word &= word - 1
		}
		i = w*64 + 64
	}
	return sum
}

func (l *ladderIndex) len() int { return l.count }

func (l *ladderIndex) depth(n int) []levelDepth {
	out := make([]levelDepth, 0, min(n, l.count)) // n may be "all" (1<<30)
	if l.side == Ask {
		for w := 0; w < len(l.bits) && len(out) < n; w++ {
			word := l.bits[w]
			for word != 0 && len(out) < n {
				b := bits.TrailingZeros64(word)
				i := w*64 + b
				out = append(out, levelDepth{l.base + int64(i), l.levels[i].total})
				word &= word - 1
			}
		}
		return out
	}
	for w := len(l.bits) - 1; w >= 0 && len(out) < n; w-- {
		word := l.bits[w]
		for word != 0 && len(out) < n {
			b := 63 - bits.LeadingZeros64(word)
			i := w*64 + b
			out = append(out, levelDepth{l.base + int64(i), l.levels[i].total})
			word &^= 1 << uint(b)
		}
	}
	return out
}

// ---- tree (sorted-slice fallback) -----------------------------------------

// treeIndex keeps parallel sorted keys + levels. O(log n) best price via
// binary search; O(n) insert/remove — acceptable for the unbounded fallback
// path (spec: identical semantics, performance choice).
type treeIndex struct {
	side Side
	keys []int64
	lvls []level
}

func newTreeIndex(side Side) *treeIndex { return &treeIndex{side: side} }

func (t *treeIndex) pos(price int64) (int, bool) {
	i := sort.Search(len(t.keys), func(i int) bool { return t.keys[i] >= price })
	if i < len(t.keys) && t.keys[i] == price {
		return i, true
	}
	return i, false
}

func (t *treeIndex) bestPrice() (int64, bool) {
	if len(t.keys) == 0 {
		return 0, false
	}
	if t.side == Bid {
		return t.keys[len(t.keys)-1], true
	}
	return t.keys[0], true
}

func (t *treeIndex) levelMut(price int64) *level {
	if i, ok := t.pos(price); ok && !t.lvls[i].empty() {
		return &t.lvls[i]
	}
	return nil
}

func (t *treeIndex) levelInsert(price int64) *level {
	i, ok := t.pos(price)
	if !ok {
		t.keys = append(t.keys, 0)
		t.lvls = append(t.lvls, level{})
		copy(t.keys[i+1:], t.keys[i:])
		copy(t.lvls[i+1:], t.lvls[i:])
		t.keys[i] = price
		t.lvls[i] = newLevel()
	}
	return &t.lvls[i]
}

func (t *treeIndex) unlinkLevel(price int64) {
	i, ok := t.pos(price)
	if !ok || !t.lvls[i].empty() {
		return
	}
	copy(t.keys[i:], t.keys[i+1:])
	copy(t.lvls[i:], t.lvls[i+1:])
	t.keys = t.keys[:len(t.keys)-1]
	t.lvls = t.lvls[:len(t.lvls)-1]
}

func (t *treeIndex) sumRange(lo, hi int64) uint64 {
	loI := sort.Search(len(t.keys), func(i int) bool { return t.keys[i] >= lo })
	var sum uint64
	for i := loI; i < len(t.keys) && t.keys[i] <= hi; i++ {
		sum += t.lvls[i].total
	}
	return sum
}

func (t *treeIndex) len() int { return len(t.keys) }

func (t *treeIndex) depth(n int) []levelDepth {
	out := make([]levelDepth, 0, min(n, len(t.keys)))
	if t.side == Ask {
		for i := 0; i < len(t.keys) && len(out) < n; i++ {
			out = append(out, levelDepth{t.keys[i], t.lvls[i].total})
		}
	} else {
		for i := len(t.keys) - 1; i >= 0 && len(out) < n; i-- {
			out = append(out, levelDepth{t.keys[i], t.lvls[i].total})
		}
	}
	return out
}
