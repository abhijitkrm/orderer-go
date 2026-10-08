package matcher

import (
	"sort"
)

// Engine is a thin multi-symbol router: one book per symbol, each its own
// single-writer domain (the exchange partitioning model).
type Engine struct {
	defaultCfg BookConfig
	books      map[uint32]*OrderBook
}

func NewEngine(defaultCfg BookConfig) *Engine {
	return &Engine{defaultCfg: defaultCfg, books: make(map[uint32]*OrderBook)}
}

// AddSymbol registers a symbol with its own config (else first Submit uses
// the engine default).
func (e *Engine) AddSymbol(sym uint32, cfg BookConfig) {
	e.books[sym] = NewOrderBook(cfg)
}

func (e *Engine) Book(sym uint32) *OrderBook { return e.books[sym] }

// Symbols returns the live symbols in ascending order (deterministic for
// snapshot serialization).
func (e *Engine) Symbols() []uint32 {
	syms := make([]uint32, 0, len(e.books))
	for s := range e.books {
		syms = append(syms, s)
	}
	sort.Slice(syms, func(i, j int) bool { return syms[i] < syms[j] })
	return syms
}

// Submit routes a command to sym's book; events flow to sink.
func (e *Engine) Submit(sym uint32, cmd Command, sink Sink) {
	b := e.books[sym]
	if b == nil {
		b = NewOrderBook(e.defaultCfg)
		e.books[sym] = b
	}
	b.Apply(cmd, sink)
}

// SubmitTagged delivers events as f(symbol, seq, event).
func (e *Engine) SubmitTagged(sym uint32, cmd Command, f func(uint32, uint64, *Event)) {
	b := e.books[sym]
	if b == nil {
		b = NewOrderBook(e.defaultCfg)
		e.books[sym] = b
	}
	b.Apply(cmd, &tagSink{sym: sym, f: f})
}

type tagSink struct {
	sym uint32
	f   func(uint32, uint64, *Event)
}

func (t *tagSink) OnEvent(seq uint64, ev *Event) { t.f(t.sym, seq, ev) }
