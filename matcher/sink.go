package matcher

// Sink is the engine's only I/O seam: every event flows through it in order
// with its per-book seq. Journaling, market data, replay all hang off this.
type Sink interface {
	OnEvent(seq uint64, ev *Event)
}

// NullSink is a no-op for benchmarks — folds content so calls aren't elided.
type NullSink struct {
	Acc uint64
}

func (s *NullSink) OnEvent(seq uint64, ev *Event) {
	s.Acc += seq ^ ev.Fold()
}

// VecSink records (seq, Event) for tests and replay.
type VecSink struct {
	Events []Event
	Seqs   []uint64
}

func (s *VecSink) OnEvent(seq uint64, ev *Event) {
	s.Seqs = append(s.Seqs, seq)
	s.Events = append(s.Events, *ev)
}

// LinesSink serializes each event to its canonical JSON line (golden harness).
type LinesSink struct {
	Lines [][]byte
	buf   []byte
}

func (s *LinesSink) OnEvent(seq uint64, ev *Event) {
	s.buf = s.buf[:0]
	ev.WriteCanonical(seq, &s.buf)
	line := make([]byte, len(s.buf))
	copy(line, s.buf)
	s.Lines = append(s.Lines, line)
}
