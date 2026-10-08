package matcher

import "io"

// CmdJournal is an append-only command journal (spec/JOURNAL.md): every
// command is serialized as one canonical line before it is applied.
type CmdJournal struct {
	out     io.Writer
	sym     *uint32 // non-nil → engine-mode tagged lines
	scratch []byte
}

func NewCmdJournal(out io.Writer) *CmdJournal { return &CmdJournal{out: out} }

// NewSymCmdJournal writes `engine:true`-format lines carrying `"symbol"`.
func NewSymCmdJournal(out io.Writer, sym uint32) *CmdJournal {
	return &CmdJournal{out: out, sym: &sym}
}

func (j *CmdJournal) Record(cmd *Command) {
	j.scratch = j.scratch[:0]
	j.scratch = cmd.writeCanonical(j.sym, j.scratch)
	j.scratch = append(j.scratch, '\n')
	j.out.Write(j.scratch)
}

// writeCanonical emits the command line; sym non-nil inserts `"symbol":N`
// after `"cmd"`.
func (c *Command) writeCanonical(sym *uint32, b []byte) []byte {
	switch c.Kind {
	case CmdNew:
		b = append(b, `{"cmd":"new"`...)
		if sym != nil {
			b = append(b, `,"symbol":`...)
			b = appendUint(b, uint64(*sym))
		}
		b = append(b, `,"order_id":`...)
		b = appendUint(b, c.OrderID)
		b = append(b, `,"side":"`...)
		b = append(b, c.Side.String()...)
		b = append(b, `","otype":"`...)
		b = append(b, c.OType.String()...)
		if c.OType == Limit {
			b = append(b, `","price":`...)
			b = appendInt(b, c.Price)
			b = append(b, `,"qty":`...)
		} else {
			b = append(b, `","qty":`...)
		}
		b = appendUint(b, c.Qty)
		b = append(b, `,"tif":"`...)
		b = append(b, c.Tif.String()...)
		b = append(b, `"}`...)
	case CmdCancel:
		b = append(b, `{"cmd":"cancel"`...)
		if sym != nil {
			b = append(b, `,"symbol":`...)
			b = appendUint(b, uint64(*sym))
		}
		b = append(b, `,"order_id":`...)
		b = appendUint(b, c.OrderID)
		b = append(b, '}')
	case CmdReplace:
		b = append(b, `{"cmd":"replace"`...)
		if sym != nil {
			b = append(b, `,"symbol":`...)
			b = appendUint(b, uint64(*sym))
		}
		b = append(b, `,"order_id":`...)
		b = appendUint(b, c.OrderID)
		b = append(b, `,"price":`...)
		b = appendInt(b, c.Price)
		b = append(b, `,"qty":`...)
		b = appendUint(b, c.Qty)
		b = append(b, '}')
	}
	return b
}

// EvtJournal is a sink decorator: every event is journaled then forwarded.
type EvtJournal struct {
	inner   Sink
	out     io.Writer
	scratch []byte
}

func NewEvtJournal(inner Sink, out io.Writer) *EvtJournal {
	return &EvtJournal{inner: inner, out: out}
}

func (j *EvtJournal) OnEvent(seq uint64, ev *Event) {
	j.scratch = j.scratch[:0]
	ev.WriteCanonical(seq, &j.scratch)
	j.scratch = append(j.scratch, '\n')
	j.out.Write(j.scratch)
	j.inner.OnEvent(seq, ev)
}

// JournalEvent journals one symbol-tagged engine event — for use inside
// SubmitTagged closures:
//
//	eng.SubmitTagged(sym, cmd, func(s, q, e) { JournalEvent(s, q, e, w, &scratch); ... })
func JournalEvent(sym uint32, seq uint64, ev *Event, out io.Writer, scratch *[]byte) {
	*scratch = (*scratch)[:0]
	ev.WriteCanonicalSym(seq, sym, scratch)
	*scratch = append(*scratch, '\n')
	out.Write(*scratch)
}
