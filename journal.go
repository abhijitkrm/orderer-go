package orderer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/abhijitkrm/orderer-go/matcher"
)

// Per-partition journals (spec/JOURNAL.md): naming, JSONL and binary
// encodings, strict readers, and the asynchronous chunk writer.

// JournalFormat is JSONL (canonical lines) or Binary (fixed records).
type JournalFormat uint8

const (
	Jsonl JournalFormat = iota
	Binary
)

// JournalKind is a command or event journal.
type JournalKind uint8

const (
	KindCmd JournalKind = 1
	KindEvt JournalKind = 2
)

func (k JournalKind) label() string {
	if k == KindCmd {
		return "cmd"
	}
	return "evt"
}

func (k JournalKind) recordSize() int {
	if k == KindCmd {
		return CmdRecord
	}
	return EvtRecord
}

const (
	Header    = 64
	CmdRecord = 40
	EvtRecord = 48
	maxRecord = 256
)

// FsyncMode selects when the I/O goroutine fsyncs. Never observable (spec/PIPELINE.md §8).
type FsyncMode uint8

const (
	FsyncNever FsyncMode = iota
	FsyncEveryN
	FsyncEvery
)

// FsyncPolicy is a FsyncMode plus its parameters.
type FsyncPolicy struct {
	Mode     FsyncMode
	N        uint64
	Idle     time.Duration // EveryN: sync after this long with nothing more queued
	Interval time.Duration // Every
}

func FsyncNeverPolicy() FsyncPolicy { return FsyncPolicy{Mode: FsyncNever} }

// FsyncEveryNPolicy group-commits once n records are pending, or after 200 µs idle.
func FsyncEveryNPolicy(n uint64) FsyncPolicy {
	return FsyncPolicy{Mode: FsyncEveryN, N: n, Idle: 200 * time.Microsecond}
}

func FsyncEveryPolicy(d time.Duration) FsyncPolicy {
	return FsyncPolicy{Mode: FsyncEvery, Idle: d, Interval: d}
}

// JournalConfig places and paces a pipeline's journals.
type JournalConfig struct {
	Dir    string
	Format JournalFormat
	Fsync  FsyncPolicy
	Events bool // event journals too (derived data)
	Append bool // reopen existing journals (after recovery)
}

// NewJournalConfig is binary-or-JSONL, durable (fsync every 1024 records), events on.
func NewJournalConfig(dir string, f JournalFormat) JournalConfig {
	return JournalConfig{Dir: dir, Format: f, Fsync: FsyncEveryNPolicy(1024), Events: true}
}

// CorruptJournal is a malformed, torn or inconsistent journal.
type CorruptJournal struct{ Msg string }

func (e *CorruptJournal) Error() string { return e.Msg }

func corrupt(p, d string) error { return &CorruptJournal{p + ": " + d} }

// JournalPath names partition p's journal of kind k.
func JournalPath(dir string, k JournalKind, p uint32, f JournalFormat) string {
	ext := ".journal"
	if f == Binary {
		ext = ".bin"
	}
	return filepath.Join(dir, fmt.Sprintf("%s-%d%s", k.label(), p, ext))
}

// ---- encodings -----------------------------------------------------------------------------

func jsonlHeader(k JournalKind, p, P uint32, b matcher.BookConfig) string {
	return fmt.Sprintf("{\"format\":\"orderer-journal/1\",\"kind\":\"%s\",\"partition\":%d,\"partitions\":%d,\"pmin\":%d,\"pmax\":%d,\"max_orders\":%d,\"index\":\"%s\"}\n",
		k.label(), p, P, b.PriceMin, b.PriceMax, b.MaxOrders, IndexName(b.Index))
}

func binaryHeader(k JournalKind, p, P uint32, b matcher.BookConfig) []byte {
	h := make([]byte, Header)
	copy(h, "ORDJ")
	le := binary.LittleEndian
	le.PutUint16(h[4:], 1)
	h[6] = byte(k)
	if b.Index == matcher.IndexTree {
		h[7] = 1
	}
	le.PutUint32(h[8:], p)
	le.PutUint32(h[12:], P)
	le.PutUint32(h[16:], uint32(k.recordSize()))
	le.PutUint64(h[24:], uint64(b.PriceMin))
	le.PutUint64(h[32:], uint64(b.PriceMax))
	le.PutUint64(h[40:], uint64(b.MaxOrders))
	return h
}

// AppendCmdLine appends a JSONL command record (no newline): canonical engine line + "iseq".
func AppendCmdLine(iseq uint64, sym uint32, c *matcher.Command, b []byte) []byte {
	b = WriteCommand(c, sym, true, b)
	b = b[:len(b)-1]
	b = append(b, `,"iseq":`...)
	b = strconv.AppendUint(b, iseq, 10)
	return append(b, '}')
}

// EncodeCmd writes a 40-byte binary command record.
func EncodeCmd(iseq uint64, sym uint32, c *matcher.Command, r []byte) {
	le := binary.LittleEndian
	le.PutUint64(r[0:], iseq)
	le.PutUint32(r[8:], sym)
	r[13], r[14], r[15] = 0, 0, 0
	var price int64
	var qty uint64
	switch c.Kind {
	case matcher.CmdNew:
		r[12], r[13], r[14], r[15] = 1, byte(c.Side), byte(c.OType), byte(c.Tif)
		price, qty = c.Price, c.Qty
	case matcher.CmdCancel:
		r[12] = 2
	default:
		r[12] = 3
		price, qty = c.Price, c.Qty
	}
	le.PutUint64(r[16:], c.OrderID)
	le.PutUint64(r[24:], uint64(price))
	le.PutUint64(r[32:], qty)
}

// CmdRec is one command record.
type CmdRec struct {
	Iseq uint64
	Sym  uint32
	Cmd  matcher.Command
}

func decodeCmd(r []byte) (CmdRec, bool) {
	le := binary.LittleEndian
	rec := CmdRec{Iseq: le.Uint64(r), Sym: le.Uint32(r[8:])}
	id, price, qty := le.Uint64(r[16:]), int64(le.Uint64(r[24:])), le.Uint64(r[32:])
	switch r[12] {
	case 1:
		if r[13] > 1 || r[14] > 1 || r[15] > 3 {
			return rec, false
		}
		rec.Cmd = matcher.Command{Kind: matcher.CmdNew, OrderID: id, Side: matcher.Side(r[13]),
			OType: matcher.OType(r[14]), Price: price, Qty: qty, Tif: matcher.Tif(r[15])}
	case 2:
		rec.Cmd = matcher.Cancel(id)
	case 3:
		rec.Cmd = matcher.Replace(id, price, qty)
	default:
		return rec, false
	}
	return rec, true
}

// EncodeEvt writes a 48-byte binary event record (spec/JOURNAL.md §3.2).
func EncodeEvt(seq uint64, sym uint32, e *matcher.Event, r []byte) {
	le := binary.LittleEndian
	le.PutUint64(r[0:], seq)
	le.PutUint32(r[8:], sym)
	var a, b, d uint64
	var c int64
	reason := byte(0)
	switch e.Kind {
	case matcher.EvAccepted:
		a, d = e.OrderID, e.LeavesQty
	case matcher.EvRejected:
		a, reason = e.OrderID, e.Reason+1
	case matcher.EvTrade:
		a, b, c, d = e.Maker, e.Taker, e.Price, e.Qty
	case matcher.EvClosed:
		a, reason = e.OrderID, e.Reason+1
	case matcher.EvReplaced:
		a, c, d = e.OrderID, e.Price, e.Qty
	}
	r[12], r[13], r[14], r[15] = byte(e.Kind)+1, reason, 0, 0
	le.PutUint64(r[16:], a)
	le.PutUint64(r[24:], b)
	le.PutUint64(r[32:], uint64(c))
	le.PutUint64(r[40:], d)
}

func decodeEvt(r []byte) (uint64, uint32, matcher.Event, bool) {
	le := binary.LittleEndian
	seq, sym := le.Uint64(r), le.Uint32(r[8:])
	a, b, c, d := le.Uint64(r[16:]), le.Uint64(r[24:]), int64(le.Uint64(r[32:])), le.Uint64(r[40:])
	reason := r[13]
	var e matcher.Event
	switch r[12] {
	case 1:
		e = matcher.Event{Kind: matcher.EvAccepted, OrderID: a, LeavesQty: d}
		return seq, sym, e, reason == 0
	case 2:
		e = matcher.Event{Kind: matcher.EvRejected, OrderID: a, Reason: reason - 1}
		return seq, sym, e, reason >= 1 && reason <= 7
	case 3:
		e = matcher.Event{Kind: matcher.EvTrade, Maker: a, Taker: b, Price: c, Qty: d}
		return seq, sym, e, reason == 0
	case 4:
		e = matcher.Event{Kind: matcher.EvClosed, OrderID: a, Reason: reason - 1}
		return seq, sym, e, reason >= 1 && reason <= 3
	case 5:
		e = matcher.Event{Kind: matcher.EvReplaced, OrderID: a, Price: c, Qty: d}
		return seq, sym, e, reason == 0
	}
	return seq, sym, e, false
}

// ---- reading (recovery) --------------------------------------------------------------------

// JournalHeader is a journal file's header.
type JournalHeader struct {
	Kind       JournalKind
	Partition  uint32
	Partitions uint32
	Book       matcher.BookConfig
}

func parseJsonlHeader(p, line string) (JournalHeader, error) {
	var h JournalHeader
	if f, _ := Get(line, "format"); f != "orderer-journal/1" {
		return h, corrupt(p, "not an orderer-journal/1 header")
	}
	switch k, _ := Get(line, "kind"); k {
	case "cmd":
		h.Kind = KindCmd
	case "evt":
		h.Kind = KindEvt
	default:
		return h, corrupt(p, "bad kind")
	}
	part, ok := U64(line, "partition")
	if !ok || part > 0xFFFFFFFF {
		return h, corrupt(p, "bad partition")
	}
	parts, ok := U64(line, "partitions")
	if !ok || parts > 0xFFFFFFFF {
		return h, corrupt(p, "bad partitions")
	}
	h.Partition, h.Partitions = uint32(part), uint32(parts)
	if h.Book.PriceMin, ok = I64(line, "pmin"); !ok {
		return h, corrupt(p, "bad pmin")
	}
	if h.Book.PriceMax, ok = I64(line, "pmax"); !ok {
		return h, corrupt(p, "bad pmax")
	}
	mo, ok := U64(line, "max_orders")
	if !ok {
		return h, corrupt(p, "bad max_orders")
	}
	h.Book.MaxOrders = int(mo)
	switch ix, _ := Get(line, "index"); ix {
	case "ladder":
		h.Book.Index = matcher.IndexLadder
	case "tree":
		h.Book.Index = matcher.IndexTree
	default:
		return h, corrupt(p, "bad index")
	}
	return h, nil
}

func parseBinaryHeader(p string, b []byte) (JournalHeader, error) {
	var h JournalHeader
	if len(b) < Header || string(b[:4]) != "ORDJ" {
		return h, corrupt(p, "bad magic")
	}
	le := binary.LittleEndian
	if le.Uint16(b[4:]) != 1 {
		return h, corrupt(p, "unsupported version")
	}
	switch b[6] {
	case 1:
		h.Kind = KindCmd
	case 2:
		h.Kind = KindEvt
	default:
		return h, corrupt(p, "bad kind")
	}
	if le.Uint32(b[16:]) != uint32(h.Kind.recordSize()) {
		return h, corrupt(p, "bad record_size")
	}
	if b[7] > 1 {
		return h, corrupt(p, "bad index")
	}
	h.Partition, h.Partitions = le.Uint32(b[8:]), le.Uint32(b[12:])
	h.Book = matcher.BookConfig{PriceMin: int64(le.Uint64(b[24:])), PriceMax: int64(le.Uint64(b[32:])),
		MaxOrders: int(le.Uint64(b[40:])), Index: matcher.IndexKind(b[7])}
	return h, nil
}

func readFile(p string) ([]byte, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, corrupt(p, err.Error())
	}
	return b, nil
}

// ReadJournalHeader reads just a journal's header.
func ReadJournalHeader(p string, f JournalFormat) (JournalHeader, error) {
	b, err := readFile(p)
	if err != nil {
		return JournalHeader{}, err
	}
	if f == Binary {
		return parseBinaryHeader(p, b)
	}
	t := string(b)
	if i := strings.IndexByte(t, '\n'); i >= 0 {
		t = t[:i]
	}
	return parseJsonlHeader(p, t)
}

func jsonlLines(p string, b []byte) ([]string, error) {
	if len(b) > 0 && b[len(b)-1] != '\n' {
		return nil, corrupt(p, "torn tail (final line has no newline)")
	}
	t := string(b)
	if t == "" {
		return nil, nil
	}
	return strings.Split(t[:len(t)-1], "\n"), nil
}

// ReadCmdJournal reads one command journal strictly.
func ReadCmdJournal(p string, f JournalFormat) (JournalHeader, []CmdRec, error) {
	b, err := readFile(p)
	if err != nil {
		return JournalHeader{}, nil, err
	}
	var h JournalHeader
	var recs []CmdRec
	if f == Binary {
		if h, err = parseBinaryHeader(p, b); err != nil {
			return h, nil, err
		}
		body := len(b) - Header
		if body%CmdRecord != 0 {
			return h, nil, corrupt(p, "torn tail (partial record)")
		}
		for i := 0; i < body/CmdRecord; i++ {
			r, ok := decodeCmd(b[Header+i*CmdRecord:])
			if !ok {
				return h, nil, corrupt(p, fmt.Sprintf("record %d: bad codes", i))
			}
			recs = append(recs, r)
		}
	} else {
		lines, err := jsonlLines(p, b)
		if err != nil {
			return h, nil, err
		}
		first := ""
		if len(lines) > 0 {
			first = lines[0]
		}
		if h, err = parseJsonlHeader(p, first); err != nil {
			return h, nil, err
		}
		for i := 1; i < len(lines); i++ {
			l := lines[i]
			iseq, ok1 := U64(l, "iseq")
			sym, ok2 := U64(l, "symbol")
			c, ok3 := ParseCommand(l)
			if !ok1 || !ok2 || sym > 0xFFFFFFFF || !ok3 {
				return h, nil, corrupt(p, fmt.Sprintf("line %d: malformed record: %s", i+1, l))
			}
			recs = append(recs, CmdRec{iseq, uint32(sym), c})
		}
	}
	if h.Kind != KindCmd {
		return h, nil, corrupt(p, "not a command journal")
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].Iseq <= recs[i-1].Iseq {
			return h, nil, corrupt(p, fmt.Sprintf("iseq not increasing (%d then %d)", recs[i-1].Iseq, recs[i].Iseq))
		}
	}
	return h, recs, nil
}

// ReadCmdDir reads every partition's command journal in dir.
func ReadCmdDir(dir string, f JournalFormat) (JournalHeader, [][]CmdRec, error) {
	first := JournalPath(dir, KindCmd, 0, f)
	h0, r0, err := ReadCmdJournal(first, f)
	if err != nil {
		return h0, nil, err
	}
	if h0.Partition != 0 {
		return h0, nil, corrupt(first, "header partition is not 0")
	}
	all := [][]CmdRec{r0}
	for p := uint32(1); p < h0.Partitions; p++ {
		path := JournalPath(dir, KindCmd, p, f)
		h, r, err := ReadCmdJournal(path, f)
		if err != nil {
			return h0, nil, err
		}
		if h.Partition != p || h.Partitions != h0.Partitions || !SameBook(h.Book, h0.Book) {
			return h0, nil, corrupt(path, "header does not match its file name, partition count or book config")
		}
		all = append(all, r)
	}
	return h0, all, nil
}

// ReadEvtJournal reads an event journal as canonical symbol-tagged lines.
func ReadEvtJournal(p string, f JournalFormat) ([]string, error) {
	b, err := readFile(p)
	if err != nil {
		return nil, err
	}
	var out []string
	if f == Binary {
		if _, err := parseBinaryHeader(p, b); err != nil {
			return nil, err
		}
		body := len(b) - Header
		if body%EvtRecord != 0 {
			return nil, corrupt(p, "torn tail (partial record)")
		}
		var buf []byte
		for i := 0; i < body/EvtRecord; i++ {
			seq, sym, e, ok := decodeEvt(b[Header+i*EvtRecord:])
			if !ok {
				return nil, corrupt(p, fmt.Sprintf("record %d: bad codes", i))
			}
			buf = buf[:0]
			e.WriteCanonicalSym(seq, sym, &buf)
			out = append(out, string(buf))
		}
		return out, nil
	}
	lines, err := jsonlLines(p, b)
	if err != nil {
		return nil, err
	}
	first := ""
	if len(lines) > 0 {
		first = lines[0]
	}
	if _, err := parseJsonlHeader(p, first); err != nil {
		return nil, err
	}
	if len(lines) > 1 {
		out = append(out, lines[1:]...)
	}
	return out, nil
}

// MergeJournals is one iseq-ordered stream of records after `after`; iseqs must be disjoint.
func MergeJournals(parts [][]CmdRec, after uint64) ([]CmdRec, error) {
	var all []CmdRec
	for _, p := range parts {
		for _, r := range p {
			if r.Iseq > after {
				all = append(all, r)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Iseq < all[j].Iseq })
	for i := 1; i < len(all); i++ {
		if all[i].Iseq == all[i-1].Iseq {
			return nil, &CorruptJournal{fmt.Sprintf("iseq %d appears in two partitions", all[i].Iseq)}
		}
	}
	return all, nil
}

// ---- writing ---------------------------------------------------------------------------------

// openJournal creates (header written) or opens for append (header checked).
func openJournal(cfg *JournalConfig, k JournalKind, p, P uint32, book matcher.BookConfig) (*os.File, error) {
	path := JournalPath(cfg.Dir, k, p, cfg.Format)
	if cfg.Append {
		if _, err := os.Stat(path); err == nil {
			h, err := ReadJournalHeader(path, cfg.Format)
			if err != nil {
				return nil, err
			}
			if h != (JournalHeader{k, p, P, book}) {
				return nil, corrupt(path, "header does not match the pipeline")
			}
			return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	var h []byte
	if cfg.Format == Jsonl {
		h = []byte(jsonlHeader(k, p, P, book))
	} else {
		h = binaryHeader(k, p, P, book)
	}
	if _, err := f.Write(h); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

const (
	chunkSize = 1 << 18
	chunks    = 64
)

type chunk struct {
	buf     []byte
	last    uint64 // id of the last record in the chunk
	records uint64
}

// ChunkWriter is the asynchronous journal writer: the owner encodes records
// into a chunk; full or idle chunks go to a dedicated I/O goroutine that
// writes and group-commits fsyncs (File.Sync: F_FULLFSYNC on macOS, fsync on
// Linux). Chunks are preallocated and recycled.
type ChunkWriter struct {
	f                *os.File
	fsync            *FsyncPolicy
	flushed, durable *atomic.Uint64
	cs               []chunk
	cur              int
	last, records    uint64
	toIO, free       chan int
	done             chan struct{}
	finished         bool
	err              atomic.Pointer[string]
}

const stopChunk = -1

func newChunkWriter(f *os.File, fsync *FsyncPolicy, flushed, durable *atomic.Uint64) *ChunkWriter {
	w := &ChunkWriter{f: f, fsync: fsync, flushed: flushed, durable: durable, cs: make([]chunk, chunks),
		toIO: make(chan int, chunks+2), free: make(chan int, chunks+2), done: make(chan struct{})}
	for i := range w.cs {
		w.cs[i].buf = make([]byte, 0, chunkSize)
		if i > 0 {
			w.free <- i
		}
	}
	go w.ioLoop()
	return w
}

// reserve makes room for one record of at most n bytes.
func (w *ChunkWriter) reserve(n int) {
	if len(w.cs[w.cur].buf)+n > chunkSize {
		w.handOff()
	}
}

func (w *ChunkWriter) pending() int { return len(w.cs[w.cur].buf) }

func (w *ChunkWriter) pushCmd(f JournalFormat, iseq uint64, sym uint32, c *matcher.Command) {
	w.reserve(maxRecord)
	ch := &w.cs[w.cur]
	if f == Binary {
		n := len(ch.buf)
		ch.buf = ch.buf[:n+CmdRecord]
		EncodeCmd(iseq, sym, c, ch.buf[n:])
	} else {
		ch.buf = append(AppendCmdLine(iseq, sym, c, ch.buf), '\n')
	}
	w.last = iseq
	w.records++
}

func (w *ChunkWriter) pushEvt(f JournalFormat, seq uint64, sym uint32, e *matcher.Event) {
	w.reserve(maxRecord)
	ch := &w.cs[w.cur]
	if f == Binary {
		n := len(ch.buf)
		ch.buf = ch.buf[:n+EvtRecord]
		EncodeEvt(seq, sym, e, ch.buf[n:])
	} else {
		e.WriteCanonicalSym(seq, sym, &ch.buf)
		ch.buf = append(ch.buf, '\n')
	}
	w.last = seq
	w.records++
}

// handOff hands the chunk to the I/O goroutine (blocks only when every chunk is in flight).
func (w *ChunkWriter) handOff() {
	ch := &w.cs[w.cur]
	if len(ch.buf) == 0 {
		return
	}
	ch.last, ch.records = w.last, w.records
	w.records = 0
	w.toIO <- w.cur
	w.cur = <-w.free
}

// finish writes and (per policy) syncs everything; the first I/O error, if any.
func (w *ChunkWriter) finish() error {
	if !w.finished {
		w.finished = true
		w.handOff()
		w.toIO <- stopChunk
		<-w.done
		if err := w.f.Close(); err != nil {
			w.setErr("journal close: " + err.Error())
		}
	}
	if e := w.err.Load(); e != nil {
		return errors.New(*e)
	}
	return nil
}

func (w *ChunkWriter) setErr(s string) { w.err.CompareAndSwap(nil, &s) }

func (w *ChunkWriter) write(c int) {
	if _, err := w.f.Write(w.cs[c].buf); err != nil {
		w.setErr("journal write: " + err.Error())
	}
	w.cs[c].buf = w.cs[c].buf[:0]
}

func (w *ChunkWriter) sync(written uint64) {
	if w.err.Load() != nil {
		return
	}
	if err := w.f.Sync(); err != nil {
		w.setErr("journal fsync: " + err.Error())
		return
	}
	w.durable.Store(written)
}

func (w *ChunkWriter) ioLoop() {
	defer close(w.done)
	var unsynced uint64
	written := w.flushed.Load()
	lastSync := time.Now()
	idle := time.Duration(1<<63 - 1)
	if w.fsync != nil {
		idle = w.fsync.Idle
	}
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		var m int
		if unsynced > 0 {
			timer.Reset(idle)
			select {
			case m = <-w.toIO:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				w.sync(written)
				unsynced, lastSync = 0, time.Now()
				continue
			}
		} else {
			m = <-w.toIO
		}
		stop := false
	drain: // write everything queued, then one fsync decision
		for {
			if m == stopChunk {
				stop = true
				break
			}
			w.write(m)
			written = w.cs[m].last
			w.flushed.Store(written)
			if r := w.cs[m].records; r > 0 {
				unsynced += r
			} else {
				unsynced++
			}
			w.free <- m
			select {
			case m = <-w.toIO:
			default:
				break drain
			}
		}
		due := false
		switch {
		case w.fsync == nil || w.fsync.Mode == FsyncNever:
			w.durable.Store(written)
			unsynced = 0
		case w.fsync.Mode == FsyncEveryN:
			due = unsynced >= w.fsync.N
		default:
			due = time.Since(lastSync) >= w.fsync.Interval
		}
		if due || (stop && unsynced > 0) {
			w.sync(written)
			unsynced, lastSync = 0, time.Now()
		}
		if stop {
			return
		}
	}
}
