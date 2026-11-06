package cluster

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abhijitkrm/orderer-go/consensus"
)

// TCP transport between replicas, wire-compatible with orderer-rust
// (transport.rs) and orderer-cpp. Sends never block the protocol: each peer
// has a bounded queue and a goroutine that reconnects with backoff; a full
// queue or a down peer drops messages, which the protocol tolerates.
// Snapshots stream in chunks and are verified by length and checksum before
// they are accepted. With a cluster key, every connection runs the
// challenge-response handshake of auth.go and every frame is MAC'd.

const (
	kMsg       = 1
	kSnapBegin = 2
	kSnapChunk = 3
	kSnapEnd   = 4
	kDigest    = 5
	kHello     = 6
	kChallenge = 7
	kAuth      = 8

	snapChunk = 1 << 20
	peerQueue = 8192
)

type inboundKind int

const (
	inMsg inboundKind = iota
	inSnapshot
	inDigest
)

// inbound is what the transport hands to the node.
type inbound struct {
	kind     inboundKind
	from     consensus.NodeID
	msg      consensus.Msg   // inMsg
	term     consensus.Term  // inSnapshot
	lastTerm consensus.Term  // inSnapshot
	index    consensus.Index // inSnapshot, inDigest
	digest   Digest          // inDigest
}

// Filter is a test hook that cuts links: Allows(from, to) is checked on every
// send and receive.
type Filter struct {
	mu      sync.Mutex
	blocked map[[2]consensus.NodeID]bool
}

// NewFilter returns a filter that allows everything.
func NewFilter() *Filter { return &Filter{blocked: map[[2]consensus.NodeID]bool{}} }

// Block cuts the link between a and b in both directions.
func (f *Filter) Block(a, b consensus.NodeID) {
	f.mu.Lock()
	f.blocked[[2]consensus.NodeID{a, b}] = true
	f.blocked[[2]consensus.NodeID{b, a}] = true
	f.mu.Unlock()
}

// Isolate cuts n off from every other node in all.
func (f *Filter) Isolate(n consensus.NodeID, all []consensus.NodeID) {
	for _, o := range all {
		if o != n {
			f.Block(n, o)
		}
	}
}

// Heal restores every link.
func (f *Filter) Heal() {
	f.mu.Lock()
	f.blocked = map[[2]consensus.NodeID]bool{}
	f.mu.Unlock()
}

// Allows reports whether from may talk to to.
func (f *Filter) Allows(from, to consensus.NodeID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.blocked[[2]consensus.NodeID{from, to}]
}

// NetStats counts transport activity.
type NetStats struct{ Sent, Dropped, Received, BadFrames atomic.Uint64 }

type jobKind int

const (
	jMsg jobKind = iota
	jSnapshot
	jDigest
)

type job struct {
	kind     jobKind
	msg      consensus.Msg
	term     consensus.Term
	lastTerm consensus.Term
	index    consensus.Index
	digest   Digest
}

type peer struct {
	addr string
	q    chan job
}

// Member is one replica's id and peer address ("host:port").
type Member struct {
	ID   consensus.NodeID
	Addr string
}

// Transport connects one replica to the others.
type Transport struct {
	me        consensus.NodeID
	ids       []consensus.NodeID
	clusterID uint64
	sink      func(inbound)
	snaps     *SnapStore
	filter    *Filter
	key       []byte // nil: no authentication

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	ln       net.Listener
	peers    map[consensus.NodeID]*peer

	mu    sync.Mutex
	conns map[net.Conn]bool

	Stats NetStats
}

// NewTransport listens on this member's address and starts the per-peer senders.
func NewTransport(me consensus.NodeID, members []Member, clusterID uint64, sink func(inbound),
	snaps *SnapStore, filter *Filter, key []byte) (*Transport, error) {
	t := &Transport{me: me, clusterID: clusterID, sink: sink, snaps: snaps, filter: filter, key: key,
		stop: make(chan struct{}), peers: map[consensus.NodeID]*peer{}, conns: map[net.Conn]bool{}}
	var mine string
	for _, m := range members {
		t.ids = append(t.ids, m.ID)
		if m.ID == me {
			mine = m.Addr
		}
	}
	ln, err := net.Listen("tcp", mine)
	if err != nil {
		return nil, err
	}
	t.ln = ln
	t.wg.Add(1)
	go t.acceptLoop()
	for _, m := range members {
		if m.ID == me {
			continue
		}
		p := &peer{addr: m.Addr, q: make(chan job, peerQueue)}
		t.peers[m.ID] = p
		t.wg.Add(1)
		go t.senderLoop(m.ID, p)
	}
	return t, nil
}

func (t *Transport) enqueue(to consensus.NodeID, j job) {
	if !t.filter.Allows(t.me, to) {
		t.Stats.Dropped.Add(1)
		return
	}
	p := t.peers[to]
	if p == nil {
		return
	}
	select {
	case p.q <- j:
	default:
		t.Stats.Dropped.Add(1)
	}
}

// Send queues a protocol message.
func (t *Transport) Send(to consensus.NodeID, m consensus.Msg) {
	t.enqueue(to, job{kind: jMsg, msg: m})
}

// SendSnapshot streams the stored snapshot at index to a peer.
func (t *Transport) SendSnapshot(to consensus.NodeID, term consensus.Term, index consensus.Index, lastTerm consensus.Term) {
	t.enqueue(to, job{kind: jSnapshot, term: term, index: index, lastTerm: lastTerm})
}

// SendDigest sends this node's state digest at index.
func (t *Transport) SendDigest(to consensus.NodeID, index consensus.Index, d Digest) {
	t.enqueue(to, job{kind: jDigest, index: index, digest: d})
}

// Shutdown stops every goroutine and closes every connection.
func (t *Transport) Shutdown() {
	t.stopOnce.Do(func() {
		close(t.stop)
		t.ln.Close()
		t.mu.Lock()
		for c := range t.conns {
			c.Close()
		}
		t.mu.Unlock()
	})
	t.wg.Wait()
}

func (t *Transport) stopped() bool {
	select {
	case <-t.stop:
		return true
	default:
		return false
	}
}

// track registers a connection so Shutdown can close it; false = already shut down.
func (t *Transport) track(c net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped() {
		return false
	}
	t.conns[c] = true
	return true
}

func (t *Transport) untrack(c net.Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
	c.Close()
}

func (t *Transport) sleep(d time.Duration) {
	select {
	case <-time.After(d):
	case <-t.stop:
	}
}

func (t *Transport) helloBody() []byte {
	return putU64([]byte{byte(t.me)}, t.clusterID)
}

// ---- outbound ----

func (t *Transport) initiatorHandshake(c *conn, peerID consensus.NodeID) (*session, bool) {
	ns := newNonce()
	if writeFrame(c, kHello, append(t.helloBody(), ns...), nil) != nil {
		return nil, false
	}
	f, err := c.read(nil, 2*time.Second)
	if err != nil || f.kind != kChallenge || len(f.body) != nonceLen+macLen {
		return nil, false
	}
	ids := []byte{byte(t.me), byte(peerID)}
	want := handshake(t.key, "chal", ns, f.body[:nonceLen], ids)
	if !ctEq(want[:macLen], f.body[nonceLen:]) {
		return nil, false
	}
	s := newSession(handshake(t.key, "sess", ns, f.body[:nonceLen], ids), 0)
	// Proves this side knows the key too.
	if writeFrame(c, kAuth, nil, s) != nil {
		return nil, false
	}
	return s, true
}

func (t *Transport) senderLoop(id consensus.NodeID, p *peer) {
	defer t.wg.Done()
	backoff := 20 * time.Millisecond
	for !t.stopped() {
		c, err := dial(p.addr, 300*time.Millisecond)
		if err != nil {
			// Shed what queued meanwhile.
			for drained := false; !drained; {
				select {
				case <-p.q:
					t.Stats.Dropped.Add(1)
				default:
					drained = true
				}
			}
			t.sleep(backoff)
			if backoff *= 2; backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
			continue
		}
		backoff = 20 * time.Millisecond
		if !t.track(c) {
			c.Close()
			return
		}
		t.sendOn(id, p, c)
		t.untrack(c)
	}
}

func (t *Transport) sendOn(id consensus.NodeID, p *peer, c *conn) {
	var sess *session
	if t.key != nil {
		var ok bool
		if sess, ok = t.initiatorHandshake(c, id); !ok {
			t.Stats.BadFrames.Add(1)
			t.sleep(200 * time.Millisecond)
			return
		}
	} else if writeFrame(c, kHello, t.helloBody(), nil) != nil {
		return
	}
	var out []byte
	flush := func() bool {
		if len(out) == 0 {
			return true
		}
		_, err := c.Write(out)
		out = out[:0]
		return err == nil
	}
	for {
		var j job
		select {
		case j = <-p.q:
		case <-t.stop:
			return
		}
		for more := true; more; {
			if t.filter.Allows(t.me, id) {
				t.Stats.Sent.Add(1)
				if !t.writeJob(c, &out, sess, j) {
					return
				}
				if len(out) > 256<<10 && !flush() {
					return
				}
			}
			select {
			case j = <-p.q:
			default:
				more = false
			}
		}
		if !flush() {
			return
		}
	}
}

// writeJob appends a job's frames to out; false = the connection failed.
func (t *Transport) writeJob(c *conn, out *[]byte, sess *session, j job) bool {
	switch j.kind {
	case jMsg:
		body := encodeMsg(j.msg, nil)
		*out = putFrame(*out, kMsg, body, sess)
	case jDigest:
		body := putU64(nil, uint64(j.index))
		body = append(body, j.digest[:]...)
		*out = putFrame(*out, kDigest, body, sess)
	case jSnapshot:
		return t.streamSnapshot(c, out, sess, j)
	}
	return true
}

func (t *Transport) streamSnapshot(c *conn, out *[]byte, sess *session, j job) bool {
	meta, data, ok := t.snaps.Get(j.index)
	if !ok {
		return true // gone (superseded): the peer will ask again
	}
	b := putU32(nil, uint32(j.term))
	b = putU64(b, uint64(j.index))
	b = putU32(b, uint32(j.lastTerm))
	b = putU64(b, meta.Len)
	b = putU32(b, meta.CRC)
	*out = putFrame(*out, kSnapBegin, b, sess)
	for at := 0; at < len(data); at += snapChunk {
		end := min(at+snapChunk, len(data))
		*out = putFrame(*out, kSnapChunk, data[at:end], sess)
		if _, err := c.Write(*out); err != nil {
			return false
		}
		*out = (*out)[:0]
	}
	*out = putFrame(*out, kSnapEnd, nil, sess)
	return true
}

// ---- inbound ----

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		nc, err := t.ln.Accept()
		if err != nil {
			if t.stopped() {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			t.sleep(10 * time.Millisecond)
			continue
		}
		c := newConn(nc)
		if !t.track(c) {
			c.Close()
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.untrack(c)
			t.readerLoop(c)
		}()
	}
}

func (t *Transport) known(id consensus.NodeID) bool {
	for _, x := range t.ids {
		if x == id {
			return true
		}
	}
	return false
}

func (t *Transport) readerLoop(c *conn) {
	bad := func() { t.Stats.BadFrames.Add(1) }
	first, err := c.read(nil, 5*time.Second)
	if err != nil {
		return
	}
	want := 9
	if t.key != nil {
		want += nonceLen
	}
	if first.kind != kHello || len(first.body) != want {
		bad()
		return
	}
	from := consensus.NodeID(first.body[0])
	if le.Uint64(first.body[1:]) != t.clusterID || from == t.me || !t.known(from) {
		bad()
		return
	}
	var sess *session
	if t.key != nil {
		ns := first.body[9:]
		nr := newNonce()
		ids := []byte{byte(from), byte(t.me)}
		mac := handshake(t.key, "chal", ns, nr, ids)
		if writeFrame(c, kChallenge, append(append([]byte(nil), nr...), mac[:macLen]...), nil) != nil {
			return
		}
		sess = newSession(handshake(t.key, "sess", ns, nr, ids), 1)
		a, err := c.read(sess, 2*time.Second)
		if err != nil || a.kind != kAuth || len(a.body) != 0 {
			bad()
			return
		}
	}
	var incoming *Incoming
	defer func() {
		if incoming != nil {
			incoming.Abort()
		}
	}()
	var inTerm, inLast consensus.Term
	var inIndex consensus.Index
	for {
		f, err := c.read(sess, 0)
		if err != nil {
			if isFrameErr(err) {
				bad()
			}
			return
		}
		if !t.filter.Allows(from, t.me) {
			continue
		}
		t.Stats.Received.Add(1)
		r := reader{b: f.body}
		switch f.kind {
		case kMsg:
			m := decodeMsg(&r)
			if r.finish() != nil {
				bad()
				return
			}
			t.sink(inbound{kind: inMsg, from: from, msg: m})
		case kDigest:
			idx := consensus.Index(r.u64())
			d := r.bytes(32)
			if r.err != nil {
				bad()
				return
			}
			in := inbound{kind: inDigest, from: from, index: idx}
			copy(in.digest[:], d)
			t.sink(in)
		case kSnapBegin:
			inTerm = consensus.Term(r.u32())
			inIndex = consensus.Index(r.u64())
			inLast = consensus.Term(r.u32())
			n, crc := r.u64(), r.u32()
			if r.err != nil {
				bad()
				return
			}
			if incoming != nil {
				incoming.Abort()
				incoming = nil
			}
			if incoming, err = t.snaps.Receive(inIndex, inLast, n, crc); err != nil {
				return
			}
		case kSnapChunk:
			if incoming == nil {
				bad()
				return
			}
			if incoming.Write(f.body) != nil {
				return
			}
		case kSnapEnd:
			if incoming == nil {
				bad()
				return
			}
			err := incoming.Finish()
			incoming = nil
			if err != nil {
				return
			}
			t.sink(inbound{kind: inSnapshot, from: from, term: inTerm, index: inIndex, lastTerm: inLast})
		default:
			bad()
			return
		}
	}
}
