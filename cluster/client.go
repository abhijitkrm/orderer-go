package cluster

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	orderer "github.com/abhijitkrm/orderer-go"
	"github.com/abhijitkrm/orderer-go/consensus"
)

// The client protocol, wire-compatible with orderer-rust (client.rs) and
// orderer-cpp: requests PROPOSE / STATUS / HASH / BARRIER over the framing of
// net.go, a leader-following Client, and exactly-once sessions. A server
// holding a cluster key requires the challenge-response handshake.

const (
	qHello   = 0
	qPropose = 1
	qStatus  = 2
	qHash    = 3
	qBarrier = 4

	crOK        = 1
	crNotLeader = 2
	crBusy      = 3
	crUnknown   = 4
	crErr       = 5
	crText      = 6
	crHash      = 7
	crChallenge = 8
)

// ClientServer serves one replica's client port. The Replica must outlive it.
type ClientServer struct {
	replica *Replica
	key     []byte
	ln      net.Listener
	stop    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]bool
}

// NewClientServer listens on addr and serves requests until Shutdown.
func NewClientServer(addr string, r *Replica, key []byte) (*ClientServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &ClientServer{replica: r, key: key, ln: ln, stop: make(chan struct{}), conns: map[net.Conn]bool{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			nc, err := ln.Accept()
			if err != nil {
				select {
				case <-s.stop:
					return
				default:
					time.Sleep(10 * time.Millisecond)
					continue
				}
			}
			c := newConn(nc)
			s.mu.Lock()
			s.conns[c] = true
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() {
					s.mu.Lock()
					delete(s.conns, c)
					s.mu.Unlock()
					c.Close()
				}()
				select {
				case <-s.stop:
					return
				default:
				}
				s.serve(c)
			}()
		}
	}()
	return s, nil
}

// Shutdown stops accepting and closes every connection.
func (s *ClientServer) Shutdown() {
	s.once.Do(func() {
		close(s.stop)
		s.ln.Close()
		s.mu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()
	})
	s.wg.Wait()
}

func roleName(r consensus.Role) string { return r.String() }

func (s *ClientServer) serve(c *conn) {
	var sess *session
	if s.key != nil {
		hello, err := c.read(nil, 5*time.Second)
		if err != nil || hello.kind != qHello || len(hello.body) != nonceLen {
			return
		}
		ns := newNonce()
		mac := handshake(s.key, "cchal", hello.body, ns)
		if writeFrame(c, crChallenge, append(append([]byte(nil), ns...), mac[:macLen]...), nil) != nil {
			return
		}
		sess = newSession(handshake(s.key, "csess", hello.body, ns), 1)
	}
	reply := func(kind byte, body []byte) bool { return writeFrame(c, kind, body, sess) == nil }
	for {
		f, err := c.read(sess, 0)
		if err != nil {
			return
		}
		ok := true
		switch f.kind {
		case qHello:
			reply(crErr, []byte("this server does not use authentication"))
			return
		case qPropose:
			ok = s.answer(reply, s.replica.Propose(f.body))
		case qBarrier:
			ok = s.answer(reply, s.replica.Barrier())
		case qStatus:
			st := s.replica.Status()
			halted := st.Halted
			if halted == "" {
				halted = "-"
			}
			ok = reply(crText, []byte(fmt.Sprintf(
				"id=%d role=%s term=%d leader=%d commit=%d applied=%d base=%d last=%d pending=%d halted=%s digests_verified=%d\n",
				st.ID, roleName(st.Role), st.Term, st.Leader, st.Commit, st.Applied, st.Base, st.LastIndex,
				st.Pending, halted, st.DigestsChecked)))
		case qHash:
			if idx, d, have := s.replica.StateHash(); have {
				ok = reply(crHash, append(putU64(nil, uint64(idx)), d[:]...))
			} else {
				ok = reply(crErr, []byte("state hash unsupported"))
			}
		default:
			ok = reply(crErr, []byte("unknown request"))
		}
		if !ok {
			return
		}
	}
}

func (s *ClientServer) answer(reply func(byte, []byte) bool, r ProposeResult) bool {
	switch r.Kind {
	case ProposeOK:
		return reply(crOK, putU64(nil, uint64(r.Index)))
	case ProposeNotLeader:
		h := byte(255)
		if r.Hint != consensus.None {
			h = byte(r.Hint)
		}
		return reply(crNotLeader, []byte{h})
	case ProposeStopped:
		return reply(crNotLeader, []byte{255})
	case ProposeBusy:
		return reply(crBusy, nil)
	default:
		return reply(crUnknown, []byte("unknown outcome"))
	}
}

// ClientResultKind classifies a client call's outcome.
type ClientResultKind int

const (
	// ClientOK: committed on a quorum.
	ClientOK ClientResultKind = iota
	// ClientNoLeader: no leader could be reached in time.
	ClientNoLeader
	// ClientUnknown: the outcome is unknown; it may still commit. Never retry blindly (use a session).
	ClientUnknown
	// ClientProtocol: a malformed or unexpected reply.
	ClientProtocol
)

// ClientResult is the outcome of Propose.
type ClientResult struct {
	Kind  ClientResultKind
	Index consensus.Index
	Text  string
}

// OK reports whether the call committed.
func (r ClientResult) OK() bool { return r.Kind == ClientOK }

// Conn is a connection to one replica's client port.
type Conn struct {
	c    *conn
	sess *session
}

// Connect opens a connection, authenticating when key is set; nil on failure.
func Connect(addr string, key []byte) *Conn {
	c, err := dial(addr, 500*time.Millisecond)
	if err != nil {
		return nil
	}
	var sess *session
	if key != nil {
		nc := newNonce()
		if writeFrame(c, qHello, nc, nil) != nil {
			c.Close()
			return nil
		}
		f, err := c.read(nil, 30*time.Second)
		if err != nil || f.kind != crChallenge || len(f.body) != nonceLen+macLen {
			c.Close()
			return nil
		}
		want := handshake(key, "cchal", nc, f.body[:nonceLen])
		if !ctEq(want[:macLen], f.body[nonceLen:]) {
			c.Close()
			return nil
		}
		sess = newSession(handshake(key, "csess", nc, f.body[:nonceLen]), 0)
	}
	return &Conn{c, sess}
}

// Close closes the connection.
func (c *Conn) Close() { c.c.Close() }

// call sends a request and waits for the reply; ok=false on any I/O or protocol failure.
func (c *Conn) call(kind byte, body []byte) (frame, bool) {
	if writeFrame(c.c, kind, body, c.sess) != nil {
		return frame{}, false
	}
	f, err := c.c.read(c.sess, 30*time.Second)
	return f, err == nil
}

// Status is the replica's status line.
func (c *Conn) Status() (string, bool) {
	f, ok := c.call(qStatus, nil)
	if !ok || f.kind != crText {
		return "", false
	}
	return string(f.body), true
}

// Barrier is a linearizable read barrier (this node must be the leader).
func (c *Conn) Barrier() (consensus.Index, bool) {
	f, ok := c.call(qBarrier, nil)
	if !ok || f.kind != crOK || len(f.body) != 8 {
		return 0, false
	}
	return consensus.Index(le.Uint64(f.body)), true
}

// Hash is the node's (applied index, state digest).
func (c *Conn) Hash() (consensus.Index, Digest, bool) {
	f, ok := c.call(qHash, nil)
	if !ok || f.kind != crHash || len(f.body) != 40 {
		return 0, Digest{}, false
	}
	var d Digest
	copy(d[:], f.body[8:])
	return consensus.Index(le.Uint64(f.body)), d, true
}

// Client submits batches to a cluster, following leader hints and surviving restarts.
type Client struct {
	addrs   []Member
	conns   []*Conn
	current int
	key     []byte
	session *SessionKey // Client = id, Seq = last used
	pending []byte

	// RetryBudget bounds ProposeSession's retries; default 30s.
	RetryBudget time.Duration
}

// NewClient addresses a cluster by its members' client ports.
func NewClient(addrs []Member) *Client {
	return &Client{addrs: addrs, conns: make([]*Conn, len(addrs)), RetryBudget: 30 * time.Second}
}

// WithKey sets the cluster key for authenticated connections.
func (c *Client) WithKey(key []byte) *Client { c.key = key; return c }

// WithSession gives exactly-once submission under session id (one sequential stream per id).
func (c *Client) WithSession(id uint64) *Client { c.session = &SessionKey{Client: id}; return c }

// Close closes every open connection.
func (c *Client) Close() {
	for i, cn := range c.conns {
		if cn != nil {
			cn.Close()
			c.conns[i] = nil
		}
	}
}

// Propose submits an encoded payload. OK = committed on a quorum. Unknown = may still commit.
func (c *Client) Propose(payload []byte) ClientResult {
	for attempt := 0; attempt < 600; attempt++ {
		i := c.current % len(c.addrs)
		if c.conns[i] == nil {
			c.conns[i] = Connect(c.addrs[i].Addr, c.key)
		}
		cn := c.conns[i]
		if cn == nil {
			c.current++
			time.Sleep(10 * time.Millisecond)
			continue
		}
		f, ok := cn.call(qPropose, payload)
		if !ok {
			// Died mid-request: it may have been accepted. Unknown, never a blind retry.
			cn.Close()
			c.conns[i] = nil
			c.current++
			return ClientResult{Kind: ClientUnknown, Text: "connection lost"}
		}
		switch f.kind {
		case crOK:
			if len(f.body) != 8 {
				return ClientResult{Kind: ClientProtocol, Text: "bad reply"}
			}
			return ClientResult{Kind: ClientOK, Index: consensus.Index(le.Uint64(f.body))}
		case crNotLeader:
			pos := len(c.addrs)
			if len(f.body) > 0 {
				for k, a := range c.addrs {
					if byte(a.ID) == f.body[0] {
						pos = k
					}
				}
			}
			if pos < len(c.addrs) {
				c.current = pos
			} else {
				c.current++
			}
			time.Sleep(5 * time.Millisecond)
		case crBusy:
			time.Sleep(2 * time.Millisecond)
		case crUnknown:
			cn.Close()
			c.conns[i] = nil
			return ClientResult{Kind: ClientUnknown, Text: string(f.body)}
		default:
			return ClientResult{Kind: ClientProtocol, Text: fmt.Sprintf("reply %d", f.kind)}
		}
	}
	return ClientResult{Kind: ClientNoLeader}
}

// ProposeSession submits a batch with exactly-once effect: the same
// (session, seq) is retried through lost connections and leader changes until
// it is definitely committed.
func (c *Client) ProposeSession(cmds []orderer.SymCmd) ClientResult {
	if c.session == nil {
		return ClientResult{Kind: ClientProtocol, Text: "no session: call WithSession first"}
	}
	if c.pending == nil {
		c.session.Seq++
		c.pending = EncodeBatch(cmds, nil, &SessionKey{c.session.Client, c.session.Seq})
	}
	deadline := time.Now().Add(c.RetryBudget)
	for {
		r := c.Propose(c.pending)
		if r.OK() {
			c.pending = nil
			return r
		}
		if r.Kind == ClientProtocol || !time.Now().Before(deadline) {
			return r
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ParseMembers parses "ID=HOST:PORT,ID=HOST:PORT,...".
func ParseMembers(s string) ([]Member, error) {
	var out []Member
	for _, m := range strings.Split(s, ",") {
		eq := strings.IndexByte(m, '=')
		if eq < 0 {
			return nil, fmt.Errorf("want ID=HOST:PORT, got %q", m)
		}
		var id uint8
		if _, err := fmt.Sscanf(m[:eq], "%d", &id); err != nil {
			return nil, fmt.Errorf("bad node id %q", m[:eq])
		}
		out = append(out, Member{consensus.NodeID(id), strings.Replace(m[eq+1:], "localhost", "127.0.0.1", 1)})
	}
	return out, nil
}
