package cluster

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// TCP helpers and the framing shared by the replica transport and the client
// protocol: len u32 | crc32c u32 | kind u8 | body [| 16-byte MAC once the
// connection is authenticated]. len and crc cover kind, body and MAC.
// Byte-compatible with orderer-rust and orderer-cpp.

const maxNetFrame = 64 << 20

// netError is a framing, checksum or authentication failure on a connection.
type netError struct{ msg string }

func (e *netError) Error() string { return e.msg }

func isFrameErr(err error) bool {
	var ne *netError
	return errors.As(err, &ne)
}

type frame struct {
	kind byte
	body []byte
}

// putFrame appends one frame (sealed when sess is set) to out without sending.
func putFrame(out []byte, kind byte, body []byte, sess *session) []byte {
	extra := 0
	if sess != nil {
		extra = macLen
	}
	start := len(out)
	out = append(out, make([]byte, 8)...)
	out = append(out, kind)
	out = append(out, body...)
	if sess != nil {
		out = append(out, sess.seal(kind, body)...)
	}
	payload := out[start+8:]
	le.PutUint32(out[start:], uint32(1+len(body)+extra))
	le.PutUint32(out[start+4:], crc32c(payload))
	return out
}

func writeFrame(w io.Writer, kind byte, body []byte, sess *session) error {
	_, err := w.Write(putFrame(nil, kind, body, sess))
	return err
}

// readFrame reads one frame, verifying length, checksum and (with sess) the
// MAC. io.EOF means a clean close at a frame boundary.
func readFrame(r io.Reader, sess *session) (frame, error) {
	var head [8]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return frame{}, fmt.Errorf("unexpected eof")
		}
		return frame{}, err
	}
	n, crc := le.Uint32(head[:]), le.Uint32(head[4:])
	if n == 0 || n > maxNetFrame {
		return frame{}, &netError{"frame length"}
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			err = fmt.Errorf("unexpected eof")
		}
		return frame{}, err
	}
	if crc32c(body) != crc {
		return frame{}, &netError{"frame checksum"}
	}
	f := frame{kind: body[0], body: body[1:]}
	if sess != nil {
		if len(f.body) < macLen {
			return frame{}, &netError{"frame authentication"}
		}
		k := len(f.body) - macLen
		if !sess.open(f.kind, f.body[:k], f.body[k:]) {
			return frame{}, &netError{"frame authentication"}
		}
		f.body = f.body[:k]
	}
	return f, nil
}

// conn is a TCP connection with a buffered reader.
type conn struct {
	net.Conn
	r *bufio.Reader
}

func newConn(c net.Conn) *conn {
	if t, ok := c.(*net.TCPConn); ok {
		t.SetNoDelay(true)
	}
	return &conn{Conn: c, r: bufio.NewReaderSize(c, 64<<10)}
}

func (c *conn) read(sess *session, within time.Duration) (frame, error) {
	if within > 0 {
		c.SetReadDeadline(time.Now().Add(within))
		defer c.SetReadDeadline(time.Time{})
	}
	return readFrame(c.r, sess)
}

func dial(addr string, timeout time.Duration) (*conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newConn(c), nil
}
