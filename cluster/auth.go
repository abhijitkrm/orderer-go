package cluster

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
)

// Pre-shared-key authentication: a challenge-response handshake proves both
// sides know the cluster key, and every later frame carries a MAC over
// (direction, sequence, kind, body). That gives authentication and integrity,
// not encryption. Same construction as orderer-rust's auth.rs.

const (
	macLen   = 16
	nonceLen = 16
)

// Digest is a SHA-256 value.
type Digest = [32]byte

func hmacSum(key []byte, parts ...[]byte) Digest {
	m := hmac.New(sha256.New, key)
	for _, p := range parts {
		m.Write(p)
	}
	var d Digest
	m.Sum(d[:0])
	return d
}

// handshake derives a handshake value: HMAC(key, label | parts...).
func handshake(key []byte, label string, parts ...[]byte) Digest {
	return hmacSum(key, append([][]byte{[]byte(label)}, parts...)...)
}

func newNonce() []byte {
	n := make([]byte, nonceLen)
	if _, err := rand.Read(n); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return n
}

func ctEq(a, b []byte) bool { return subtle.ConstantTimeCompare(a, b) == 1 }

// session MACs the frames of one authenticated connection direction pair.
type session struct {
	key              Digest
	sendDir          byte
	sendSeq, recvSeq uint64
}

func newSession(key Digest, sendDir byte) *session { return &session{key: key, sendDir: sendDir} }

func (s *session) mac(dir byte, seq uint64, kind byte, body []byte) []byte {
	var head [10]byte
	head[0] = dir
	le.PutUint64(head[1:], seq)
	head[9] = kind
	d := hmacSum(s.key[:], head[:], body)
	return d[:macLen]
}

func (s *session) seal(kind byte, body []byte) []byte {
	m := s.mac(s.sendDir, s.sendSeq, kind, body)
	s.sendSeq++
	return m
}

// open verifies a frame; a rejection does not advance the sequence.
func (s *session) open(kind byte, body, tag []byte) bool {
	want := s.mac(1-s.sendDir, s.recvSeq, kind, body)
	if !ctEq(want, tag) {
		return false
	}
	s.recvSeq++
	return true
}
