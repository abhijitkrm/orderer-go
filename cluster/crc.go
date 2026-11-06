// Package cluster is the replica runtime around the protocol core in
// ../consensus: write-ahead log, snapshot store, TCP transport, client
// protocol and the state machines it drives. It is the Go port of
// orderer-rust's orderer-cluster and orderer-cpp's include/orderer/cluster
// (spec/REPLICATION.md 13), byte-compatible with both: WAL and snapshot files,
// wire frames, client protocol and state digests are interchangeable, so a
// cluster can mix Rust, C++ and Go replicas.
package cluster

import "hash/crc32"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// crc32c is the Castagnoli CRC (hardware-accelerated by the standard library).
func crc32c(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// crc32cUpdate extends a finished CRC with more bytes (start from 0).
func crc32cUpdate(c uint32, b []byte) uint32 { return crc32.Update(c, castagnoli, b) }
