package orderer

import "runtime"

type runtimeMem struct{ mallocs uint64 }

func (m *runtimeMem) read() {
	var s runtime.MemStats
	runtime.ReadMemStats(&s)
	m.mallocs = s.Mallocs
}
