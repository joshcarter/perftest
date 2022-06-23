package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Histogram for performance samples, which should be a bit faster for cuncurrent
// use than the one that's in stat.Histogram. This puts samples into buckets:
//   - under 1ms
//   - 1 to 5ms
//   - 5 to 10ms
//   - 10 to 20ms
//   - 20 to 50ms
//   - 50 to 100ms
//   - 100 to 250ms
//   - 250 to 1000ms
//   - 1000 to 2000ms
//   - over 2000ms
type Histogram struct {
	data [10]int64
}

func NewHistogram() *Histogram {
	return &Histogram{}
}

func (h *Histogram) Add(sample time.Duration) {
	usec := int64(sample.Nanoseconds() / 1000)

	switch {
	case usec < 1_000:
		atomic.AddInt64(&h.data[0], 1)
	case usec < 5_000:
		atomic.AddInt64(&h.data[1], 1)
	case usec < 10_000:
		atomic.AddInt64(&h.data[2], 1)
	case usec < 20_000:
		atomic.AddInt64(&h.data[3], 1)
	case usec < 50_000:
		atomic.AddInt64(&h.data[4], 1)
	case usec < 100_000:
		atomic.AddInt64(&h.data[5], 1)
	case usec < 250_000:
		atomic.AddInt64(&h.data[6], 1)
	case usec < 1_000_000:
		atomic.AddInt64(&h.data[7], 1)
	case usec < 2_000_000:
		atomic.AddInt64(&h.data[8], 1)
	default:
		atomic.AddInt64(&h.data[9], 1)
	}
}

func (h *Histogram) Reset() {
	for i := 0; i < 10; i++ {
		atomic.StoreInt64(&h.data[i], 0)
	}
}

func (h *Histogram) String() string {
	// These format widths are chosen to line up with the headers below
	return fmt.Sprintf("%5d,%5d,%6d,%6d,%6d,%6d,%6d,%5d,%5d,%6d",
		h.data[0], h.data[1], h.data[2], h.data[3], h.data[4], h.data[5], h.data[6], h.data[7], h.data[8], h.data[9])
}

func (h *Histogram) Headers() string {
	return "< 1ms,  5ms,  10ms,  20ms,  50ms, 100ms, 250ms, 1sec, 2sec, >2sec"
}
