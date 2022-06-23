package main

import (
	"fmt"
	th "github.com/spectralogic/go-core/test_helpers"
	"testing"
	"time"
)

func TestHistogram_Add(t *testing.T) {
	h := NewHistogram()
	for _, sample := range []string{
		"0.1ms",
		"0.2ms",
		"1ms",
		"1.5ms",
		"55ms"} {
		d, e := time.ParseDuration(sample)
		th.AbortOnError(t, e)
		h.Add(d)
	}

	fmt.Println(h.Headers())
	fmt.Println(h.String())
}
