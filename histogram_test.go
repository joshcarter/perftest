package main

import (
	"fmt"
	"testing"
	"time"

	th "github.com/spectralogic/go-core/test_helpers"
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

	fmt.Println("add test")
	fmt.Println(h.Headers())
	fmt.Println(h.String())
}

func TestHistogram_Timing(t *testing.T) {
	h := NewHistogram()
	d, e := time.ParseDuration("15ms")
	th.AbortOnError(t, e)

	for i := 0; i < 100; i++ {
		start := time.Now()
		time.Sleep(d)
		h.Add(time.Now().Sub(start))
	}

	fmt.Println("timing test")
	fmt.Println(h.Headers())
	fmt.Println(h.String())
}
