package main

import (
	"fmt"
	"runtime/debug"
	"testing"
	"time"
)

// AbortOnError Aborts if err is not nil
func AbortOnError(t *testing.T, err error) {
	t.Helper()
	AbortOnErrorf(t, err, "")
}

// AbortOnErrorf causes the testing framework to abort if the given error is not nil.
func AbortOnErrorf(t *testing.T, e error, msg string, args ...interface{}) {
	t.Helper()
	if e != nil {
		if testing.Verbose() {
			debug.PrintStack()
		}
		t.Fatalf("%s%v", argsMsg(msg, args...), e)
	}
}

func argsMsg(msg string, args ...interface{}) string {
	if len(args) < 1 {
		if msg == "" {
			return ""
		}
		return fmt.Sprintf("%s: ", msg)
	}
	return fmt.Sprintf("%s: ", fmt.Sprintf(msg, args...))
}

func TestHistogram_Add(t *testing.T) {
	h := NewHistogram()
	for _, sample := range []string{
		"0.1ms",
		"0.2ms",
		"1ms",
		"1.5ms",
		"55ms"} {
		d, e := time.ParseDuration(sample)
		AbortOnError(t, e)
		h.Add(d)
	}

	fmt.Println("add test")
	fmt.Println(h.Headers())
	fmt.Println(h.String())
}

func TestHistogram_Timing(t *testing.T) {
	h := NewHistogram()
	d, e := time.ParseDuration("15ms")
	AbortOnError(t, e)

	for i := 0; i < 100; i++ {
		start := time.Now()
		time.Sleep(d)
		h.Add(time.Now().Sub(start))
	}

	fmt.Println("timing test")
	fmt.Println(h.Headers())
	fmt.Println(h.String())
}
