package main

import (
	"runtime/debug"
	"testing"
)

func TestMedian(t *testing.T) {
	ExpectEqual(t, int64(0), Median([]int64{}))
	ExpectEqual(t, int64(5), Median([]int64{5}))
	ExpectEqual(t, int64(15), Median([]int64{10, 20}))
	ExpectEqual(t, int64(20), Median([]int64{10, 20, 30}))
	ExpectEqual(t, int64(25), Median([]int64{10, 20, 30, 40}))
	ExpectEqual(t, int64(30), Median([]int64{10, 20, 30, 40, 50}))
}

func ExpectEqual(t *testing.T, expected, actual interface{}) {
	t.Helper()
	if expected != actual {
		if testing.Verbose() {
			debug.PrintStack()
		}

		t.Errorf("Expected %v (%T), got %v (%T)", expected, expected, actual, actual)
	}
}
