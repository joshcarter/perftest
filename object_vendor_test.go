package main

import (
	"fmt"
	"runtime/debug"
	"testing"
)

func TestParseBlockSizes_InvalidInput(t *testing.T) {
	var err error

	_, err = parseSizeSpec("")
	ExpectErrorf(t, err, "malformed bssplit")

	_, err = parseSizeSpec("///")
	ExpectErrorf(t, err, "malformed bssplit")

	_, err = parseSizeSpec(":")
	ExpectErrorf(t, err, "malformed bssplit")

	_, err = parseSizeSpec(":/")
	ExpectErrorf(t, err, "malformed bssplit")

	_, err = parseSizeSpec("4k")
	ExpectErrorf(t, err, "no percentage")

	_, err = parseSizeSpec("4k/99")
	ExpectErrorf(t, err, "percent != 100")

	_, err = parseSizeSpec("4k/10:8k/89")
	ExpectErrorf(t, err, "percent != 100")

	_, err = parseSizeSpec("4k/foo")
	ExpectErrorf(t, err, "invalid percent")

	_, err = parseSizeSpec("4f/100")
	ExpectErrorf(t, err, "invalid size")

	_, err = parseSizeSpec("foo/100")
	ExpectErrorf(t, err, "invalid size")
}

func TestParseBlockSizes_ValidInput(t *testing.T) {
	config, err := parseSizeSpec("4KB/10/foo:8KB/20/bar:16KB/70/baz")
	// sizes, maxSize, err := parseBlockSizeSplit("4KB/100")
	AbortOnErrorf(t, err, "parse split")

	if config.MaxSize != 16*1024 {
		t.Fail()
	}

	fmt.Println(config)

}

// ExpectErrorf causes the testing framework to error if the given
// error is nil (i.e. we expected an error but did not get one).
func ExpectErrorf(t *testing.T, e error, msg string, args ...interface{}) {
	t.Helper()
	if e == nil {
		if testing.Verbose() {
			debug.PrintStack()
		}
		t.Errorf("%sError expected", argsMsg(msg, args...))
	}
}

// ExpectError causes the testing framework to error if the given error is nil.
func ExpectError(t *testing.T, e error) {
	t.Helper()
	ExpectErrorf(t, e, "")
}
