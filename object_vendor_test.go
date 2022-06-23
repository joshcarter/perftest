package main

import (
	"fmt"
	"testing"

	th "github.com/spectralogic/go-core/test_helpers"
)

func TestParseBlockSizes_InvalidInput(t *testing.T) {
	var err error

	_, err = parseSizeSpec("")
	th.ErrorOnNoError(t, err, "malformed bssplit")

	_, err = parseSizeSpec("///")
	th.ErrorOnNoError(t, err, "malformed bssplit")

	_, err = parseSizeSpec(":")
	th.ErrorOnNoError(t, err, "malformed bssplit")

	_, err = parseSizeSpec(":/")
	th.ErrorOnNoError(t, err, "malformed bssplit")

	_, err = parseSizeSpec("4k")
	th.ErrorOnNoError(t, err, "no percentage")

	_, err = parseSizeSpec("4k/99")
	th.ErrorOnNoError(t, err, "percent != 100")

	_, err = parseSizeSpec("4k/10:8k/89")
	th.ErrorOnNoError(t, err, "percent != 100")

	_, err = parseSizeSpec("4k/foo")
	th.ErrorOnNoError(t, err, "invalid percent")

	_, err = parseSizeSpec("4f/100")
	th.ErrorOnNoError(t, err, "invalid size")

	_, err = parseSizeSpec("foo/100")
	th.ErrorOnNoError(t, err, "invalid size")
}

func TestParseBlockSizes_ValidInput(t *testing.T) {
	config, err := parseSizeSpec("4KB/10/foo:8KB/20/bar:16KB/70/baz")
	// sizes, maxSize, err := parseBlockSizeSplit("4KB/100")
	th.ErrorOnError(t, err, "parse split")
	th.AssertEqual(t, 16*1024, config.MaxSize)

	fmt.Println(config)

}
