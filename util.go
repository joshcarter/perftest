package main

import (
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// this adapted from spf13/viper:
// parseSizeInBytes converts strings like 1GB or 12 mb into an unsigned integer number of bytes
func parseSizeInBytes(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	lastChar := len(sizeStr) - 1
	multiplier := int64(1)

	if lastChar > 0 {
		if sizeStr[lastChar] == 'b' || sizeStr[lastChar] == 'B' {
			if lastChar > 1 {
				switch unicode.ToLower(rune(sizeStr[lastChar-1])) {
				case 'k':
					multiplier = 1 << 10
					sizeStr = strings.TrimSpace(sizeStr[:lastChar-1])
				case 'm':
					multiplier = 1 << 20
					sizeStr = strings.TrimSpace(sizeStr[:lastChar-1])
				case 'g':
					multiplier = 1 << 30
					sizeStr = strings.TrimSpace(sizeStr[:lastChar-1])
				default:
					multiplier = 1
					sizeStr = strings.TrimSpace(sizeStr[:lastChar])
				}
			}
		}
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)

	if err != nil {
		return 0, fmt.Errorf("cannot parse '%s' as int64", sizeStr)
	}

	if size < 0 {
		size = 0
	}

	return size * multiplier, nil
}

var EicSuffixes = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"}
var DecimalSuffixes = []string{"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"}

// Scales a size to kilobytes, megabytes, etc.. Will flip to the
// next-larger size when 1,000 is passed, however the size will be
// printed as a fraction of 1024, not 1000. Thus 1023 will be rendered
// as "1.0 KiB" rather than "1023 B".
func SprintSize(size int64) string {
	sizef := float64(size)
	i := 0

	for sizef > 999 && i < len(EicSuffixes) {
		sizef /= 1024
		i++
	}

	if i == 0 {
		// No fractional part needed
		return fmt.Sprintf("%d B", int(size))
	} else {
		return fmt.Sprintf("%0.1f %s", sizef, EicSuffixes[i])
	}
}

// Scales a size to kilobytes, megabytes, etc.. Uses base-1000 instead
// of base-1024.
func SprintDecimalSize(size int64) string {
	sizef := float64(size)
	i := 0

	for sizef > 999 && i < len(EicSuffixes) {
		sizef /= 1000
		i++
	}

	if i == 0 {
		// No fractional part needed
		return fmt.Sprintf("%d B", int(size))
	} else {
		return fmt.Sprintf("%0.1f %s", sizef, DecimalSuffixes[i])
	}
}

func Median(data []int64) int64 {
	l := len(data)
	if l == 0 {
		return 0
	}

	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })

	if l%2 == 0 {
		return (data[l/2-1] + data[l/2]) / 2
	} else {
		return data[l/2]
	}
}

func Mean(data []int64) int64 {
	l := len(data)
	if l == 0 {
		return 0
	}

	var sum int64

	for _, d := range data {
		sum += d
	}

	return sum / int64(l)
}

func RunCmd(command string) (out []byte, e error) {
	c := strings.Split(command, " ")
	out, e = exec.Command(c[0], c[1:]...).Output()
	if e != nil {
		return nil, fmt.Errorf("running '%s': %s", command, e)
	}

	return
}
