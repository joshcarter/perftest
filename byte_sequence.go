package main

// #include "sequences.h"
import "C"

import (
	"fmt"
	"io"
	"os"
	"unsafe"
)

// ByteSequence is a generator of not-crypto-strong random bytes.
type ByteSequence struct {
	size   int64
	offset int64
	next   uint64
}

// NewByteSequence creates a new generator of given size.
func NewByteSequence(size int64) *ByteSequence {
	return &ByteSequence{
		size:   size,
		offset: 0,
		next:   0x490c734ad1ccf6e9, // Initialize with a couple rounds of the generator
	}
}

// Read fills the buffer until the sequence's size is exhausted, after which
// it returns io.EOF. Useful with io.Copy and other code that wants to treat
// the sequence like an io.Reader.
func (seq *ByteSequence) Read(buf []byte) (int, error) {
	if seq.offset >= seq.size {
		return 0, io.EOF
	}

	remaining := seq.size - seq.offset

	var readSize int64
	buflen := int64(len(buf))
	if buflen < remaining {
		readSize = buflen
	} else {
		readSize = remaining
	}

	temp := C.fillbytes(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(readSize),
		C.uint64_t(seq.next))
	seq.next = uint64(temp)
	seq.offset += readSize

	return int(readSize), nil
}

// Fill fills the buffer without paying attention to the sequence size.
func (seq *ByteSequence) Fill(buf []byte) {
	if len(buf) == 0 {
		return
	}

	temp := C.fillbytes(
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
		C.uint64_t(seq.next))
	seq.next = uint64(temp)
}

var patternBlock []byte

// PatternFill fills buffer with a certain amount of compressibility,
// ranging from 0 (not compressible) to 100 (completely compressible).
func (seq *ByteSequence) PatternFill(buf []byte, compressibility int) {
	if len(buf) == 0 {
		return
	}

	if compressibility == 0 {
		seq.Fill(buf)
		return
	}

	blkSize := 65536

	if patternBlock == nil {
		patternBlock = make([]byte, blkSize)
		for i := range patternBlock {
			patternBlock[i] = byte('A')
		}
	}

	blocks := len(buf) / blkSize
	leftover := len(buf) % blkSize
	patternBlocks := int(float32(blocks) * float32(compressibility) / float32(100))
	randomBlocks := blocks - patternBlocks
	rand := NewNumberSequence()
	rand.Set(int64(seq.next))

	// Do all the full blocks we've got
	for i := 0; i < blocks; i++ {
		if randomBlocks == 0 {
			// Only pattern blocks remaining
			copy(buf[i*blkSize:], patternBlock)
			patternBlocks--
		} else if patternBlocks == 0 {
			// Only random blocks remaining
			temp := C.fillbytes(
				(*C.uint8_t)(unsafe.Pointer(&buf[i*blkSize])),
				C.size_t(blkSize),
				C.uint64_t(seq.next))
			seq.next = uint64(temp)
			randomBlocks--
		} else if rand.Next() > 0 {
			copy(buf[i*blkSize:], patternBlock)
			patternBlocks--
		} else {
			temp := C.fillbytes(
				(*C.uint8_t)(unsafe.Pointer(&buf[i*blkSize])),
				C.size_t(blkSize),
				C.uint64_t(seq.next))
			seq.next = uint64(temp)
			randomBlocks--
		}
	}

	// Fill in leftover
	if leftover == 0 {
		// Done
	} else if rand.Next() > 0 {
		copy(buf[blocks*blkSize:], patternBlock[:leftover])
	} else {
		temp := C.fillbytes(
			(*C.uint8_t)(unsafe.Pointer(&buf[blocks*blkSize])),
			C.size_t(leftover),
			C.uint64_t(seq.next))
		seq.next = uint64(temp)
	}
}

// Seed sets the sequence's seed to a given value.
func (seq *ByteSequence) Seed(seed uint64) {
	seq.next = seed
}

// Seek lets you rewind (or otherwise change position) when using this
// sequence as a stream. Note, reading from a sequence, seeking to
// zero, and rewinding will result in different data being read. Seek
// only changes the position within the stream, not the random seed.
func (seq *ByteSequence) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case os.SEEK_SET:
		newOffset = offset
	case os.SEEK_CUR:
		newOffset = seq.offset + offset
	case os.SEEK_END:
		newOffset = seq.size + offset
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("Cannot seek to negative offset %d", newOffset)
	}

	if newOffset > seq.size {
		return 0, fmt.Errorf("Cannot seek past end of sequence to offset %d (size %d)", newOffset, seq.size)
	}

	seq.offset = newOffset
	return seq.offset, nil
}

// Write just drops everything on the floor. Provided for compatibility
// with io.ReadWriter.
func (seq *ByteSequence) Write(buf []byte) (int, error) {
	return len(buf), nil
}

// Close sets any remaining sequence size to zero. Provided for compatibility
// with io.Closer.
func (seq *ByteSequence) Close() error {
	seq.size = 0
	return nil
}
