package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"
)

type Object struct {
	Id        ulid.ULID
	Extension string // file extension
	dataBuf   []byte // full-size buffer
	Data      []byte // slice of dataBuf to use (may be smaller)
}

type ObjectVendorConfig struct {
	Compressibility int
	Sizes           []int
	MaxSize         int
	Extensions      map[int]string // map size -> file extension for size
}

type ObjectVendor struct {
	*zap.SugaredLogger
	config *ObjectVendorConfig
	pool   sync.Pool
	seq    *ByteSequence
}

// Size spec follows fio 'bsplit' format:
// "blocksize/percentage:blocksize/percentage:..." For example
// "4K/10:8K/90" means 4K blocks 10 percent of the time and 8K blocks
// 90 percent of the time. The percentages must sum to 100.
//
// Compressibility should be 0 for incompressible, 100 for totally
// compressible data, or any percentage between.
func NewObjectVendor(sizespec string, compressibility int) (*ObjectVendor, error) {
	config, err := parseSizeSpec(sizespec)

	if err != nil {
		return nil, err
	}

	config.Compressibility = compressibility

	b := &ObjectVendor{
		SugaredLogger: Logger(),
		config:        config,
		pool: sync.Pool{
			New: func() interface{} {
				return &Object{
					Id:      ulid.Make(),
					dataBuf: make([]byte, config.MaxSize),
				}
			},
		},
		seq: NewByteSequence(0),
	}

	b.Infof("object size spec: %s", sizespec)
	b.Infof("compressibility: %d", compressibility)

	return b, nil
}

func (b *ObjectVendor) GetObject() *Object {
	blk := b.pool.Get().(*Object)
	blk.Id = ulid.Make() // Need to assign new one every time to prevent recycling

	// slice block down to size
	size := b.config.Sizes[rand.Int31n(100)]
	blk.Data = blk.dataBuf[:size]
	blk.Extension = b.config.Extensions[size]

	b.seq.PatternFill(blk.Data, b.config.Compressibility)
	return blk
}

func (b *ObjectVendor) ReturnObject(blk *Object) {
	b.pool.Put(blk)
}

func parseSizeSpec(sizespec string) (*ObjectVendorConfig, error) {
	config := &ObjectVendorConfig{
		Sizes:      make([]int, 100),
		Extensions: make(map[int]string),
		MaxSize:    0,
	}

	totalPercent := 0
	splits := strings.Split(sizespec, ":")

	if len(splits) == 0 {
		return nil, fmt.Errorf("size spec needs at least one size; try 'blocksize/100/dat'")
	}

	for _, s := range splits {
		sizeStr := ""
		percentStr := "100"
		extension := "dat"

		strs := strings.Split(s, "/")
		switch {
		case len(strs) == 1:
			sizeStr = strs[0]
		case len(strs) == 2:
			sizeStr = strs[0]
			percentStr = strs[1]
		case len(strs) == 3:
			sizeStr = strs[0]
			percentStr = strs[1]
			extension = strs[2]
		default:
			return nil, fmt.Errorf("malformed split '%s'; should be blocksize/percent/extension", s)
		}

		size, err := parseSizeInBytes(sizeStr)

		if err != nil {
			return nil, fmt.Errorf("cannot parse block size spec: %s", err)
		} else if size <= 0 {
			return nil, fmt.Errorf("block size '%s' must be above 0", sizeStr)
		}

		percent, err := strconv.ParseInt(percentStr, 10, 64)

		if err != nil {
			return nil, fmt.Errorf("cannot parse '%s' as int64", percentStr)
		} else if totalPercent+int(percent) > 100 {
			return nil, fmt.Errorf("percents must sum to 100")
		}

		for i := totalPercent; i < totalPercent+int(percent); i++ {
			config.Sizes[i] = int(size)
		}

		if int(size) > config.MaxSize {
			config.MaxSize = int(size)
		}

		config.Extensions[int(size)] = extension

		totalPercent += int(percent)
	}

	if totalPercent != 100 {
		return nil, fmt.Errorf("percents must sum to 100")
	}

	return config, nil
}
