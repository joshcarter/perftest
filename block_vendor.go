package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/spectralogic/go-core/log"
	"github.com/spectralogic/go-core/sequence"
	"github.com/spectralogic/go-core/ulid"
)

type Block struct {
	Id        *ulid.ULID
	Extension string // file extension
	dataBuf   []byte // full-size buffer
	Data      []byte // slice of dataBuf to use (may be smaller)
}

type BlockConfig struct {
	Compressibility int
	Sizes           []int
	MaxSize         int
	Extensions      map[int]string // map size -> file extension for size
}

type BlockVendor struct {
	log.Logger
	config    *BlockConfig
	seq       *sequence.ByteSequence
	blockPool sync.Pool
}

// Size spec follows fio 'bsplit' format:
// "blocksize/percentage:blocksize/percentage:..." For example
// "4K/10:8K/90" means 4K blocks 10 percent of the time and 8K blocks
// 90 percent of the time. The percentages must sum to 100.
//
// Compressibility should be 0 for incompressible, 100 for totally
// compressible data, or any percentage between.
func NewBlockVendor(bssplit string, compressibility int) (*BlockVendor, error) {
	config, err := parseBlockSizeSplit(bssplit)

	if err != nil {
		return nil, err
	}

	config.Compressibility = compressibility

	b := &BlockVendor{
		Logger: log.GetLogger("blockvendor"),
		config: config,
		seq:    sequence.NewByteSequence(int64(0)),
		blockPool: sync.Pool{
			New: func() interface{} {
				return &Block{
					Id:      nil, // ID gets assigned later
					dataBuf: make([]byte, config.MaxSize),
				}
			},
		},
	}

	b.Infof("block size spec: %s", bssplit)
	b.Infof("compressibility: %d", compressibility)

	return b, nil
}

func (b *BlockVendor) GetBlock() *Block {
	blk := b.blockPool.Get().(*Block)

	blk.Id = ulid.New()

	// slice block down to size
	size := b.config.Sizes[rand.Int31n(100)]
	blk.Data = blk.dataBuf[:size]
	blk.Extension = b.config.Extensions[size]

	b.seq.PatternFill(blk.Data, b.config.Compressibility)
	return blk
}

func (b *BlockVendor) ReturnBlock(blk *Block) {
	b.blockPool.Put(blk)
}

func parseBlockSizeSplit(bssplit string) (*BlockConfig, error) {
	config := &BlockConfig{
		Sizes:      make([]int, 100),
		Extensions: make(map[int]string),
		MaxSize:    0,
	}

	totalPercent := 0
	splits := strings.Split(bssplit, ":")

	if len(splits) == 0 {
		return nil, fmt.Errorf("size spec needs at least one size; try 'blocksize/100/dat'")
	}

	for _, s := range splits {
		strs := strings.Split(s, "/")
		if len(strs) != 3 {
			return nil, fmt.Errorf("malformed split '%s'; should be blocksize/percent/extension", s)
		}

		sizeStr := strs[0]
		percentStr := strs[1]
		extension := strs[2]

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
