package main

import (
	"fmt"
	"os"
	"path/filepath"
)

type BlockWriter interface {
	Write(p []byte) (n int, err error)
	Close() error
	Sync() error
}

type BlockStore interface {
	GetWriter(name string) (BlockWriter, error)
}

type FileBlockStore struct {
	root string
}

func NewFileBlockStore(root string) (bs BlockStore, e error) {
	if e = os.MkdirAll(root, 0750); e != nil {
		e = fmt.Errorf("cannot init block store: %s", e)
		return
	}

	bs = &FileBlockStore{root}
	return
}

func (f *FileBlockStore) GetWriter(name string) (bw BlockWriter, e error) {
	bw, e = os.Create(filepath.Join(f.root, name))
	return
}
