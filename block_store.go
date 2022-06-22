package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type BlockStore interface {
	GetWriter(name string) (io.WriteCloser, error)
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

func (f *FileBlockStore) GetWriter(name string) (file io.WriteCloser, e error) {
	file, e = os.Create(filepath.Join(f.root, name))
	return
}
