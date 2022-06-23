package main

import (
	"fmt"
	"os"
	"path/filepath"
)

type ObjectWriter interface {
	Write(p []byte) (n int, err error)
	Close() error
	Sync() error
}

type ObjectStore interface {
	GetWriter(name string) (ObjectWriter, error)
}

type FileObjectStore struct {
	root string
}

func NewFileObjectStore(root string) (bs ObjectStore, e error) {
	if e = os.MkdirAll(root, 0750); e != nil {
		e = fmt.Errorf("cannot init file store: %s", e)
		return
	}

	bs = &FileObjectStore{root}
	return
}

func (f *FileObjectStore) GetWriter(name string) (bw ObjectWriter, e error) {
	bw, e = os.Create(filepath.Join(f.root, name))
	return
}
