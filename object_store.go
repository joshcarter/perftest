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
	root      string
	openFlags int
}

func NewFileObjectStore(root string, openFlags int) (bs ObjectStore, e error) {
	if e = os.MkdirAll(root, 0750); e != nil {
		e = fmt.Errorf("cannot init file store: %s", e)
		return
	}

	bs = &FileObjectStore{root, openFlags}
	return
}

func (f *FileObjectStore) GetWriter(name string) (bw ObjectWriter, e error) {
	bw, e = os.OpenFile(filepath.Join(f.root, name), os.O_WRONLY|os.O_CREATE|f.openFlags, 0775)
	return
}
