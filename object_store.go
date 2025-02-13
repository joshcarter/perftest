package main

import (
	"fmt"
	"github.com/iceber/iouring-go"
	"os"
	"path/filepath"
)

type ObjectStore interface {
	GetFile(name string) (*os.File, error)
	Close() error
	Ring() *iouring.IOURing
}

type FileObjectStore struct {
	root      string
	openFlags int
	ring      *iouring.IOURing
}

func NewFileObjectStore(root string, openFlags int) (bs ObjectStore, e error) {
	path := filepath.Join(root, global.RunId)

	if e = os.MkdirAll(path, 0750); e != nil {
		e = fmt.Errorf("cannot init file store: %s", e)
		return
	}

	ring, e := iouring.New(uint(256))
	if e != nil {
		e = fmt.Errorf("cannot initialize io_uring: %w", e)
		return
	}

	bs = &FileObjectStore{path, openFlags, ring}
	return
}

func (f *FileObjectStore) GetFile(name string) (file *os.File, e error) {
	return os.OpenFile(filepath.Join(f.root, name), os.O_WRONLY|os.O_CREATE|f.openFlags, 0775)
}

func (f *FileObjectStore) Close() error {
	return f.ring.Close()
}

func (f *FileObjectStore) Ring() *iouring.IOURing {
	return f.ring
}
