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
	GetFileFixMe(name string) (*os.File, error)
}

type FileObjectStore struct {
	root      string
	openFlags int
}

func NewFileObjectStore(root string, openFlags int) (bs ObjectStore, e error) {
	path := filepath.Join(root, global.RunId)

	if e = os.MkdirAll(path, 0750); e != nil {
		e = fmt.Errorf("cannot init file store: %s", e)
		return
	}

	bs = &FileObjectStore{path, openFlags}
	return
}

func (f *FileObjectStore) GetWriter(name string) (bw ObjectWriter, e error) {
	bw, e = os.OpenFile(filepath.Join(f.root, name), os.O_WRONLY|os.O_CREATE|f.openFlags, 0775)
	return
}

func (f *FileObjectStore) GetFileFixMe(name string) (file *os.File, e error) {
	return os.OpenFile(filepath.Join(f.root, name), os.O_WRONLY|os.O_CREATE|f.openFlags, 0775)
}
