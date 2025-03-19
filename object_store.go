package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

type ObjectWriter interface {
	Write(p []byte) (n int, err error)
	Close() error
	Sync() error
}

type ObjectReader interface {
	Read(p []byte) (n int, err error)
	Close() error
}

type ObjectStore interface {
	GetWriter(name string) (ObjectWriter, error)
	GetReader(name string) (ObjectReader, error)
}

type FileObjectStore struct {
	root      string
	openFlags int
	subdirs   []string
}

func NewFileObjectStore(root string, openFlags int) (bs ObjectStore, e error) {
	if e = os.MkdirAll(root, 0755); e != nil {
		e = fmt.Errorf("cannot init file store: %s", e)
		return
	}

	var subdirs []string

	if global.Subdirs > 0 {
		for i := 0; i < global.Subdirs; i++ {
			subdir := fmt.Sprintf("dir-%d", i)
			if e = os.MkdirAll(filepath.Join(root, subdir), 0755); e != nil {
				e = fmt.Errorf("cannot create file store subdirectory: %s", e)
				return
			}
			subdirs = append(subdirs, subdir)
		}
	}

	bs = &FileObjectStore{
		root,
		openFlags,
		subdirs,
	}
	return
}

func (f *FileObjectStore) GetWriter(name string) (bw ObjectWriter, e error) {
	var path string

	if len(f.subdirs) == 0 {
		path = filepath.Join(f.root, name)
	} else {
		path = filepath.Join(f.root, f.subdirs[rand.Intn(len(f.subdirs))], name)
	}

	bw, e = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|f.openFlags, 0775)
	return
}

func (f *FileObjectStore) GetReader(name string) (br ObjectReader, e error) {
	br, e = os.OpenFile(filepath.Join(f.root, name), os.O_RDONLY|f.openFlags, 0)
	return
}
