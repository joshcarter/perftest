package main

import (
	"fmt"
	"github.com/oklog/ulid/v2"
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
	RandomExistingObjectName() (string, error)
}

type FileObjectStore struct {
	root      string
	openFlags int
	subdirs   []string
	objects   []string
}

func NewFileObjectStore(root string, openFlags int) (ObjectStore, error) {
	var e error

	if e = os.MkdirAll(root, 0755); e != nil {
		e = fmt.Errorf("cannot init file store: %s", e)
		return nil, e
	}

	var subdirs []string

	if global.Subdirs > 0 {
		for i := 0; i < global.Subdirs; i++ {
			subdir := fmt.Sprintf("dir-%d", i)
			if e = os.MkdirAll(filepath.Join(root, subdir), 0755); e != nil {
				e = fmt.Errorf("cannot create file store subdirectory: %s", e)
				return nil, e
			}
			subdirs = append(subdirs, subdir)
		}
	}

	f := &FileObjectStore{
		root,
		openFlags,
		subdirs,
		make([]string, 0),
	}

	if global.ReadPercent > 0 {
		f.ScanExistingObjects()
	}

	return f, nil
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

func (f *FileObjectStore) RandomExistingObjectName() (name string, e error) {
	if len(f.objects) == 0 {
		e = fmt.Errorf("no objects available")
		return
	}

	name = f.objects[rand.Intn(len(f.objects))]
	return
}

func (f *FileObjectStore) ScanExistingObjects() {
	err := filepath.Walk(f.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && info.Size() > 0 {
			if isValidULIDWithExtension(filepath.Base(path)) {
				relPath, err := filepath.Rel(f.root, path)
				if err != nil {
					panic(err)
				}
				f.objects = append(f.objects, relPath)
			}
		}

		return nil
	})

	if err != nil {
		fmt.Printf("error scanning existing objects: %s\n", err)
	}
}

func isValidULIDWithExtension(name string) bool {
	ext := filepath.Ext(name)
	base := name[:len(name)-len(ext)]
	_, err := ulid.Parse(base)
	return err == nil
}
