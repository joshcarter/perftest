package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"io"
	"math/rand"
)

type Runner struct {
	*zap.SugaredLogger
	objectStore  ObjectStore
	objectVendor *ObjectVendor
	reporter     *Reporter
	syncer       Syncer
	syncWhen     SyncWhen
	iosize       int64
	errchan      chan error
}

func NewRunner(os ObjectStore, n int) (*Runner, error) {
	r := &Runner{
		SugaredLogger: Logger().With(zap.Int("id", n)),
		objectStore:   os,
		objectVendor:  global.ObjectVendor,
		reporter:      global.Reporter,
		syncer:        global.Syncer,
		syncWhen:      global.SyncWhen,
		iosize:        global.IoSize,
		errchan:       global.RunnerError,
	}

	r.Infof("creating runner")

	return r, nil
}

func (r *Runner) Run(ctx context.Context) {
	r.Infof("running")

	for {
		select {
		case <-ctx.Done():
			return

		default:
			if err := r.Op(ctx); err != nil {
				select {
				case r.errchan <- err:
					// error sent
				default:
					// error chan was full, discard
				}
			}
		}
	}
}

func (r *Runner) Op(ctx context.Context) error {
	if global.ReadPercent == 0 {
		return r.WriteObject(ctx)
	} else if global.ReadPercent == 100 {
		return r.ReadObject(ctx)
	} else {
		if rand.Intn(100) < global.ReadPercent {
			return r.ReadObject(ctx)
		} else {
			return r.WriteObject(ctx)
		}
	}
}

func (r *Runner) WriteObject(ctx context.Context) (e error) {
	blk := r.objectVendor.GetObject()
	defer r.objectVendor.ReturnObject(blk)

	wr, e := r.objectStore.GetWriter(fmt.Sprintf("%s.%s", blk.Id.String(), blk.Extension))

	if e != nil {
		return fmt.Errorf("cannot get block writer: %s", e)
	}

	defer func() {
		if e == nil {
			e = wr.Close()
		} else {
			_ = wr.Close() // attempt to close, but don't nuke existing error
		}
	}()

	offset := 0
	remaining := len(blk.Data)

	// r.Infof("starting block '%s': %d bytes", blk.Id, remaining)

	for remaining > 0 && len(ctx.Done()) == 0 {
		var bw int
		iosize := int(r.iosize)

		if iosize > remaining {
			iosize = remaining
		}

		sample := r.reporter.GetSample()
		bw, e = wr.Write(blk.Data[offset : offset+iosize])
		r.reporter.CaptureSample(sample, bw, Write)

		remaining -= bw
		offset += iosize

		if e == io.EOF || remaining == 0 {
			e = nil
			break
		} else if e != nil {
			r.Errorf("write: %s", e)
			return
		} else if bw < iosize {
			e = fmt.Errorf("short write: expected %d, got %d", iosize, bw)
			return
		}

		if r.syncWhen == SyncOnWrite {
			if e = r.syncer.Sync(wr); e != nil {
				r.Errorf("sync: %s", e)
				return
			}
		}
	}

	if r.syncWhen == SyncOnClose {
		e = r.syncer.Sync(wr)
	}

	// r.Infof("wrote block '%s'", blk.Id)

	return
}

func (r *Runner) ReadObject(ctx context.Context) (e error) {
	name, e := r.objectStore.RandomExistingObjectName()

	if e != nil {
		return e
	}

	rr, e := r.objectStore.GetReader(name)

	if e != nil {
		return fmt.Errorf("cannot get block reader: %s", e)
	}

	defer func() {
		if e == nil {
			e = rr.Close()
		} else {
			_ = rr.Close() // attempt to close, but don't nuke existing error
		}
	}()

	buf := make([]byte, int(r.iosize))

	for len(ctx.Done()) == 0 {
		var br int

		sample := r.reporter.GetSample()
		br, e = rr.Read(buf)
		r.reporter.CaptureSample(sample, br, Read)

		if e == io.EOF {
			e = nil
			break
		}
		if e != nil {
			r.Errorf("read: %s", e)
			return
		}
	}

	return nil
}
