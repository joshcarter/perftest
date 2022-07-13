package main

import (
	"bytes"
	"context"
	"fmt"
	"go.uber.org/zap"
	"io"
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
			err := r.WriteObject(ctx)

			if err != nil {
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

	rd := bytes.NewReader(blk.Data)
	remaining := len(blk.Data)

	// r.Infof("starting block '%s': %d bytes", blk.Id, remaining)

	for remaining > 0 && len(ctx.Done()) == 0 {
		var bw int64
		iosize := r.iosize

		if iosize > int64(remaining) {
			iosize = int64(remaining)
		}

		// r.Infof("writing '%s': %d bytes, %d remaining", blk.Id, iosize, remaining)

		sample := r.reporter.GetSample()
		bw, e = io.CopyN(wr, rd, iosize)
		r.reporter.CaptureSample(sample, bw)

		remaining -= int(bw)

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
