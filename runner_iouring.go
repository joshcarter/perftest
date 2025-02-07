package main

import (
	"context"
	"fmt"
	"github.com/iceber/iouring-go"
	"go.uber.org/zap"
	"time"
)

type RunnerIoUring struct {
	*zap.SugaredLogger
	objectStore  ObjectStore
	objectVendor *ObjectVendor
	reporter     *Reporter
	syncer       Syncer
	syncWhen     SyncWhen
	iosize       int64
	errchan      chan error
}

func NewRunnerIoUring(os ObjectStore, n int) (Runner, error) {
	r := &RunnerIoUring{
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

func (r *RunnerIoUring) Run(ctx context.Context) {
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

func (r *RunnerIoUring) WriteObject(ctx context.Context) (e error) {
	blk := r.objectVendor.GetObject()
	defer r.objectVendor.ReturnObject(blk)

	// Open the file for writing
	file, e := r.objectStore.GetFileFixMe(fmt.Sprintf("%s.%s", blk.Id.String(), blk.Extension))
	if e != nil {
		return fmt.Errorf("cannot open file: %w", e)
	}

	defer func() {
		if e == nil {
			e = file.Close()
		} else {
			_ = file.Close() // attempt to close, but don't nuke existing error
		}
	}()

	// Initialize io_uring with a queue depth of 128
	ringEntries := 128
	ring, e := iouring.New(uint(ringEntries))
	if e != nil {
		return fmt.Errorf("failed to initialize io_uring: %w", e)
	}

	defer func() {
		if e == nil {
			e = ring.Close()
		} else {
			_ = ring.Close() // attempt to close, but don't nuke existing error
		}
	}()

	offset := 0
	remaining := len(blk.Data)
	iosize := int(r.iosize)
	sample := r.reporter.GetSample()
	inFlight := 0 // Tracks the number of outstanding writes
	results := make(chan iouring.Result, ringEntries)

	for remaining > 0 || inFlight > 0 {
		// While we have data to write and room in the queue, submit write requests
		for remaining > 0 && inFlight < ringEntries { // Limit the number of in-flight operations
			if iosize > remaining {
				iosize = remaining
			}

			data := blk.Data[offset : offset+iosize]

			_, e = ring.Pwrite(file, data, uint64(offset), results)
			if e != nil {
				r.Errorf("write submit: %s", e)
				return
			}

			offset += iosize
			remaining -= iosize
			inFlight++
		}

		// Process any complete
		select {
		case res := <-results:
			inFlight--
			e = res.Err()
			if e != nil {
				r.Errorf("write: %s", e)
				return
			}
		default:
			// this is probably not necessary:
			if remaining == 0 || inFlight == ringEntries {
				time.Sleep(time.Microsecond * 100)
			}
		}
	}

	// FIXME: capture samples IO by IO, not entire object
	r.reporter.CaptureSample(sample, int64(len(blk.Data)))

	// Sync the file if required
	if r.syncWhen == SyncOnClose {
		if err := r.syncer.Sync(file); err != nil {
			return fmt.Errorf("sync error: %w", err)
		}
	}

	return nil
}
