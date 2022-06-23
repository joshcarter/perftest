package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/spectralogic/go-core/log"
)

type SyncWhen int

const (
	SyncOnClose SyncWhen = 1
	SyncOnWrite SyncWhen = 2
)

type Runner struct {
	log.Logger
	objectStore  ObjectStore
	objectVendor *ObjectVendor
	reporter     *Reporter
	syncer       Syncer
	syncWhen     SyncWhen
	iosize       int64
	stop         chan chan bool
	errchan      chan error
}

var numRunners = 0

func NewRunner(os ObjectStore, iosize int64, syncWhen SyncWhen) (*Runner, error) {
	numRunners += 1

	r := &Runner{
		Logger:       log.GetLogger(fmt.Sprintf("runner.%d", numRunners)),
		objectStore:  os,
		objectVendor: global.ObjectVendor,
		reporter:     global.Reporter,
		syncer:       global.Syncer,
		syncWhen:     syncWhen,
		iosize:       iosize,
		stop:         make(chan chan bool, 1),
		errchan:      global.RunnerError,
	}

	r.Infof("creating runner")

	return r, nil
}

func (r *Runner) Stop() {
	r.Infof("stopping")
	stopChan := make(chan bool, 1)
	r.stop <- stopChan // request stop
	<-stopChan         // stop acknowledged
}

func StopRunners(runners []*Runner) {
	stopChans := make([]chan bool, len(runners))

	// Request for all to stop
	for i, r := range runners {
		stopChan := make(chan bool, 1)
		r.Infof("stopping")
		r.stop <- stopChan
		stopChans[i] = stopChan
	}

	// Wait for all to stop
	for _, stopChan := range stopChans {
		<-stopChan
	}
}

func (r *Runner) Run() {
	r.Infof("running")

	for {
		select {
		case stopChan := <-r.stop:
			stopChan <- true
			r.Infof("stopped")
			return

		default:
			err := r.WriteObject()

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

func (r *Runner) WriteObject() (e error) {
	blk := r.objectVendor.GetObject()
	defer r.objectVendor.ReturnObject(blk)

	wr, e := r.objectStore.GetWriter(fmt.Sprintf("%s.%s", blk.Id.String(), blk.Extension))

	if e != nil {
		return r.LogError(fmt.Errorf("cannot get block writer: %s", e))
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

	for remaining > 0 {
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
			e = r.LogError(fmt.Errorf("short write: expected %d, got %d", iosize, bw))
			return
		}

		if r.syncWhen == SyncOnWrite {
			if e = r.syncer.Sync(wr); e != nil {
				r.Errorf("sync: %s", e)
				return
			}
		}
	}

	// r.Infof("wrote block '%s'", blk.Id)

	if r.syncWhen == SyncOnClose {
		e = r.syncer.Sync(wr)
	}

	return
}
