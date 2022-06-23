package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/spectralogic/go-core/log"
)

type SyncOpt int

const (
	SyncOnClose SyncOpt = 1
	SyncOnWrite SyncOpt = 2
)

type Runner struct {
	log.Logger
	blockstore  BlockStore
	blockvendor *BlockVendor
	reporter    *Reporter
	syncer      Syncer
	syncOpt     SyncOpt
	iosize      int64
	stop        chan chan bool
	errchan     chan error
}

var numRunners = 0

func NewRunner(bs BlockStore, iosize int64, syncOpt SyncOpt) (*Runner, error) {
	numRunners += 1

	r := &Runner{
		Logger:      log.GetLogger(fmt.Sprintf("runner.%d", numRunners)),
		blockstore:  bs,
		blockvendor: global.BlockVendor,
		reporter:    global.Reporter,
		syncer:      global.Syncer,
		syncOpt:     syncOpt,
		iosize:      iosize,
		stop:        make(chan chan bool, 1),
		errchan:     global.RunnerError,
	}

	r.Infof("creating runner")

	return r, nil
}

func (r *Runner) Stop() {
	r.Infof("stopping")
	stopChan := make(chan bool)
	r.stop <- stopChan // request stop
	<-stopChan         // stop acknowledged
}

func StopRunners(runners []*Runner) {
	stopChans := make([]chan bool, len(runners))

	// Request for all to stop
	for i, r := range runners {
		stopChan := make(chan bool)
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
			err := r.WriteBlock()

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

func (r *Runner) WriteBlock() (e error) {
	blk := r.blockvendor.GetBlock()
	defer r.blockvendor.ReturnBlock(blk)

	wr, e := r.blockstore.GetWriter(fmt.Sprintf("%s.%s", blk.Id.String(), blk.Extension))

	if e != nil {
		return r.LogError(fmt.Errorf("cannot get block writer: %s", e))
	}

	defer func() {
		if e == nil && r.syncOpt == SyncOnClose {
			e = r.syncer.Sync(wr)
		}

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
		iosize := r.iosize

		if iosize > int64(remaining) {
			iosize = int64(remaining)
		}

		// r.Infof("writing '%s': %d bytes, %d remaining", blk.Id, iosize, remaining)

		sample := r.reporter.GetSample()
		bw, err := io.CopyN(wr, rd, iosize)
		r.reporter.CaptureSample(sample, bw)

		remaining -= int(bw)

		if err == io.EOF || remaining == 0 {
			break
		} else if err != nil {
			return r.LogError(err)
		} else if bw < iosize {
			r.Infof("short write: expected %d, got %d", iosize, bw)
		}

		if r.syncOpt == SyncOnWrite {
			if err = r.syncer.Sync(wr); err != nil {
				return r.LogError(err)
			}
		}
	}

	// r.Infof("wrote block '%s'", blk.Id)

	return nil
}
