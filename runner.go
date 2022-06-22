package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/spectralogic/go-core/log"
)

type Runner struct {
	log.Logger
	blockstore  BlockStore
	blockvendor *BlockVendor
	reporter    *Reporter
	iosize      int64
	stop        chan chan bool
	errchan     chan error
}

var numRunners = 0

func NewRunner(bs BlockStore, iosize int64) (*Runner, error) {
	numRunners += 1

	r := &Runner{
		Logger:      log.GetLogger(fmt.Sprintf("runner.%d", numRunners)),
		blockstore:  bs,
		blockvendor: global.BlockVendor,
		reporter:    global.Reporter,
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

func (r *Runner) WriteBlock() error {
	blk := r.blockvendor.GetBlock()
	defer r.blockvendor.ReturnBlock(blk)

	wr, err := r.blockstore.GetWriter(fmt.Sprintf("%s.%s", blk.Id.String(), blk.Extension))

	if err != nil {
		return r.LogError(fmt.Errorf("cannot get block writer: %s", err))
	}

	defer wr.Close()

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
	}

	// r.Infof("wrote block '%s'", blk.Id)

	return nil
}
