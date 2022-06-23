package main

import (
	"fmt"
	"github.com/spectralogic/go-core/log"
	"github.com/spectralogic/go-core/stat"
	"time"
)

type Syncer interface {
	Sync(bw BlockWriter) error
}

type SyncNone struct{}

func (s *SyncNone) Sync(_ BlockWriter) error {
	return nil
}

type SyncInline struct{}

func (s *SyncInline) Sync(bw BlockWriter) error {
	return bw.Sync()
}

type SyncRequest struct {
	bw BlockWriter
	e  chan error
}

type SyncBatcher struct {
	log.Logger
	incoming   chan *SyncRequest
	pending    chan *SyncRequest
	interval   time.Duration
	maxPending int
	stop       chan chan bool
}

func NewSyncBatcher(interval time.Duration, maxPending int) *SyncBatcher {
	s := &SyncBatcher{
		Logger:     log.GetLogger("sync-batcher"),
		incoming:   make(chan *SyncRequest, 100),
		pending:    make(chan *SyncRequest, 100),
		interval:   interval,
		maxPending: maxPending,
		stop:       make(chan chan bool),
	}

	go s.Run()

	return s
}

func (s *SyncBatcher) Sync(bw BlockWriter) (e error) {
	req := &SyncRequest{bw, make(chan error)}
	s.incoming <- req
	e = <-req.e
	return e
}

func (s *SyncBatcher) Stop() {
	s.Infof("stopping")
	stopChan := make(chan bool)
	s.stop <- stopChan // request stop
	<-stopChan         // stop acknowledged
}

func (s *SyncBatcher) Run() {
	s.Infof("running")

	t := time.NewTimer(s.interval)

	for {
		select {
		case stopChan := <-s.stop:
			t.Stop()
			stopChan <- true
			s.Infof("stopped")
			return

		case req := <-s.incoming:
			s.pending <- req
			// s.Infof("enqueuing sync request (incoming %d, pending %d)", len(s.incoming), len(s.pending))
			if len(s.pending) >= s.maxPending {
				// s.Infof("flushing early")
				s.SyncPending()

				// Reset timer
				if !t.Stop() {
					<-t.C
				}
				t.Reset(s.interval)
			}

		case <-t.C:
			// s.Infof("tick")
			s.SyncPending()
			t.Reset(s.interval)
		}
	}
}

// SyncPending will sync what's in the pending queue.
func (s *SyncBatcher) SyncPending() {
	pending := len(s.pending)
	if pending == 0 {
		return
	}

	// s.Infof("sync'ing %d pending writers", pending)

	hist := stat.NewHistogram()

	for i := 0; i < pending; i++ {
		// s.Infof(" - sync %d", i)
		req := <-s.pending
		start := time.Now()
		e := req.bw.Sync()
		end := time.Now()
		req.e <- e

		elapsed := end.Sub(start)
		hist.Add(elapsed)
	}

	fmt.Println(hist.Headers())
	fmt.Println(hist.String())
	// log.Infof("batch sync timings:\n%s\n%s\n", hist.Headers(), hist.String())
}
