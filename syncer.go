package main

import (
	"fmt"
	"github.com/spectralogic/go-core/log"
	"time"
)

type Syncer interface {
	// Issue sync on BlockWriter based on policy.
	Sync(bw BlockWriter) error

	// Log a report on sync timings and reset them.
	Report()
}

type SyncNone struct{}

func (s *SyncNone) Sync(_ BlockWriter) error {
	return nil
}

func (s *SyncNone) Report() {
}

type SyncInline struct {
	log.Logger
	timings *Histogram
}

func NewSyncInline() *SyncInline {
	return &SyncInline{
		log.GetLogger("sync-inline"),
		NewHistogram(),
	}
}

func (s *SyncInline) Sync(bw BlockWriter) (e error) {
	start := time.Now()
	e = bw.Sync()
	elapsed := time.Now().Sub(start)
	s.timings.Add(elapsed)

	return e
}

func (s *SyncInline) Report() {
	fmt.Println("inline sync timings")
	fmt.Println(s.timings.Headers())
	fmt.Println(s.timings.String())
	s.timings.Reset()
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
	timings    *Histogram
	stop       chan chan bool
}

func NewSyncBatcher(interval time.Duration, maxPending int) *SyncBatcher {
	s := &SyncBatcher{
		Logger:     log.GetLogger("sync-batcher"),
		incoming:   make(chan *SyncRequest, 100),
		pending:    make(chan *SyncRequest, 100),
		interval:   interval,
		maxPending: maxPending,
		timings:    NewHistogram(),
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

	for i := 0; i < pending; i++ {
		// s.Infof(" - sync %d", i)
		req := <-s.pending
		start := time.Now()
		e := req.bw.Sync()
		elapsed := time.Now().Sub(start)
		req.e <- e

		s.timings.Add(elapsed)
	}
}

func (s *SyncBatcher) Report() {
	fmt.Println("batch sync timings")
	fmt.Println(s.timings.Headers())
	fmt.Println(s.timings.String())
	s.timings.Reset()
}
