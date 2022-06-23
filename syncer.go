package main

import (
	"fmt"
	"github.com/spectralogic/go-core/log"
	"time"
)

type Syncer interface {
	// Issue sync on ObjectWriter based on policy.
	Sync(bw ObjectWriter) error

	// Log a report on sync syncTime and reset them.
	Report()
}

type SyncNone struct{}

func (s *SyncNone) Sync(_ ObjectWriter) error {
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

func (s *SyncInline) Sync(bw ObjectWriter) (e error) {
	start := time.Now()
	e = bw.Sync()
	elapsed := time.Now().Sub(start)
	s.timings.Add(elapsed)

	return e
}

func (s *SyncInline) Report() {
	fmt.Println("inline sync syncTime")
	fmt.Println(s.timings.Headers())
	fmt.Println(s.timings.String())
	s.timings.Reset()
}

type SyncRequest struct {
	bw ObjectWriter
	e  chan error
}

type SyncBatcher struct {
	log.Logger
	incoming   chan *SyncRequest
	pending    chan *SyncRequest
	interval   time.Duration
	maxPending int
	syncTime   *Histogram // time waiting for sync only to complete
	totalTime  *Histogram // total time waiting (batch delay + sync)
	stop       chan chan bool
}

func NewSyncBatcher(interval time.Duration, maxPending int) *SyncBatcher {
	s := &SyncBatcher{
		Logger:     log.GetLogger("sync-batcher"),
		incoming:   make(chan *SyncRequest, 100),
		pending:    make(chan *SyncRequest, 100),
		interval:   interval,
		maxPending: maxPending,
		syncTime:   NewHistogram(),
		totalTime:  NewHistogram(),
		stop:       make(chan chan bool),
	}

	go s.Run()

	return s
}

func (s *SyncBatcher) Sync(bw ObjectWriter) (e error) {
	start := time.Now()

	// Create sync request and wait for it to be processed.
	req := &SyncRequest{bw, make(chan error)}
	s.incoming <- req
	e = <-req.e

	s.totalTime.Add(time.Now().Sub(start))
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
		s.syncTime.Add(time.Now().Sub(start))
		req.e <- e
	}
}

func (s *SyncBatcher) Report() {
	fmt.Println("batch sync times (sync only, then wait+sync)")
	fmt.Println(s.syncTime.Headers())
	fmt.Println(s.syncTime.String())
	fmt.Println(s.totalTime.String())
	s.syncTime.Reset()
	s.totalTime.Reset()
}
