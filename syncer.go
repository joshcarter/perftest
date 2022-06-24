package main

import (
	"fmt"
	"github.com/spectralogic/go-core/log"
	"sync"
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
	fmt.Println("inline sync times")
	fmt.Println(s.timings.Headers())
	fmt.Println(s.timings.String())
	s.timings.Reset()
}

type SyncRequest struct {
	bw        ObjectWriter
	submitted time.Time
	e         chan error
}

type SyncBatcher struct {
	log.Logger
	incoming   chan *SyncRequest
	pending    chan *SyncRequest
	maxWait    time.Duration
	maxPending int
	parallel   bool       // do syncs in many goroutines, or just one
	syncTime   *Histogram // time waiting for sync only to complete
	totalTime  *Histogram // total time waiting (batch delay + sync)
	stop       chan chan bool
}

func NewSyncBatcher(maxWait time.Duration, maxPending int, parallel bool) *SyncBatcher {
	s := &SyncBatcher{
		Logger:     log.GetLogger("sync-batcher"),
		incoming:   make(chan *SyncRequest, 100),
		pending:    make(chan *SyncRequest, 100),
		maxWait:    maxWait,
		maxPending: maxPending,
		parallel:   parallel,
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
	req := &SyncRequest{bw, time.Now(), make(chan error, 1)}
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

	// Init timer with "long" time. It'll get reset to a shorter duration once we have a sync to process.
	longInterval := time.Second * 10
	t := time.NewTimer(longInterval)

	for {
		select {
		case stopChan := <-s.stop:
			t.Stop()
			stopChan <- true
			s.Infof("stopped")
			return

		case req := <-s.incoming:
			if len(s.pending) == 0 {
				// Set timer to ensure the 1st pending sync gets processed within its deadline. Any syncs received
				// between now and then will get processed in the same batch.
				wait := s.maxWait - time.Now().Sub(req.submitted)
				if !t.Stop() {
					<-t.C
				}
				t.Reset(wait)
			}

			s.pending <- req

			if len(s.pending) >= s.maxPending {
				// s.Infof("flushing early")
				s.SyncPending()

				// Reset timer
				if !t.Stop() {
					<-t.C
				}
				t.Reset(longInterval)
			}

		case <-t.C:
			// s.Infof("flushing because deadline expired")
			s.SyncPending()
			t.Reset(longInterval)
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

	if s.parallel {
		s.syncPendingParallel(pending)
	} else {
		s.syncPendingSequential(pending)
	}
}

// SyncPending will sync what's in the pending queue.
func (s *SyncBatcher) syncPendingSequential(pending int) {
	for i := 0; i < pending; i++ {
		req := <-s.pending

		// fmt.Printf("req %d of %d waited %d ms\n", i+1, pending, time.Now().Sub(req.submitted).Milliseconds())

		start := time.Now()
		e := req.bw.Sync()
		s.syncTime.Add(time.Now().Sub(start))

		req.e <- e
	}
}

// SyncPending will sync what's in the pending queue.
func (s *SyncBatcher) syncPendingParallel(pending int) {
	var wg sync.WaitGroup
	for i := 0; i < pending; i++ {
		req := <-s.pending

		wg.Add(1)
		go func(req *SyncRequest, syncTime *Histogram) {
			// fmt.Printf(" - req waited %d ms\n", time.Now().Sub(req.submitted).Milliseconds())

			start := time.Now()
			e := req.bw.Sync()
			s.syncTime.Add(time.Now().Sub(start))
			req.e <- e
			wg.Done()
		}(req, s.syncTime)
	}
	wg.Wait()
}

func (s *SyncBatcher) Report() {
	fmt.Println("batch sync times (sync only, then wait+sync)")
	fmt.Println(s.syncTime.Headers())
	fmt.Println(s.syncTime.String())
	fmt.Println(s.totalTime.String())
	s.syncTime.Reset()
	s.totalTime.Reset()
}
