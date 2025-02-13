package main

import (
	"context"
	"github.com/iceber/iouring-go"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

type Syncer interface {
	// Issue sync on ObjectWriter based on policy.
	Sync(s ObjectStore, f *os.File) error

	// Log a report on sync syncTime and reset them.
	Report()

	// Stop any background goroutines.
	Stop()
}

type SyncNone struct{}

func (s *SyncNone) Sync(_ ObjectStore, _ *os.File) error {
	return nil
}

func (s *SyncNone) Report() {
}

func (s *SyncNone) Stop() {
}

type SyncInline struct {
	*zap.SugaredLogger
	timings *Histogram
}

func NewSyncInline() *SyncInline {
	return &SyncInline{
		Logger(),
		NewHistogram(),
	}
}

func (s *SyncInline) Sync(store ObjectStore, f *os.File) (e error) {
	start := time.Now()

	preq := iouring.Fsync(int(f.Fd()))
	ch := make(chan iouring.Result, 1)
	if _, e = store.Ring().SubmitRequest(preq, ch); e != nil {
		return
	}
	res := <-ch

	elapsed := time.Now().Sub(start)
	s.timings.Add(elapsed)

	e = res.Err()
	return
}

func (s *SyncInline) Report() {
	s.Infof("inline sync times")
	s.Infof(s.timings.Headers())
	s.Infof(s.timings.String())
	s.timings.Reset()
}

func (s *SyncInline) Stop() {
}

type SyncRequest struct {
	file      *os.File
	store     ObjectStore
	submitted time.Time
	e         chan error
}

type SyncBatcher struct {
	*zap.SugaredLogger
	incoming   chan *SyncRequest
	pending    chan *SyncRequest
	maxWait    time.Duration
	maxPending int
	syncTime   *Histogram // time waiting for sync only to complete
	totalTime  *Histogram // total time waiting (batch delay + sync)
	stop       func()
}

func NewSyncBatcher(maxWait time.Duration, maxPending int) *SyncBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	s := &SyncBatcher{
		SugaredLogger: Logger(),
		incoming:      make(chan *SyncRequest, 100),
		pending:       make(chan *SyncRequest, 100),
		maxWait:       maxWait,
		maxPending:    maxPending,
		syncTime:      NewHistogram(),
		totalTime:     NewHistogram(),
		stop: func() {
			cancel()
			wg.Wait()
		},
	}

	wg.Add(1)
	go func() {
		s.Run(ctx)
		wg.Done()
	}()

	return s
}

func (s *SyncBatcher) Sync(store ObjectStore, f *os.File) (e error) {
	start := time.Now()

	// Create sync request and wait for it to be processed.
	req := &SyncRequest{f, store, time.Now(), make(chan error, 1)}
	s.incoming <- req
	e = <-req.e

	s.totalTime.Add(time.Now().Sub(start))
	return e
}

func (s *SyncBatcher) Stop() {
	s.stop()
	s.Infof("stopped")
}

func (s *SyncBatcher) Run(ctx context.Context) {
	s.Infof("running")

	// Init timer with "long" time. It'll get reset to a shorter duration once we have a sync to process.
	longInterval := time.Second * 10
	t := time.NewTimer(longInterval)

	for {
		select {
		case <-ctx.Done():
			t.Stop()
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

	reqs := make([]*SyncRequest, pending)

	for i := 0; i < pending; i++ {
		reqs[i] = <-s.pending
	}

	// for the moment I'm being lazy and don't want to support multiple stores
	ring := reqs[0].store.Ring()
	results := make(chan iouring.Result, pending)
	start := time.Now()

	// Submit all the syncs
	for _, req := range reqs {
		preq := iouring.Fsync(int(req.file.Fd())).WithInfo(req)
		_, e := ring.SubmitRequest(preq, results)

		if e != nil {
			s.Errorf("sync error: %v", e)
		}
	}

	// Consume all the results
	for i := 0; i < pending; i++ {
		res := <-results
		req := res.GetRequestInfo().(*SyncRequest)
		req.e <- res.Err()
		s.syncTime.Add(time.Now().Sub(start))
	}
}

func (s *SyncBatcher) Report() {
	s.Infof("batch sync times (sync only, then wait+sync)")
	s.Infof(s.syncTime.Headers())
	s.Infof(s.syncTime.String())
	s.Infof(s.totalTime.String())
	s.syncTime.Reset()
	s.totalTime.Reset()
}
