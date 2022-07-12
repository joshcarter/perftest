package main

import (
	"context"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Syncer interface {
	// Issue sync on ObjectWriter based on policy.
	Sync(bw ObjectWriter) error

	// Log a report on sync syncTime and reset them.
	Report()

	// Stop any background goroutines.
	Stop()

	Locker() sync.Locker
}

type NilLocker struct{}

func (l *NilLocker) Lock() {
	return
}

func (l *NilLocker) Unlock() {
	return
}

type SyncNone struct{}

func (s *SyncNone) Sync(_ ObjectWriter) error {
	return nil
}

func (s *SyncNone) Report() {
}

func (s *SyncNone) Stop() {
}

func (s *SyncNone) Locker() sync.Locker {
	return &NilLocker{}
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

func (s *SyncInline) Sync(bw ObjectWriter) (e error) {
	start := time.Now()
	e = bw.Sync()
	elapsed := time.Now().Sub(start)
	s.timings.Add(elapsed)

	return e
}

func (s *SyncInline) Report() {
	s.Infof("inline sync times")
	s.Infof(s.timings.Headers())
	s.Infof(s.timings.String())
	s.timings.Reset()
}

func (s *SyncInline) Stop() {
}

func (s *SyncInline) Locker() sync.Locker {
	return &NilLocker{}
}

type SyncRequest struct {
	bw        ObjectWriter
	submitted time.Time
	e         chan error
}

type SyncBatcher struct {
	*zap.SugaredLogger
	incoming   chan *SyncRequest
	pending    chan *SyncRequest
	maxWait    time.Duration
	maxPending int
	parallel   bool // do syncs in many goroutines, or just one
	rwlock     *sync.RWMutex
	syncTime   *Histogram // time waiting for sync only to complete
	totalTime  *Histogram // total time waiting (batch delay + sync)
	stop       func()
}

func NewSyncBatcher(maxWait time.Duration, maxPending int, parallel bool, locked bool) *SyncBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	s := &SyncBatcher{
		SugaredLogger: Logger(),
		incoming:      make(chan *SyncRequest, 100),
		pending:       make(chan *SyncRequest, 100),
		maxWait:       maxWait,
		maxPending:    maxPending,
		parallel:      parallel,
		syncTime:      NewHistogram(),
		totalTime:     NewHistogram(),
		stop: func() {
			cancel()
			wg.Wait()
		},
	}

	if locked {
		s.rwlock = &sync.RWMutex{}
		s.Infof("locking IO during batch sync")
	}

	wg.Add(1)
	go func() {
		s.Run(ctx)
		wg.Done()
	}()

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
	s.stop()
	s.Infof("stopped")
}

func (s *SyncBatcher) Locker() sync.Locker {
	if s.rwlock != nil {
		// s.Infof("locker: returning rlocker")
		return s.rwlock.RLocker()
	} else {
		// s.Infof("locker: returning nil locker")
		return &NilLocker{}
	}
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

	if s.rwlock != nil {
		s.rwlock.Lock()
		defer s.rwlock.Unlock()
	}

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
	s.Infof("batch sync times (sync only, then wait+sync)")
	s.Infof(s.syncTime.Headers())
	s.Infof(s.syncTime.String())
	s.Infof(s.totalTime.String())
	s.syncTime.Reset()
	s.totalTime.Reset()
}
