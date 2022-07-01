package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/spectralogic/go-core/fmt2"
	"github.com/spectralogic/go-core/log"
)

type ReporterConfig struct {
	LatencyEnabled   bool
	BandwidthEnabled bool
	Interval         time.Duration
}

type Sample struct {
	Start  time.Time
	Finish time.Time
	Size   int64
}

type Reporter struct {
	log.Logger
	config     *ReporterConfig
	stop       func()
	samples    chan *Sample
	samplePool sync.Pool
	bwlog      *os.File
	latlog     *os.File
}

func NewReporter(config *ReporterConfig) (r *Reporter, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	r = &Reporter{
		Logger: log.GetLogger("reporter"),
		config: config,
		stop: func() {
			cancel()
			wg.Wait()
		},
		samples: make(chan *Sample, 1000),
		samplePool: sync.Pool{
			New: func() interface{} {
				return &Sample{}
			},
		},
	}

	if err = r.openFiles(); err != nil {
		return nil, err
	}

	wg.Add(1)
	go func() {
		r.Run(ctx)
		wg.Done()
	}()

	return
}

func (r *Reporter) openFiles() (err error) {
	defer func() {
		// Close both files on error
		if err != nil {
			if r.bwlog != nil {
				_ = r.bwlog.Close()
			}
			if r.latlog != nil {
				_ = r.latlog.Close()
			}
		}
	}()

	if r.config.BandwidthEnabled {
		r.bwlog, err = os.OpenFile(fmt.Sprintf("bandwidth.%s.csv", global.RunId), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			err = r.LogError(fmt.Errorf("failed creating bandwidth log: %s", err))
			return
		}

		_, err = fmt.Fprintf(r.bwlog, "# %s, %s\n", "Time(sec)", "Rate(bytes/sec)")

		if err != nil {
			err = r.LogError(fmt.Errorf("failed writing to bandwidth log: %s", err))
			return
		}
	}

	if r.config.LatencyEnabled {
		r.latlog, err = os.OpenFile(fmt.Sprintf("latency.%s.csv", global.RunId), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

		if err != nil {
			err = r.LogError(fmt.Errorf("failed creating latency log: %s", err))
			return
		}

		_, err = fmt.Fprintf(r.latlog, "# %s, %s, %s\n", "Time(sec)", "Latency(sec)", "Size(bytes)")

		if err != nil {
			err = r.LogError(fmt.Errorf("failed writing to latency log: %s", err))
			return
		}
	}

	return nil
}

func (r *Reporter) Stop() {
	r.stop()
	r.Infof("stopped")
}

func (r *Reporter) GetSample() *Sample {
	s := r.samplePool.Get().(*Sample)
	s.Start = time.Now()
	return s
}

func (r *Reporter) CaptureSample(s *Sample, size int64) {
	s.Finish = time.Now()
	s.Size = size
	r.samples <- s
}

func (r *Reporter) Run(ctx context.Context) {
	defer func() {
		if r.bwlog != nil {
			r.bwlog.Close()
			r.bwlog = nil
		}

		if r.latlog != nil {
			r.latlog.Close()
			r.latlog = nil
		}
	}()

	r.Infof("running")
	intervalBytes := int64(0)
	startTime := time.Now()
	lastReportTime := startTime

	t := time.NewTicker(r.config.Interval)
	t2 := time.NewTicker(time.Second * 10)

	for {
		select {
		case <-ctx.Done():
			t.Stop()
			t2.Stop()
			return

		case sample := <-r.samples:
			intervalBytes += sample.Size

			if r.latlog != nil {
				fmt.Fprintf(r.latlog, "%.3f, %.6f, %d\n",
					sample.Finish.Sub(startTime).Seconds(),
					sample.Finish.Sub(sample.Start).Seconds(),
					sample.Size)
			}

			r.samplePool.Put(sample)

		case tick := <-t.C:
			interval := tick.Sub(lastReportTime).Seconds()

			// Convert from accumulated bytes in the interval to the
			// rate (bytes/sec) for that interval
			rate := int64(float64(intervalBytes) / interval)
			fmt.Printf("- %s/sec\n", fmt2.SprintSize(rate))

			if r.bwlog != nil {
				fmt.Fprintf(r.bwlog, "%.3f, %d\n", tick.Sub(startTime).Seconds(), rate)
			}

			lastReportTime = tick
			intervalBytes = int64(0)

		case <-t2.C:
			global.Syncer.Report()
		}
	}
}
