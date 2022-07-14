package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type ReporterConfig struct {
	LatencyEnabled   bool
	BandwidthEnabled bool
	Interval         time.Duration
	WarmUp           time.Duration
	Capture          map[string]string // commands to run at startup
}

type Sample struct {
	Start  time.Time
	Finish time.Time
	Size   int64
}

type Reporter struct {
	*zap.SugaredLogger
	config     *ReporterConfig
	dir        string // directory for log files
	stop       func()
	preStop    bool // stop logging if true
	samples    chan *Sample
	samplePool sync.Pool
	bwtotal    []int64
	totalBytes int64
	bwlog      *os.File
	latlog     *os.File
}

func NewReporter(config *ReporterConfig) (r *Reporter, e error) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	r = &Reporter{
		SugaredLogger: Logger(),
		config:        config,
		dir:           global.RunId,
		stop: func() {
			cancel()
			wg.Wait()
		},
		preStop: false,
		samples: make(chan *Sample, 1000),
		samplePool: sync.Pool{
			New: func() interface{} {
				return &Sample{}
			},
		},
		bwtotal: make([]int64, 0, 1000),
	}

	if e = r.openFiles(); e != nil {
		return nil, e
	}

	if e = r.captureRunState(); e != nil {
		return nil, e
	}

	wg.Add(1)
	go func() {
		r.Run(ctx)
		wg.Done()
	}()

	return
}

func (r *Reporter) openFiles() (e error) {
	defer func() {
		if e != nil {
			r.closeFiles()
		}
	}()

	if r.config.BandwidthEnabled {
		path := filepath.Join(r.dir, "bandwidth.csv")
		r.bwlog, e = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)

		if e != nil {
			e = fmt.Errorf("failed creating bandwidth log: %s", e)
			return
		}

		_, e = fmt.Fprintf(r.bwlog, "# %s, %s\n", "Time(sec)", "Rate(bytes/sec)")

		if e != nil {
			e = fmt.Errorf("failed writing to bandwidth log: %s", e)
			return
		}
	}

	if r.config.LatencyEnabled {
		path := filepath.Join(r.dir, "latency.csv")
		r.latlog, e = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)

		if e != nil {
			e = fmt.Errorf("failed creating latency log: %s", e)
			return
		}

		_, e = fmt.Fprintf(r.latlog, "# %s, %s, %s\n", "Time(sec)", "Latency(sec)", "Size(bytes)")

		if e != nil {
			e = fmt.Errorf("failed writing to latency log: %s", e)
			return
		}
	}

	return nil
}

func (r *Reporter) closeFiles() {
	if r.bwlog != nil {
		_ = r.bwlog.Close()
		r.bwlog = nil
	}

	if r.latlog != nil {
		_ = r.latlog.Close()
		r.latlog = nil
	}
}

func copyFile(src string, dst string) (e error) {
	input, e := ioutil.ReadFile(src)
	if e != nil {
		return fmt.Errorf("cannot read %s: %s", src, e)
	}

	e = ioutil.WriteFile(dst, input, 0664)
	if e != nil {
		return fmt.Errorf("cannot write %s: %s", dst, e)
	}

	return nil
}

func (r *Reporter) captureRunState() (e error) {
	e = copyFile("config.json", filepath.Join(r.dir, "config.json"))
	if e != nil {
		return e
	}

	for file, command := range r.config.Capture {
		var out []byte

		c := strings.Split(command, " ")
		out, e = exec.Command(c[0], c[1:]...).Output()
		if e != nil {
			return fmt.Errorf("running '%s': %s", command, e)
		}

		e = ioutil.WriteFile(filepath.Join(r.dir, file), out, 0664)
		if e != nil {
			return fmt.Errorf("cannot write %s: %s", file, e)
		}
	}

	return nil
}

// PreStop is used to disable logging before runners are shut down.
func (r *Reporter) PreStop() {
	r.preStop = true
}

func (r *Reporter) Stop() {
	r.stop()
	r.Infof("stopped")
	r.Infof("median bandwidth: %s/sec", SprintSize(Median(r.bwtotal)))
	r.Infof("mean bandwidth: %s/sec", SprintSize(Mean(r.bwtotal)))
	r.Infof("total written: %s", SprintSize(r.totalBytes))
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
	defer r.closeFiles()

	if e := r.warmUp(ctx); e != nil {
		return
	}

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
			r.totalBytes += sample.Size

			if r.latlog != nil && !r.preStop {
				fmt.Fprintf(r.latlog, "%.3f, %.6f, %d\n",
					sample.Finish.Sub(startTime).Seconds(),
					sample.Finish.Sub(sample.Start).Seconds(),
					sample.Size)
			}

			r.samplePool.Put(sample)

		case tick := <-t.C:
			if !r.preStop {
				interval := tick.Sub(lastReportTime).Seconds()

				// Convert from accumulated bytes in the interval to the
				// rate (bytes/sec) for that interval
				rate := int64(float64(intervalBytes) / interval)
				fmt.Printf("intervalBytes: %d\n", intervalBytes)
				fmt.Printf("interval: %f\n", interval)
				fmt.Printf("rate: %d\n", rate)
				r.Infof("bandwidth: %s/sec", SprintSize(rate))

				r.bwtotal = append(r.bwtotal, rate)

				if r.bwlog != nil {
					fmt.Fprintf(r.bwlog, "%.3f, %d\n", tick.Sub(startTime).Seconds(), rate)
				}
			}

			lastReportTime = tick
			intervalBytes = int64(0)

		case <-t2.C:
			if !r.preStop {
				global.Syncer.Report()
			}
		}
	}
}

// warmUp simply delays reporting until the warm-up time is complete, giving runners some time to get to speed.
func (r *Reporter) warmUp(ctx context.Context) error {
	warmUp := r.config.WarmUp
	if warmUp == 0 {
		return nil
	}

	r.Infof("not enabled yet, in pre-start (%.0f seconds)", warmUp.Seconds())
	done := time.NewTimer(warmUp)
	report := time.NewTicker(time.Second * 5)

	defer func() {
		done.Stop()
		report.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled")

		case <-done.C:
			r.Infof("warm-up finished")
			return nil

		case <-report.C:
			r.Infof("waiting for warm-up to finish...")

		case sample := <-r.samples:
			r.samplePool.Put(sample) // discard any samples during warmup
		}
	}
}
