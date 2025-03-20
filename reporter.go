package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	Read  = 0 // Match fio's read op
	Write = 1 // Match fio's write op
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
	Op     int
	Size   int
}

type Reporter struct {
	*zap.SugaredLogger
	config         *ReporterConfig
	dir            string // directory for log files
	stop           func()
	preStop        bool // stop logging if true
	samples        chan *Sample
	samplePool     sync.Pool
	readBandwidth  []int64
	writeBandwidth []int64
	readTotal      int64
	writeTotal     int64
	bwlog          *os.File
	latlog         *os.File
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
		readBandwidth:  make([]int64, 0, 1000),
		writeBandwidth: make([]int64, 0, 1000),
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

		if out, e = RunCmd(command); e != nil {
			return e
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

	if r.readTotal > 0 {
		r.Infof("read bandwidth (median): %s/sec", SprintSize(Median(r.readBandwidth)))
		r.Infof("read bandwidth (mean): %s/sec", SprintSize(Mean(r.readBandwidth)))
		r.Infof("total read: %s", SprintSize(r.readTotal))
	}

	if r.writeTotal > 0 {
		r.Infof("write bandwidth (median): %s/sec", SprintSize(Median(r.writeBandwidth)))
		r.Infof("write bandwidth (mean): %s/sec", SprintSize(Mean(r.writeBandwidth)))
		r.Infof("total written: %s", SprintSize(r.writeTotal))
	}
}

func (r *Reporter) GetSample() *Sample {
	s := r.samplePool.Get().(*Sample)
	s.Start = time.Now()
	return s
}

func (r *Reporter) CaptureSample(s *Sample, size int, op int) {
	s.Finish = time.Now()
	s.Size = size
	s.Op = op
	r.samples <- s
}

func (r *Reporter) Run(ctx context.Context) {
	defer r.closeFiles()

	if e := r.warmUp(ctx); e != nil {
		return
	}

	r.Infof("running")
	intervalReadBytes := int64(0)
	intervalWriteBytes := int64(0)
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
			switch sample.Op {
			case Read:
				intervalReadBytes += int64(sample.Size)
				r.readTotal += int64(sample.Size)
			case Write:
				intervalWriteBytes += int64(sample.Size)
				r.writeTotal += int64(sample.Size)
			default:
				r.Errorf("unknown op: %d", sample.Op)
			}

			if r.latlog != nil && !r.preStop && sample.Size > 0 {
				fmt.Fprintf(r.latlog, "%.3f, %.6f, %d, %d\n",
					sample.Finish.Sub(startTime).Seconds(),
					sample.Finish.Sub(sample.Start).Seconds(),
					sample.Op,
					sample.Size)
			}

			r.samplePool.Put(sample)

		case tick := <-t.C:
			if !r.preStop {
				interval := tick.Sub(lastReportTime).Seconds()

				// Convert from accumulated bytes in the interval to the
				// rate (bytes/sec) for that interval
				readBandwidth := int64(float64(intervalReadBytes) / interval)
				r.readBandwidth = append(r.readBandwidth, readBandwidth)

				if readBandwidth > 1 {
					r.Infof("read bandwidth:  %s/sec", SprintSize(readBandwidth))
				}

				writeBandwidth := int64(float64(intervalWriteBytes) / interval)
				r.writeBandwidth = append(r.writeBandwidth, writeBandwidth)

				if writeBandwidth > 1 {
					r.Infof("write bandwidth: %s/sec", SprintSize(writeBandwidth))
				}

				if r.bwlog != nil {
					fmt.Fprintf(r.bwlog, "%.3f, %d, %d\n", tick.Sub(startTime).Seconds(), Read, readBandwidth)
					fmt.Fprintf(r.bwlog, "%.3f, %d, %d\n", tick.Sub(startTime).Seconds(), Write, writeBandwidth)
				}
			}

			lastReportTime = tick
			intervalWriteBytes = int64(0)
			intervalReadBytes = int64(0)

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
