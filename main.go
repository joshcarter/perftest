package main

import (
	"fmt"
	"github.com/spectralogic/go-core/log"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"time"
)

type SyncWhen int

const (
	SyncOnClose SyncWhen = 1
	SyncOnWrite SyncWhen = 2
)

// runnerInitFn is a function that takes a RunnerList and adds its own Error
// should be non-nil if there was a fatal error starting runners.
type runnerInitFn func(rl *RunnerList) error

type Globals struct {
	ObjectVendor  *ObjectVendor
	Reporter      *Reporter
	RunId         string // unique name for this run
	RunnerInitFns []runnerInitFn
	RunnerError   chan error
	Syncer        Syncer
	SyncWhen      SyncWhen
	IoSize        int64
}

var global = &Globals{
	RunnerInitFns: []runnerInitFn{},
	RunnerError:   make(chan error, 10),
}

func init() {
	global.RunId = time.Now().Format("2006-02-01-15-04-05")
	global.RunnerInitFns = append(global.RunnerInitFns, startFileRunners)
}

func main() {
	var err error
	logger := log.GetLogger("main")

	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetDefault("iosize", "1MB")
	viper.SetDefault("size", "4MB/100/dat")
	viper.SetDefault("reporter.maxWait", "1s")
	viper.SetDefault("compressibility", "50")

	if err = viper.ReadInConfig(); err != nil {
		logger.Errorf("error reading config file: %s\n", err)
		os.Exit(-1)
	}

	iosize := viper.GetSizeInBytes("iosize")

	if iosize == 0 {
		logger.Errorf("no io size specified; create 'iosize' in config.json")
		os.Exit(-1)
	}

	sizespec := viper.GetString("size")

	if len(sizespec) == 0 {
		logger.Errorf("no file size specified; create 'size' in config.json")
		os.Exit(-1)
	}

	compressibility := viper.GetInt("compressibility")

	global.ObjectVendor, err = NewObjectVendor(sizespec, compressibility)

	if err != nil {
		logger.Errorf("cannot create object vendor: %s", err)
		os.Exit(-1)
	}

	reporterInterval := viper.GetDuration("reporter.interval")
	if reporterInterval == 0 {
		logger.Errorf("no reporter interval specified; create 'reporter.interval' in config.json")
		os.Exit(-1)
	}

	reporterConfig := &ReporterConfig{
		Interval:         reporterInterval,
		LatencyEnabled:   viper.GetBool("reporter.loglatency"),
		BandwidthEnabled: viper.GetBool("reporter.logbandwidth"),
	}

	global.Reporter, err = NewReporter(reporterConfig)

	if err != nil {
		logger.Errorf("failed creating reporter: %s", err)
	}

	rl := NewRunnerList()

	for _, fn := range global.RunnerInitFns {
		err = fn(rl)

		if err != nil {
			logger.Errorf(err.Error())
			os.Exit(-1)
		}
	}

	rl.Start()

	logger.Infof("running... press Control-C to stop.")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	for {
		select {
		case <-sig:
			logger.Infof("Control-C, stopping.")
			goto stop

		case err = <-global.RunnerError:
			logger.Errorf("runner error: %s", err)
			goto stop
		}
	}

stop:

	rl.Stop()
	global.Syncer.Stop()
	global.Reporter.Stop()
	os.Exit(0)
}

func startFileRunners(rl *RunnerList) (err error) {
	logger := log.GetLogger("main")
	paths := viper.GetStringSlice("file.paths")

	if len(paths) == 0 {
		logger.Infof("no file runner paths specified; skipping")
		return nil
	}

	runnersPerPath := viper.GetInt("file.runners_per_path")

	if runnersPerPath == 0 {
		return fmt.Errorf("file store must have more than 1 runner per path (set file.runners_per_path)")
	}

	global.IoSize = int64(viper.GetSizeInBytes("iosize"))

	willSync := false
	switch viper.GetString("file.sync") {
	case "close", "inline":
		logger.Infof("syncing inline")
		global.Syncer = NewSyncInline()
		willSync = true
	case "batch", "batched", "batcher":
		logger.Infof("syncing in batches")

		syncBatcherMaxWait := viper.GetDuration("sync_batcher.max_wait")
		if syncBatcherMaxWait == 0 {
			return fmt.Errorf("no max_wait specified; create 'sync_batcher.max_wait' in config.json")
		}

		syncBatcherMaxPending := viper.GetInt("sync_batcher.max_pending")
		if syncBatcherMaxPending == 0 {
			return fmt.Errorf("no max_pending specified; create 'sync_batcher.max_pending' in config.json")
		}

		syncBatcherParallel := viper.GetBool("sync_batcher.parallel")

		global.Syncer = NewSyncBatcher(syncBatcherMaxWait, syncBatcherMaxPending, syncBatcherParallel)
		willSync = true
	default:
		global.Syncer = &SyncNone{}
	}

	global.SyncWhen = SyncOnClose
	if willSync {
		switch viper.GetString("file.sync_on") {
		case "write", "io":
			global.SyncWhen = SyncOnWrite
			logger.Infof("sync after every write")
		default:
			logger.Infof("sync on file close")
		}
	}

	openFlags := parseOpenFlags(viper.GetStringSlice("file.open_flags"))
	totalRunners := 0

	for _, path := range paths {
		var o ObjectStore

		if o, err = NewFileObjectStore(path, openFlags); err != nil {
			return fmt.Errorf("cannot init store: %s", err)
		}

		rl.AddStore(o)

		for i := 0; i < runnersPerPath; i++ {
			var r *Runner

			if r, err = NewRunner(o, totalRunners+1); err != nil {
				return fmt.Errorf("error initializing runner: %s", err)
			}

			rl.AddRunner(r)
			totalRunners++
		}
	}

	return nil
}
