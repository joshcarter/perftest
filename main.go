package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/spectralogic/go-core/log"
	"github.com/spf13/viper"
)

// runnerInitFn is a function that takes a runner list, starts its own
// runners, and returns a new runner list with its own runners
// inserted (if any). Error should only be non-nil if there was a
// fatal error starting runners.
type runnerInitFn func([]*Runner) ([]*Runner, error)

type Globals struct {
	ObjectVendor  *ObjectVendor
	Reporter      *Reporter
	RunnerInitFns []runnerInitFn
	RunnerError   chan error
	Syncer        Syncer
}

var global = &Globals{
	RunnerInitFns: []runnerInitFn{},
	RunnerError:   make(chan error, 10),
}

func init() {
	global.RunnerInitFns = append(global.RunnerInitFns, startFileRunners)
}

func main() {
	var err error
	logger := log.GetLogger("main")

	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetDefault("iosize", "1MB")
	viper.SetDefault("size", "4MB/100/dat")
	viper.SetDefault("reporter.interval", "1s")
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

	runners := make([]*Runner, 0)

	for _, fn := range global.RunnerInitFns {
		runners, err = fn(runners)

		if err != nil {
			logger.Errorf(err.Error())
			os.Exit(-1)
		}
	}

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

	StopRunners(runners)
	global.Reporter.Stop()

	os.Exit(0)
}

func startFileRunners(runners []*Runner) ([]*Runner, error) {
	logger := log.GetLogger("main")
	paths := viper.GetStringSlice("file.paths")
	// fsync := viper.GetString("fsync")

	if len(paths) == 0 {
		logger.Infof("no file runner paths specified; skipping")
		return runners, nil
	}

	runnersPerPath := viper.GetInt("file.runners_per_path")

	if runnersPerPath == 0 {
		return nil, fmt.Errorf("file store must have more than 1 runner per path (set file.runners_per_path)")
	}

	iosize := viper.GetSizeInBytes("iosize")

	willSync := false
	switch viper.GetString("file.sync") {
	case "close", "inline":
		logger.Infof("syncing inline")
		global.Syncer = NewSyncInline()
		willSync = true
	case "batch", "batched", "batcher":
		logger.Infof("syncing in batches")

		syncBatcherInterval := viper.GetDuration("sync_batcher.interval")
		if syncBatcherInterval == 0 {
			logger.Errorf("no sync_batcher interval specified; create 'sync_batcher.interval' in config.json")
			os.Exit(-1)
		}

		syncBatcherMaxPending := viper.GetInt("sync_batcher.max_pending")
		if syncBatcherMaxPending == 0 {
			logger.Errorf("no sync_batcher max_pending specified; create 'sync_batcher.max_pending' in config.json")
			os.Exit(-1)
		}

		global.Syncer = NewSyncBatcher(syncBatcherInterval, syncBatcherMaxPending)
		willSync = true
	default:
		global.Syncer = &SyncNone{}
	}

	var syncWhen = SyncOnClose
	if willSync {
		switch viper.GetString("file.sync_on") {
		case "write", "io":
			syncWhen = SyncOnWrite
			logger.Infof("sync after every write")
		default:
			logger.Infof("sync on file close")
		}
	}

	for _, path := range paths {
		for i := 0; i < runnersPerPath; i++ {
			bs, err := NewFileObjectStore(path)

			if err != nil {
				return nil, fmt.Errorf("cannot init store: %s", err)
			}

			r, err := NewRunner(bs, int64(iosize), syncWhen)

			if err != nil {
				return nil, fmt.Errorf("error initializing runner: %s", err)
			}

			go r.Run()

			runners = append(runners, r)
		}
	}

	return runners, nil
}
