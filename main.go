package main

import (
	"fmt"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
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
	Subdirs       int // each runner will have this many subdirs
	ReadPercent   int // range 0-100
}

var global = &Globals{
	RunnerInitFns: []runnerInitFn{},
	RunnerError:   make(chan error, 10),
}

func init() {
	global.RunId = time.Now().Format("2006-01-02-15-04-05")
	global.RunnerInitFns = append(global.RunnerInitFns, startFileRunners)
}

func main() {
	var err error

	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetDefault("iosize", "1MB")
	viper.SetDefault("size", "4MB/100/dat")
	viper.SetDefault("reporter.maxWait", "1s")
	viper.SetDefault("compressibility", "50")
	viper.SetDefault("subdirs", "0")
	viper.SetDefault("read", "0")

	if err = viper.ReadInConfig(); err != nil {
		fmt.Printf("error reading config file: %s\n", err)
		os.Exit(-1)
	}

	runId := viper.GetString("runid")

	if len(runId) > 0 {
		global.RunId = runId
	}

	if err = os.MkdirAll(global.RunId, 0750); err != nil {
		fmt.Printf("cannot make output directory: %s\n", err)
		os.Exit(-1)
	}

	logger := Logger()
	logger.Infof("starting run %s", global.RunId)

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
	global.Subdirs = viper.GetInt("subdirs")
	global.ReadPercent = viper.GetInt("read")

	if global.ReadPercent < 0 || global.ReadPercent > 100 {
		logger.Errorf("read percent must be between 0 and 100")
		os.Exit(-1)
	}

	logger.Infof("read percent: %d", global.ReadPercent)

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
		WarmUp:           viper.GetDuration("reporter.warmup"),
		LatencyEnabled:   viper.GetBool("reporter.loglatency"),
		BandwidthEnabled: viper.GetBool("reporter.logbandwidth"),
		Capture:          viper.GetStringMapString("reporter.capture"),
	}

	global.Reporter, err = NewReporter(reporterConfig)

	if err != nil {
		logger.Errorf("failed creating reporter: %s", err)
		os.Exit(-1)
	}

	runners := NewRunnerList(viper.GetString("file.setup"), viper.GetString("file.teardown"))

	for _, fn := range global.RunnerInitFns {
		err = fn(runners)

		if err != nil {
			logger.Errorf(err.Error())
			os.Exit(-1)
		}
	}

	if err = runners.Start(); err != nil {
		logger.Errorf(err.Error())
		os.Exit(-1)
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

	global.Reporter.PreStop() // stops further logging
	runners.Stop()
	global.Syncer.Stop()
	global.Reporter.Stop()
	logger.Infof("finished run %s", global.RunId)
	os.Exit(0)
}

func startFileRunners(rl *RunnerList) (err error) {
	logger := Logger()
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

		global.Syncer = NewSyncBatcher(syncBatcherMaxWait, syncBatcherMaxPending)
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

	for i, path := range paths {
		var o ObjectStore

		if o, err = NewFileObjectStore(path, openFlags); err != nil {
			return fmt.Errorf("cannot init store: %s", err)
		}

		logger.Infof("file object store: %s", path)

		rl.AddStore(o)

		for j := 0; j < runnersPerPath; j++ {
			var r *Runner

			if r, err = NewRunner(o, (i*runnersPerPath)+j+1); err != nil {
				return fmt.Errorf("error initializing runner: %s", err)
			}

			rl.AddRunner(r)
		}
	}

	return nil
}

var __logger *zap.Logger
var __logLevel zap.AtomicLevel
var __loggerOnce sync.Once

func Logger() *zap.SugaredLogger {
	__loggerOnce.Do(func() {
		var err error

		__logLevel = zap.NewAtomicLevel()
		__logLevel.SetLevel(zap.InfoLevel)

		// config := zap.NewDevelopmentConfig()
		// config.Level = __logLevel
		// config.OutputPaths = []string{"stdout", filepath.Join(global.RunId, "log.txt")}
		//
		// __logger, err = config.Build()

		cfg := zap.Config{
			Encoding:    "console",
			Level:       __logLevel,
			OutputPaths: []string{"stdout", filepath.Join(global.RunId, "log.txt")},
			EncoderConfig: zapcore.EncoderConfig{
				MessageKey: "message",

				LevelKey:    "level",
				EncodeLevel: zapcore.CapitalLevelEncoder,

				TimeKey:    "time",
				EncodeTime: zapcore.TimeEncoderOfLayout("15:04:05"),
			},
		}
		__logger, err = cfg.Build()

		if err != nil {
			panic(err)
		}
	})

	return __logger.Sugar()
}
