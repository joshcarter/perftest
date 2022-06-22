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
	BlockVendor   *BlockVendor
	Reporter      *Reporter
	RunnerInitFns []runnerInitFn
	RunnerError   chan error
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
	viper.SetDefault("bssplit", "4MB/100/dat")
	viper.SetDefault("reporter.interval", "1s")
	viper.SetDefault("compressibility", "50")

	if err := viper.ReadInConfig(); err != nil {
		logger.Errf("error reading config file: %s\n", err)
		os.Exit(-1)
	}

	iosize := viper.GetSizeInBytes("iosize")

	if iosize == 0 {
		logger.Errf("no io size specified; create 'iosize' in config.json")
		os.Exit(-1)
	}

	bssplit := viper.GetString("bssplit")

	if len(bssplit) == 0 {
		logger.Errf("no block size specified; create 'bssplit' in config.json")
		os.Exit(-1)
	}

	compressibility := viper.GetInt("compressibility")

	global.BlockVendor, err = NewBlockVendor(bssplit, compressibility)

	if err != nil {
		logger.Errf("cannot create block vendor: %s", err)
		os.Exit(-1)
	}

	reporterInterval := viper.GetDuration("reporter.interval")
	if reporterInterval == 0 {
		logger.Errf("no reporter interval specified; create 'reporter.interval' in config.json")
		os.Exit(-1)
	}

	reporterConfig := &ReporterConfig{
		Interval:         reporterInterval,
		LatencyEnabled:   viper.GetBool("reporter.loglatency"),
		BandwidthEnabled: viper.GetBool("reporter.logbandwidth"),
	}

	global.Reporter, err = NewReporter(reporterConfig)

	if err != nil {
		logger.Errf("failed creating reporter: %s", err)
	}

	runners := make([]*Runner, 0)

	for _, fn := range global.RunnerInitFns {
		runners, err = fn(runners)

		if err != nil {
			logger.Errf(err.Error())
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

		case err := <-global.RunnerError:
			logger.Errf("runner error: %s", err)
			goto stop
		}
	}

stop:

	for _, r := range runners {
		r.Stop()
	}

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

	for _, path := range paths {
		for i := 0; i < runnersPerPath; i++ {
			bs, err := NewFileBlockStore(path)

			if err != nil {
				return nil, fmt.Errorf("cannot init store: %s", err)
			}

			r, err := NewRunner(bs, int64(iosize))

			if err != nil {
				return nil, fmt.Errorf("error initializing runner: %s", err)
			}

			go r.Run()

			runners = append(runners, r)
		}
	}

	return runners, nil
}
