package main

import (
	"context"
	"go.uber.org/zap"
	"sync"
)

type RunnerList struct {
	*zap.SugaredLogger
	runners     []Runner
	stores      []ObjectStore
	setupCmd    string
	teardownCmd string
	stop        func()
}

func NewRunnerList(setupCmd, teardownCmd string) *RunnerList {
	return &RunnerList{
		SugaredLogger: Logger(),
		runners:       make([]Runner, 0),
		stores:        make([]ObjectStore, 0),
		setupCmd:      setupCmd,
		teardownCmd:   teardownCmd,
	}
}

func (rl *RunnerList) AddRunner(r Runner) {
	rl.runners = append(rl.runners, r)
}

func (rl *RunnerList) AddStore(s ObjectStore) {
	rl.stores = append(rl.stores, s)
}

func (rl *RunnerList) Start() error {
	if len(rl.setupCmd) > 0 {
		rl.Infof("running: %s", rl.setupCmd)
		if out, e := RunCmd(rl.setupCmd); e != nil {
			rl.Errorf("setup: %s", out)
			return e
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	rl.stop = func() {
		cancel()
		wg.Wait()
	}

	for _, runner := range rl.runners {
		wg.Add(1)
		go func(r Runner) {
			r.Run(ctx)
			wg.Done()
		}(runner)
	}

	rl.Infof("all runners started")
	return nil
}

func (rl *RunnerList) Stop() {
	if rl.stop != nil {
		rl.stop()
		rl.stop = nil
		rl.Infof("stopped")
	}

	if len(rl.teardownCmd) > 0 {
		rl.Infof("running: %s", rl.teardownCmd)

		if out, e := RunCmd(rl.teardownCmd); e != nil {
			rl.Errorf("teardown: %s", out)
		}
	}
}
