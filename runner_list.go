package main

import (
	"context"
	"go.uber.org/zap"
	"sync"
)

type RunnerList struct {
	*zap.SugaredLogger
	runners []*Runner
	stores  []ObjectStore
	stop    func()
}

func NewRunnerList() *RunnerList {
	return &RunnerList{
		SugaredLogger: Logger(),
		runners:       make([]*Runner, 0),
		stores:        make([]ObjectStore, 0),
	}
}

func (rl *RunnerList) AddRunner(r *Runner) {
	rl.runners = append(rl.runners, r)
}

func (rl *RunnerList) AddStore(s ObjectStore) {
	rl.stores = append(rl.stores, s)
}

func (rl *RunnerList) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	rl.stop = func() {
		cancel()
		wg.Wait()
	}

	for _, runner := range rl.runners {
		wg.Add(1)
		go func() {
			runner.Run(ctx)
			wg.Done()
		}()
	}

	rl.Infof("all runners started")
}

func (rl *RunnerList) Stop() {
	if rl.stop != nil {
		rl.stop()
		rl.stop = nil
		rl.Infof("stopped")
	}

	rl.Infof("cleaning up")

	for _, store := range rl.stores {
		err := store.Cleanup()
		if err != nil {
			rl.Errorf("cleanup: %s", err)
		}
	}
}
