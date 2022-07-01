package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func run(ctx context.Context, name string, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("%s done\n", name)
			wg.Done()
			return
		}
	}
}

func TestContext_CancelOrder(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	child1, child1cancel := context.WithCancel(parent)
	subchild1, _ := context.WithCancel(child1)
	child2, _ := context.WithCancel(parent)
	subchild2, _ := context.WithCancel(child2)
	child3, _ := context.WithCancel(parent)
	var wg sync.WaitGroup

	wg.Add(6)
	go run(parent, "parent", &wg)
	go run(child1, "child1", &wg)
	go run(subchild1, "subchild1", &wg)
	go run(child2, "child2", &wg)
	go run(subchild2, "subchild2", &wg)
	go run(child3, "child3", &wg)

	time.Sleep(time.Second)

	fmt.Println("cancelling child1")
	child1cancel()
	time.Sleep(time.Second)

	fmt.Println("cancelling parent")
	cancel()
	wg.Wait()
}
