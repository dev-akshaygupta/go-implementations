package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func worker(ctx context.Context, name string, ch chan int, wg *sync.WaitGroup, start time.Time) {
	defer wg.Done() // decrement when this goroutine finishes

	for i := 0; i < 100; i++ {
		fmt.Printf("[T=%3dms] %s: waiting to receive...\n", time.Since(start).Milliseconds(), name)
		select {
		case val := <-ch: // receive from channel
			select {
			case ch <- val + 1:
				fmt.Printf("[T=%3dms] %s: got %d, sending %d\n", time.Since(start).Milliseconds(), name, val, val+1)
			case <-ctx.Done():
				fmt.Printf("[T=%3dms] %s: cancelled during sending\n", time.Since(start).Milliseconds(), name)
				return
			}
		case <-ctx.Done():
			fmt.Printf("[T=%3dms] %s: context cancelled\n", time.Since(start).Milliseconds(), name)
			return
		}

	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// create an unbuffered channel of type int
	channel := make(chan int)

	start := time.Now()

	// wait groups - to sync go routines
	var wg sync.WaitGroup
	wg.Add(2) // wait for 2 goroutines
	go worker(ctx, "A", channel, &wg, start)
	go worker(ctx, "B", channel, &wg, start)

	// main goroutine: starts the game
	fmt.Printf("[T=%3dms]main: sending 1\n", time.Since(start).Milliseconds())
	channel <- 1

	// blocks until both goroutines call Done()
	wg.Wait()
	fmt.Printf("[T=%3dms]main: done!\n", time.Since(start).Milliseconds())
}
