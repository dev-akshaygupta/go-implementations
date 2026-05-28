package main

import (
	"fmt"
	"sync"
)

func main() {
	// create an unbuffered channel of type int
	channel := make(chan int)
	// fmt.Println("Channel created:", channel)

	// wait groups - to sync go routines
	var wg sync.WaitGroup
	wg.Add(2) // wait for 2 goroutines

	// launch goroutine
	// Goroutine A: sends first, then receives and sends again
	go func(name string, ch chan int) {
		defer wg.Done() // decrement when this goroutine finishes

		for i := 0; i < 100; i++ {
			val, ok := <-ch // receive from channel
			if !ok {
				fmt.Printf("%s exiting\n", name)
				return
			}
			fmt.Printf("%s recevied: %d\n", name, val)

			if val >= 100 {
				close(ch)
				return
			}

			val++     // increment
			ch <- val // send back
		}
		fmt.Printf("%s finished\n", name)
	}("A", channel)

	// Goroutine B: receives first, then alternates
	go func(name string, ch chan int) {
		defer wg.Done() // decrement when this goroutine finishes

		for i := 0; i < 100; i++ {
			val, ok := <-ch // receive from channel
			if !ok {
				fmt.Printf("%s exiting\n", name)
				return
			}
			fmt.Printf("%s recevied: %d\n", name, val)

			if val >= 100 {
				close(ch)
				return
			}

			val++     // increment
			ch <- val // send back
		}
		fmt.Printf("%s finished\n", name)
	}("B", channel)

	// main goroutine: starts the game
	channel <- 1

	// blocks until both goroutines call Done()
	wg.Wait()
	fmt.Println("Main: All done!")
}
