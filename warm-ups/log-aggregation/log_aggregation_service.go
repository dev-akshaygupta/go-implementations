package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

func producer(id string, wg *sync.WaitGroup, msgs chan string) {
	defer wg.Done()
	for range 5 {
		msgs <- "hello from " + id
		time.Sleep(50 * time.Millisecond)
	}
}

func consumer(wg *sync.WaitGroup, msgs chan string) {
	defer wg.Done()
	for msg := range msgs {
		fmt.Println(msg)
	}
}

func main() {
	msgs := make(chan string)
	var cwg sync.WaitGroup
	var pwg sync.WaitGroup

	cwg.Add(1)
	go consumer(&cwg, msgs)

	for i := range 3 {
		pwg.Add(1)
		go producer("P-"+strconv.Itoa(i), &pwg, msgs)
	}
	pwg.Wait()
	close(msgs)
	cwg.Wait()
}
