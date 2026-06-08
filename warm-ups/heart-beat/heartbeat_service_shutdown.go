package main

// Practice #1 — Heartbeat Service Shutdown

import (
	"fmt"
	"sync"
	"time"
)

func healthStatus(wg *sync.WaitGroup, stopCh chan struct{}) {
	defer wg.Done()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			fmt.Println("Stopping health check...")
			return
		case <-ticker.C:
			fmt.Println("Service is alive!")
		}
	}
}

func main() {
	var wg sync.WaitGroup

	stopCh := make(chan struct{})

	wg.Add(1)
	go healthStatus(&wg, stopCh)

	time.Sleep(3 * time.Second)

	close(stopCh)
	wg.Wait()
}
