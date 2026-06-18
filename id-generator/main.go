package main

import (
	"fmt"
	"sync"
)

func main() {
	idGen := IDGenerator{
		machineId: 7,
	}

	ids := make(chan int64, 100_000)

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			for range 1000 {
				ids <- idGen.NextID()
			}
		})
	}

	wg.Wait()
	close(ids)

	seen := make(map[int64]struct{})
	duplicates := 0

	for id := range ids {
		if _, exists := seen[id]; exists {
			duplicates++
		} else {
			seen[id] = struct{}{}
		}
	}

	fmt.Println("Generated:", len(seen))
	fmt.Println("Duplicates:", duplicates)
}
