package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Transaction struct {
	Id          int
	Amount      int64
	Description string
}

func processTransaction(wg *sync.WaitGroup, jobs chan Transaction, result chan<- string) {
	defer wg.Done()

	for job := range jobs {
		time.Sleep(200 * time.Millisecond)
		fmt.Println("Validating Transaction - ", job.Id)
		result <- strconv.Itoa(job.Id)
	}
}

func main() {
	transactions := []Transaction{
		{
			Id:          1,
			Amount:      24,
			Description: "Food",
		}, {
			Id:          2,
			Amount:      50,
			Description: "Travel",
		}, {
			Id:          3,
			Amount:      31,
			Description: "Bag",
		}, {
			Id:          4,
			Amount:      10,
			Description: "Parking",
		}, {
			Id:          5,
			Amount:      15,
			Description: "Water",
		},
		{
			Id:          6,
			Amount:      24,
			Description: "Food",
		}, {
			Id:          7,
			Amount:      50,
			Description: "Travel",
		}, {
			Id:          8,
			Amount:      31,
			Description: "Bag",
		}, {
			Id:          9,
			Amount:      10,
			Description: "Parking",
		}, {
			Id:          10,
			Amount:      15,
			Description: "Water",
		},
	}

	jobs := make(chan Transaction)                 // unbuffered
	result := make(chan string, len(transactions)) // buffered

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go processTransaction(&wg, jobs, result)
	}

	for _, tran := range transactions {
		jobs <- tran
	}
	close(jobs)
	wg.Wait()

	close(result)
	for res := range result {
		fmt.Println("Processed transaction - ", res)
	}

}

// Should work and results channels be buffered? Why? What size?
// Jobs channel can be buffered or unbuffered. Buffered reduces blocking between producer and workers.
// Results channel should be buffered in my implementation because main waits for workers before consuming results.
// A buffer large enough to hold all results prevents workers from blocking.

// How do workers know when there's no more work?
// Workers range over the jobs channel. When the producer closes the jobs channel
// and all queued jobs are drained, the range loop ends automatically and workers exit.

// How do you know when all results have arrived?
// After all workers finish, wg.Wait() returns. At that point no more results can be produced, so the results channel is closed.
// The collector ranges over the results channel and knows all results have arrived when the range loop ends.
