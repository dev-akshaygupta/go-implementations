package main

import (
	"sync"
)

func SubmitExpenses(expenses []Expense, out chan<- Expense) {
	var wg sync.WaitGroup
	for _, expense := range expenses {
		wg.Add(1)
		go func(expense Expense) {
			defer wg.Done()

			// r := rand.New(rand.NewSource(time.Now().UnixNano()))
			// time.Sleep(time.Duration(r.Intn(21) * int(time.Millisecond)))

			out <- expense
		}(expense)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
}
