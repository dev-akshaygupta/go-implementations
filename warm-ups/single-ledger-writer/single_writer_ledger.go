package main

import (
	"fmt"
	"math/rand"
	"sync"
)

type Ledger struct {
	balance int64
}

type Transaction struct {
	Id     int8
	amount int64
}

func submitTransaction(wg *sync.WaitGroup, trnsCh chan Transaction, trn Transaction) {
	defer wg.Done()
	trnsCh <- trn
}

func ledgerWorker(wg *sync.WaitGroup, trnsCh chan Transaction, ledgerBal *Ledger) {
	defer wg.Done()
	for trn := range trnsCh {
		ledgerBal.balance += trn.amount
		fmt.Printf("TID: %-2d | Amount: %-4d | Balance: %-5d\n", trn.Id, trn.amount, ledgerBal.balance)
	}
}

func main() {
	ledger := Ledger{}

	trnsCh := make(chan Transaction)

	var twg sync.WaitGroup
	var lwg sync.WaitGroup

	lwg.Add(1)
	go ledgerWorker(&lwg, trnsCh, &ledger)
	count := 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 3; j++ {
			trns := Transaction{Id: int8(count), amount: rand.Int63n(491) + 10}
			twg.Add(1)
			go submitTransaction(&twg, trnsCh, trns)
			count++
		}
	}
	twg.Wait()
	close(trnsCh)
	lwg.Wait()

	fmt.Println("\nFinal Ledger balance is: ", ledger.balance)
}
