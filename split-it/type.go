package main

type Person string

type Expense struct {
	Payer       Person
	Amount      int64
	SplitAmong  []Person
	Description string
}

// — net balance per person
// — positive means they are owed money
// — negative means they owe money
type DebtMap map[Person]int64

type Settlement struct {
	From   Person
	To     Person
	Amount int64
}
