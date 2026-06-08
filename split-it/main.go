package main

import (
	"fmt"
)

func main() {
	// persons := []Person{
	// 	"Raj",
	// 	"Bob",
	// 	"Alice",
	// 	"Priya",
	// }

	allExpenses := []Expense{
		{
			Payer:  "Raj",
			Amount: 10000,
			SplitAmong: []Person{
				"Raj",
				"Alice",
				"Bob",
				"Priya",
			},
			Description: "Food",
		},
		{
			Payer:  "Bob",
			Amount: 30000,
			SplitAmong: []Person{
				"Alice",
				"Bob",
				"Priya",
			},
			Description: "Ride",
		},
	}

	expenses := make(chan Expense, len(allExpenses))

	SubmitExpenses(allExpenses, expenses)

	debtCh := RunLedger(expenses)
	debts := <-debtCh

	settlements := Settle(debts)

	line := "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

	fmt.Println(line)
	fmt.Printf(" Trip Settlement — %d people, %d expenses\n",
		len(settlements), len(allExpenses))
	fmt.Println(line)

	fmt.Println(" Expenses logged:")
	for _, e := range allExpenses {
		fmt.Printf("   %-6s paid $%7.2f  %-12s (split %d ways)\n",
			e.Payer,
			float64(e.Amount)/100,
			e.Description,
			len(e.SplitAmong),
		)
	}

	fmt.Println()
	fmt.Println(" Net balances:")

	var remaining int64
	for person, amount := range debts {
		if amount > 0 {
			fmt.Printf("   %-6s is owed  $%7.2f\n",
				person,
				float64(amount)/100)
		} else if amount < 0 {
			fmt.Printf("   %-6s owes     $%7.2f\n",
				person,
				float64(-amount)/100)
		}
		remaining += amount
	}

	fmt.Println()
	fmt.Printf(" Settle up (%d transfers):\n", len(settlements))

	for _, s := range settlements {
		fmt.Printf("   %-6s → %-6s $%7.2f\n",
			s.From,
			s.To,
			float64(s.Amount)/100)
	}

	fmt.Println(line)
	fmt.Printf(" Total verified: $%.2f remaining\n",
		float64(remaining)/100)
	fmt.Println(line)
}
