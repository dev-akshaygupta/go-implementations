package main

import (
	"sort"
)

func Settle(debts DebtMap) []Settlement {
	var creditors []Person
	var debtors []Person
	var settlements []Settlement

	for person, amount := range debts {
		if amount > 0 {
			creditors = append(creditors, person)
		} else if amount < 0 {
			debtors = append(debtors, person)
		}
	}

	sort.Slice(creditors, func(i, j int) bool {
		if debts[creditors[i]] == debts[creditors[j]] {
			return creditors[i] < creditors[j]
		}
		return debts[creditors[i]] > debts[creditors[j]]
	})

	sort.Slice(debtors, func(i, j int) bool {
		if debts[debtors[i]] == debts[debtors[j]] {
			return debtors[i] < debtors[j]
		}
		return debts[debtors[i]] < debts[debtors[j]]
	})

	c := 0
	d := 0
	for c < len(creditors) && d < len(debtors) {
		payment := min(debts[creditors[c]], -debts[debtors[d]])
		settlements = append(settlements, Settlement{
			To:     creditors[c],
			From:   debtors[d],
			Amount: payment,
		})
		debts[creditors[c]] -= payment
		debts[debtors[d]] += payment
		if debts[creditors[c]] == 0 {
			c++
		}
		if debts[debtors[d]] == 0 {
			d++
		}
	}
	return settlements
}
