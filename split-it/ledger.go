package main

func RunLedger(in <-chan Expense) <-chan DebtMap {
	out := make(chan DebtMap)
	go func() {
		defer close(out)
		balances := make(DebtMap)
		for expense := range in {
			sharePerPerson := (expense.Amount) / int64(len(expense.SplitAmong))
			for _, person := range expense.SplitAmong {
				if person == expense.Payer {
					balances[person] += expense.Amount - sharePerPerson
				} else {
					balances[person] -= sharePerPerson
				}
			}
		}
		out <- balances
	}()
	return out
}
