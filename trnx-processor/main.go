package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"text/tabwriter"
	"time"
)

type TxnType string

const (
	Credit   TxnType = "CREDIT"
	Debit    TxnType = "DEBIT"
	Transfer TxnType = "TRANSFER"
)

// Transaction is the raw input — what arrives from the outside world
type Transaction struct {
	ID          string
	AccountID   string
	Type        TxnType
	AmountCents int64
	Timestamp   time.Time
	Description string
}

// ValidationResult carries a transaction through the validation layer.
// It's either approved (Approved=true) or rejected with a Reason.
type ValidationResults struct {
	Txn       Transaction
	Approved  bool
	Reason    string // empty if approved
	CheckedBy string // which validator approved/rejected
}

// AccountState is the source of truth for one account.
// For now only the ledger goroutine writes to this — no locks needed.
type AccountState struct {
	AccountID        string
	BalanceCents     int64
	DailySpendsCents int64
	TxnCount         int
	Rejected         int
}

type Config struct {
	InitialBalanceCents int64
	DailyLimitCents     int64
	PerTxnLimitCents    int64
	MaxVelocityPerHour  int // max transactions per hour before fraud flag
}

func generateTransaction(accountID string, count int) []Transaction {
	txns := make([]Transaction, count)
	descriptions := []string{"Grocery store", "Netflix subscription", "Salary deposit",
		"ATM withdrawal", "Coffee shop", "Online transfer",
		"Insurance premium", "Rent payment", "Freelance payment",
	}

	for i := range txns {
		txType := Debit
		amount := int64(rand.Intn(50000) + 100) // $0.01 to $500

		if i%5 == 0 {
			txType = Credit
			amount = int64(rand.Intn(500000) + 100000) // bigger credits
		}

		// Occasionally inject a suspicious large transaction
		if i%7 == 0 {
			amount = int64(rand.Intn(200000) + 500000) // occasinally over $5000
		}
		txns[i] = Transaction{
			ID:          fmt.Sprintf("TXN-%04d", i+1),
			AccountID:   accountID,
			Type:        txType,
			AmountCents: amount,
			Timestamp:   time.Now().Add(-time.Duration(rand.Intn(3600)) * time.Second),
			Description: descriptions[rand.Intn(len(descriptions))],
		}
	}
	return txns
}

// ingest() takes a slice of transactions and emits them on a channel.
// This is the pipeline source — it owns and closes the output channel.
func ingest(txns []Transaction) <-chan Transaction {
	out := make(chan Transaction, len(txns))
	go func() {
		defer close(out)
		for _, t := range txns {
			out <- t
		}
	}()
	return out
}

// fraudCheck flags transactions that look like velocity abuse
func fraudCheck(txn Transaction, recentCount int, cfg Config) ValidationResults {
	// Simulate a small processing delay (real fraud checks hit ML models)
	time.Sleep(time.Duration(rand.Intn(5) * int(time.Millisecond)))

	if recentCount >= cfg.MaxVelocityPerHour {
		return ValidationResults{
			Txn: txn, Approved: false, CheckedBy: "fraud",
			Reason: fmt.Sprintf("velocity limit: %d txns/hr exceeded", cfg.MaxVelocityPerHour),
		}
	}
	return ValidationResults{Txn: txn, Approved: true, CheckedBy: "fraud"}
}

// balanceCheck ensures sufficient funds for debits
func balanceCheck(txn Transaction, balanceCents int64) ValidationResults {
	if txn.Type == Debit && txn.AmountCents > balanceCents {
		return ValidationResults{
			Txn: txn, Approved: false, CheckedBy: "balance",
			Reason: fmt.Sprintf("insufficient funds: need %d have %d cents", txn.AmountCents, balanceCents),
		}
	}
	return ValidationResults{Txn: txn, Approved: true, CheckedBy: "balance"}
}

// limitsCheck enforces per-transaction and daily spend caps
func limitsCheck(txn Transaction, dailySpendsCents int64, cfg Config) ValidationResults {
	if txn.AmountCents > cfg.PerTxnLimitCents {
		return ValidationResults{
			Txn: txn, Approved: false, CheckedBy: "limits",
			Reason: fmt.Sprintf("exceeds per-txn limit: %d > %d cents", txn.AmountCents, cfg.PerTxnLimitCents),
		}
	}

	if txn.Type == Debit && dailySpendsCents+txn.AmountCents > cfg.DailyLimitCents {
		return ValidationResults{
			Txn: txn, Approved: false, CheckedBy: "limit",
			Reason: fmt.Sprintf("exceeds daily limit: %d + %d > %d cents", dailySpendsCents, txn.AmountCents, cfg.DailyLimitCents),
		}
	}
	return ValidationResults{Txn: txn, Approved: true, CheckedBy: "limits"}
}

// validate fans out to 3 concurrent validators.
// Each transaction is checked by whichever validator picks it up first.
// Failed transactions are routed to the audit channel.
func validate(txns <-chan Transaction, state *AccountState, cfg Config) (<-chan ValidationResults, <-chan ValidationResults) {
	approved := make(chan ValidationResults, 50)
	rejected := make(chan ValidationResults, 50)

	var wg sync.WaitGroup

	// Run 3 validators concurrently, all reading from the same txns channel
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			for txn := range txns {
				checks := []ValidationResults{
					fraudCheck(txn, state.TxnCount, cfg),
					balanceCheck(txn, state.BalanceCents),
					limitsCheck(txn, state.DailySpendsCents, cfg),
				}

				failed := false
				for _, result := range checks {
					if !result.Approved {
						rejected <- result
						failed = true
						break
					}
				}

				if !failed {
					approved <- ValidationResults{
						Txn: txn, Approved: true, CheckedBy: "all",
					}
				}
			}
		}(i)
	}

	// Close both output channels when all validators are done
	go func() {
		wg.Wait()
		close(approved)
		close(rejected)
	}()
	return approved, rejected
}

// ledger receives approved transactions,
// applies them to the account, and emits a final statement when done
func ledger(approved <-chan ValidationResults, initial AccountState) <-chan AccountState {
	done := make(chan AccountState, 1)

	go func() {
		state := initial // local copy — only this goroutine touches it

		for result := range approved {
			txn := result.Txn
			switch txn.Type {
			case Credit:
				state.BalanceCents += txn.AmountCents
			case Debit:
				state.BalanceCents -= txn.AmountCents
				state.DailySpendsCents += txn.AmountCents
			case Transfer:
				state.BalanceCents -= txn.AmountCents
				state.DailySpendsCents += txn.AmountCents
			}
			state.TxnCount++
		}
		done <- state // emit final state when all approved txns are processed
		close(done)
	}()
	return done
}

func main() {
	rand.New(rand.NewSource(time.Now().Unix()))

	cfg := Config{
		InitialBalanceCents: 1_000_000, // $10,000 starting balance
		DailyLimitCents:     500_000,   // $5,000 daily spend limt
		PerTxnLimitCents:    300_000,   // $3,000 per-transaction limit
		MaxVelocityPerHour:  10,        // max 10 txns/hr before fraud flag
	}

	initialState := AccountState{
		AccountID:    "DEP001",
		BalanceCents: cfg.InitialBalanceCents,
	}

	txns := generateTransaction(initialState.AccountID, 30)

	fmt.Printf("Processing %d transactions for %s\n", len(txns), initialState.AccountID)
	fmt.Printf("Starting balance: $%.2f\n\n", float64(cfg.InitialBalanceCents)/100)

	start := time.Now()

	// Pipeline: ingest → validate (fan-out) → ledger (sink)
	txnStream := ingest(txns)
	approved, rejected := validate(txnStream, &initialState, cfg)
	finalState := ledger(approved, initialState)

	// Drain the audit log concurrently while ledger processes approvals
	var auditLog []ValidationResults
	var auditWg sync.WaitGroup
	auditWg.Add(1)
	go func() {
		defer auditWg.Done()

		for r := range rejected {
			auditLog = append(auditLog, r)
		}
	}()

	// block until ledger is done
	statement := <-finalState
	auditWg.Wait() // ensure all rejected txns are captured

	elapsed := time.Since(start)

	// print statement
	printStatement(statement, auditLog, txns, elapsed, cfg)
}

func printStatement(state AccountState, rejected []ValidationResults, allTxns []Transaction, elapsed time.Duration, cfg Config) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Fprintf(w, "  Account Statement: %s\n", state.AccountID)
	fmt.Fprintln(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Fprintf(w, "  Opening balance:\t$%8.2f\n", float64(cfg.InitialBalanceCents)/100)
	fmt.Fprintf(w, "  Closing balance:\t$%8.2f\n", float64(state.BalanceCents)/100)
	fmt.Fprintf(w, "  Daily spend:\t$%8.2f / $%.2f\n",
		float64(state.DailySpendsCents)/100,
		float64(cfg.DailyLimitCents)/100)
	fmt.Fprintln(w, "──────────────────────────────────────────────────────")
	fmt.Fprintf(w, "  Total submitted:\t%d txns\n", len(allTxns))
	fmt.Fprintf(w, "  Approved:\t%d txns\n", state.TxnCount)
	fmt.Fprintf(w, "  Rejected:\t%d txns\n", len(rejected))
	fmt.Fprintf(w, "  Processed in:\t%v\n", elapsed.Round(time.Microsecond))
	fmt.Fprintln(w, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if len(rejected) > 0 {
		fmt.Fprintln(w, "\n  Rejected transactions (audit log):")
		fmt.Fprintln(w, "  ID\t\tAmount\t\tReason")
		fmt.Fprintln(w, "  ──\t\t──────\t\t──────")
		for _, r := range rejected {
			fmt.Fprintf(w, "  %s\t$%7.2f\t%s\n",
				r.Txn.ID,
				float64(r.Txn.AmountCents)/100,
				r.Reason)
		}
	}
	w.Flush()
}
