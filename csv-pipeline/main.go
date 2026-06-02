package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Employee record
type Record struct {
	Name       string
	Department string
	Salary     int
	Joined     time.Time
	Email      string
	YearsExp   int
}

// ParseError carries the row content plus the reason it was rejected.
// You never panic in a pipeline — you emit errors on a separate channel.
type ParseError struct {
	Line   int
	Raw    []string
	Reason string
}

func (e ParseError) Error() string {
	return fmt.Sprintf("line: %d: %s (raw: %v)", e.Line, e.Reason, e.Raw)
}

// stage 1
// readCSV opens the file, skips the header, and emits each row as []string.
// It owns the output channel and closes it when the file is exhausted.
func readCSV(fileName string) (<-chan []string, <-chan error) {
	rows := make(chan []string)
	errs := make(chan error, 1) // buffered: holds at most one fatal error

	go func() {
		defer close(rows)
		defer close(errs)

		f, err := os.Open(fileName)
		if err != nil {
			errs <- fmt.Errorf("open: %w", err)
			return
		}
		defer f.Close()

		reader := csv.NewReader(f)
		reader.TrimLeadingSpace = true // handles "  Engineering  " → "Engineering"

		//Skip header row
		if _, err := reader.Read(); err != nil {
			errs <- fmt.Errorf("read header: %w", err)
			return
		}

		for {
			row, err := reader.Read()
			if err == io.EOF {
				return // Normal end of file -— close(rows) fires via defer
			}
			if err != nil {
				errs <- fmt.Errorf("read row: %w", err)
				return
			}

			rows <- row
		}
	}()
	return rows, errs
}

// stage 2.a
// transformRows validates and enriches each raw row
// It returns two channels: clean records and parse errors
// The caller can listen to both simultaneously using select
func transformRows(rows <-chan []string) (<-chan Record, <-chan ParseError) {
	records := make(chan Record)
	parseErrs := make(chan ParseError, 10) // buffered: don't block the happy path

	go func() {
		defer close(records)
		defer close(parseErrs)

		lineNum := 2 // we skipped header line in stage 1
		for row := range rows {
			lineNum++
			rec, err := parseRow(lineNum, row)
			if err.Line > 0 {
				parseErrs <- err // Route bad rows to error channel
				continue         // Don't stop the pipeline
			}
			records <- rec
		}
	}()
	return records, parseErrs
}

// stage 2.b
// parseRow converts one []string into a typed Record, or a ParseError.
func parseRow(line int, row []string) (Record, ParseError) {
	zero := Record{}

	if len(row) < 5 {
		return zero, ParseError{line, row, "too few columns"}
	}

	name := strings.TrimSpace(cases.Title(language.English).String(cases.Lower(language.English).String(row[0])))
	if name == "" {
		return zero, ParseError{line, row, "name is empty"}
	}

	dept := strings.TrimSpace(cases.Title(language.English).String(cases.Lower(language.English).String(row[1])))

	salary, err := strconv.Atoi(strings.TrimSpace(row[2]))
	if err != nil {
		return zero, ParseError{line, row, fmt.Sprintf("salary not a number: %q", row[2])}
	}

	joined, err := time.Parse("2006-01-02", strings.TrimSpace(row[3]))
	if err != nil {
		return zero, ParseError{line, row, fmt.Sprintf("invalid date: %q", row[3])}
	}

	email := strings.TrimSpace(row[4])
	if email == "" {
		return zero, ParseError{line, row, "email is empty"}
	}

	yearsExp := time.Now().Year() - joined.Year()

	return Record{
		Name:       name,
		Department: dept,
		Salary:     salary,
		Joined:     joined,
		Email:      email,
		YearsExp:   yearsExp,
	}, ParseError{}
}

// stage 3
// writeResults prints clean records and returns a channel that closes when done.
func writeResults(records <-chan Record) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		w := csv.NewWriter(os.Stdout)
		defer w.Flush()

		w.Write([]string{"Name", "Department", "Salary", "Joined", "Email", "YearsExp"})

		for rec := range records {
			w.Write([]string{
				rec.Name,
				rec.Department,
				strconv.Itoa(rec.Salary),
				rec.Joined.Format("2006-01-02"),
				rec.Email,
				strconv.Itoa(rec.YearsExp),
			})
		}
	}()

	return done
}

func main() {
	fileName := "inputfiles/employees.csv"
	if len(os.Args) > 1 {
		fileName = os.Args[1]
	}

	// stage 1: read csv
	rows, readErrs := readCSV(fileName)

	// stage 2: tranform - rows channel flows directly into transformRows
	records, parseErr := transformRows(rows)

	// stage 3: write  - records channel flows direcly into writeResults
	done := writeResults(records)

	// Drain parse errors while the pipeline runs
	go func() {
		for err := range parseErr {
			fmt.Fprintf(os.Stderr, "SKIP line %d: %s\n", err.Line, err.Reason)
		}
	}()

	select {
	case err := <-readErrs:
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
			os.Exit(1)
		}
	case <-done:
		// pipeline completed normally
	}
	fmt.Fprintln(os.Stderr, "\nDone")
}
