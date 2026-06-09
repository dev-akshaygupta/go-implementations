package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

func reader(filePath string) (<-chan []string, <-chan string, chan struct{}) {
	rows := make(chan []string)  // channel for each row as slice of string
	errs := make(chan string, 1) // error channel
	done := make(chan struct{})

	go func() {
		defer close(rows)
		defer close(done)
		defer close(errs)

		// open csv file
		f, err := os.Open(filePath)
		if err != nil {
			fmt.Println("Unable to find csv file - ", err)
			errs <- err.Error()
			return
		}
		defer f.Close()

		// read csv file
		reader := csv.NewReader(f)
		reader.TrimLeadingSpace = true

		// skip header row
		if _, err := reader.Read(); err != nil {
			fmt.Println("Unable to read csv file - ", err)
			errs <- err.Error()
			return
		}
		// read each line
		for {
			row, err := reader.Read()
			if err == io.EOF {
				return // Normal end of file -— close(rows) fires via defer
			}
			if err != nil {
				fmt.Println("Unable to read csv file - ", err)
				errs <- err.Error()
				return
			}
			rows <- row
		}
	}()
	return rows, errs, done
}

func validator(rows <-chan []string, done chan struct{}) (<-chan []string, <-chan string) {
	vrows := make(chan []string)
	errs := make(chan string, 1)

	go func() {
		defer close(vrows)
		defer close(errs)

		for {
			select {
			case row, ok := <-rows:
				if !ok {
					return
				}

				valid := false

				for _, col := range row {
					if strings.TrimSpace(col) != "" {
						valid = true
						break
					}
				}

				if !valid {
					select {
					case errs <- "Empty row found":
					default:
					}
					continue
				}

				vrows <- row

			case <-done:
				return
			}
		}
	}()
	return vrows, errs
}

func transformer(vrows <-chan []string, done <-chan struct{}) {
	line := strings.Repeat("━", 40)

	for {
		select {
		case row, ok := <-vrows:
			if !ok {
				return
			}

			for _, col := range row {
				fmt.Print(strings.ToUpper(col), "\t")
			}
			fmt.Println()
			fmt.Println(line)
		case <-done:
			return
		}
	}
}

func main() {
	rows, readErrs, done := reader("employee.csv")
	vrows, validationErrs := validator(rows, done)

	go func() {
		for err := range readErrs {
			fmt.Println("READER ERROR:", err)
		}
	}()

	go func() {
		for err := range validationErrs {
			fmt.Println("VALIDATION ERROR:", err)
		}
	}()

	transformer(vrows, done)
}
