package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Job is what flows INTO workers: a file path to process
type Job struct {
	FilePath string
}

// Result is what flows OUT workers: the counted data
type Result struct {
	FilePath  string
	WordCount int
	LineCount int
	Duration  time.Duration
	Err       error // carry errors in your results - never panic in a worker
}

func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		// here "range jobs" block waiting for the next job.
		// when the channel is CLOSED and empty, it exits automatically.
		// This is the clean shutdown pattern.

		start := time.Now()
		wordCount, lineCount, err := count(job.FilePath)

		fmt.Println("processed by", id)

		results <- Result{
			FilePath:  job.FilePath,
			WordCount: wordCount,
			LineCount: lineCount,
			Duration:  time.Since(start),
			Err:       err,
		}
	}
}

// count lines and words
func count(path string) (wordCount int, lineCount int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	wordCount = 0
	lineCount = 0
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords) // split on whitespaces, not line
	for scanner.Scan() {
		wordCount++
	}

	// Reset file reading to beginnig
	_, err = f.Seek(0, 0)
	if err != nil {
		return 0, 0, err
	}

	scanner = bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines) // split on lines
	for scanner.Scan() {
		lineCount++
	}

	return wordCount, lineCount, scanner.Err()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: wordcount <file1> <file2> ....")
		os.Exit(1)
	}
	files := os.Args[2:]

	workers := flag.Int("workers", 3, "number of concurrent workers")
	flag.Parse()
	numWorkers := *workers

	// numWorkers := 3							// tinker: fixed worker pool size
	// jobs := make(chan Job, 4)				// tinker: output not predictable as buffer will be full initially as results are consumed new jobs will be added
	// results := make(chan Result, 4)			// tinker: fixed size channel
	jobs := make(chan Job, len(files))       //buffered: we can load all jobs upfront - buffer size based on input size
	results := make(chan Result, len(files)) // buffered: collect all results

	var wg sync.WaitGroup

	for i := range numWorkers {
		wg.Add(1)
		go worker(i+1, jobs, results, &wg)
	}

	// send all jobs - non-blocking because channel is buffered
	for _, f := range files {
		jobs <- Job{FilePath: f}
	}
	close(jobs) // CRITICAL: closing signal to workers that no more jobs are coming.
	// Without this, workers block forever on "range jobs"

	// Wait in a separate goroutine so main can keep consuming results.
	// Here buffer is of same size as input but if buffer was small it could reach deadlock
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var total int
	fmt.Printf("\n%-30s %10s %10s %10s\n", "File", "Words", "Line", "Time")
	fmt.Println(strings.Repeat("-", 54))

	for result := range results { // here "range results" exits when channel is closed
		if result.Err != nil {
			fmt.Printf("%-30s   ERROR: %v\n", result.FilePath, result.Err)
			continue
		}

		total += result.WordCount
		fmt.Printf("%-30s %10d %10d %9.2fms\n",
			result.FilePath,
			result.WordCount,
			result.LineCount,
			float64(result.Duration.Microseconds())/1000,
		)
	}
	fmt.Println(strings.Repeat("-", 54))
	fmt.Printf("%-30s %10d\n", "TOTAL", total)
}
