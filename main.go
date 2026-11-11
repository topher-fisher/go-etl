// Package main implements a concurrent CSV-to-Parquet ETL pipeline in Go.
//
// Architecture Overview:
//   - EXTRACT: Streams rows from a CSV file on disk into an unbuffered channel,
//     providing natural backpressure and constant memory usage on large files.
//   - TRANSFORM: Uses a fan-out worker pool of goroutines to validate and normalize
//     each column independently, setting invalid cells to nil (SQL/Parquet NULL)
//     while preserving valid columns, and computing a deterministic MD5 surrogate key.
//   - LOAD: Batches transformed records, sorts them deterministically by ID,
//     renders an aligned DataFrame table to stdout, and writes/overwrites a
//     compressed Apache Parquet file in the target output directory.
//
// Prerequisites:
//
//	go get ://github.com
//
// Usage Examples:
//
//  1. Quick self-contained demo (generates a dummy CSV, runs ETL, writes Parquet):
//     go run main.go -demo
//
//  2. Process a real CSV file and save to processed_files/clean_users.parquet:
//     go run main.go -file test_files/users.csv -out clean_users
//
//  3. Tune concurrency workers, batch size, and output folder:
//     go run main.go -file test_files/users.csv -dir processed_files -out users_v2 -workers 8 -batch 100
package main

import (
	"context"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/parquet-go/parquet-go"
)

// RawRecord represents an unvalidated row read directly from the source CSV.
// Fields other than ID remain raw strings so the concurrent transform workers
// can validate and null out malformed cells individually without dropping the row.
type RawRecord struct {
	LineNum int
	ID      int
	Name    string
	Email   string
	Spent   string
}

// CleanRecord defines both the in-memory DataFrame row and the output Parquet schema.
// Pointer fields (*string, *int64) allow nil values to serialize as native Parquet NULLs.
type CleanRecord struct {
	UniqueRecordID string    `parquet:"unique_record_id"`
	ID             int       `parquet:"id"`
	FullName       *string   `parquet:"full_name,optional"`
	Email          *string   `parquet:"email,optional"`
	SpentCents     *int64    `parquet:"spent_cents,optional"`
	ProcessedAt    time.Time `parquet:"processed_at,timestamp(millisecond)"`
}

// generateRecordHash computes a deterministic 32-character MD5 hex digest from
// the normalized business columns (excluding ProcessedAt).
//
// Using explicit "<NULL>" sentinels and pipe ("|") delimiters prevents hash
// collisions between empty strings, nil values, and adjacent column boundaries.
func generateRecordHash(id int, name *string, email *string, spentCents *int64) string {
	n := "<NULL>"
	if name != nil {
		n = *name
	}
	e := "<NULL>"
	if email != nil {
		e = *email
	}
	s := "<NULL>"
	if spentCents != nil {
		s = strconv.FormatInt(*spentCents, 10)
	}

	payload := fmt.Sprintf("%d|%s|%s|%s", id, n, e, s)
	sum := md5.Sum([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// extractCSV is Stage 1 (EXTRACT) of the pipeline.
// It opens the CSV file at filePath, skips the header row, validates that the primary
// key (ID) is a valid integer, and streams RawRecord structs into the returned channel.
//
// Fatal startup errors (such as missing files or unreadable headers) are sent on
// the buffered error channel so main() can inspect them after downstream stages exit.
func extractCSV(ctx context.Context, filePath string) (<-chan RawRecord, <-chan error) {
	out := make(chan RawRecord)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		file, err := os.Open(filePath)
		if err != nil {
			errCh <- fmt.Errorf("failed to open file: %w", err)
			return
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.TrimLeadingSpace = true

		// Discard the CSV header row
		if _, err := reader.Read(); err != nil {
			errCh <- fmt.Errorf("failed to read CSV header: %w", err)
			return
		}

		lineNum := 1
		for {
			lineNum++
			row, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				log.Printf("[WARN] Line %d: malformed CSV row (%v)\n", lineNum, err)
				continue
			}
			if len(row) < 4 {
				log.Printf("[WARN] Line %d: expected 4 columns, got %d\n", lineNum, len(row))
				continue
			}

			// Primary key ID must be valid to retain the row
			id, err := strconv.Atoi(strings.TrimSpace(row[0]))
			if err != nil {
				log.Printf("[WARN] Line %d: dropping row due to invalid ID %q\n", lineNum, row[0])
				continue
			}

			record := RawRecord{
				LineNum: lineNum,
				ID:      id,
				Name:    row[1],
				Email:   row[2],
				Spent:   row[3],
			}

			select {
			case <-ctx.Done():
				return
			case out <- record:
			}
		}
	}()

	return out, errCh
}

// transform is Stage 2 (TRANSFORM) of the pipeline.
// It spawns numWorkers goroutines that read from the input channel concurrently.
// Each column is validated and normalized independently:
//   - FullName: trimmed and converted to uppercase; empty strings become nil (NULL).
//   - Email: trimmed, lowercased, and checked for "@"; invalid addresses become nil (NULL).
//   - SpentCents: parsed as float64 >= 0 and converted to integer cents; invalid values become nil (NULL).
//   - UniqueRecordID: computed via MD5 over the cleaned fields.
func transform(ctx context.Context, in <-chan RawRecord, numWorkers int) <-chan CleanRecord {
	out := make(chan CleanRecord)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-in:
				if !ok {
					return
				}

				clean := CleanRecord{
					ID:          raw.ID,
					ProcessedAt: time.Now().UTC(),
				}

				// Validate & normalize Name
				trimmedName := strings.TrimSpace(raw.Name)
				if trimmedName != "" {
					upperName := strings.ToUpper(trimmedName)
					clean.FullName = &upperName
				} else {
					log.Printf("[WARN] Line %d (ID %d): empty Name set to NULL\n", raw.LineNum, raw.ID)
				}

				// Validate & normalize Email
				trimmedEmail := strings.ToLower(strings.TrimSpace(raw.Email))
				if strings.Contains(trimmedEmail, "@") {
					clean.Email = &trimmedEmail
				} else {
					log.Printf("[WARN] Line %d (ID %d): invalid Email %q set to NULL\n", raw.LineNum, raw.ID, raw.Email)
				}

				// Validate & parse Spent into integer cents to avoid floating-point drift
				parsedSpent, err := strconv.ParseFloat(strings.TrimSpace(raw.Spent), 64)
				if err == nil && parsedSpent >= 0 {
					cents := int64(parsedSpent * 100)
					clean.SpentCents = &cents
				} else {
					log.Printf("[WARN] Line %d (ID %d): invalid Spent %q set to NULL\n", raw.LineNum, raw.ID, raw.Spent)
				}

				// Compute deterministic MD5 hash after normalization & null-handling
				clean.UniqueRecordID = generateRecordHash(
					clean.ID,
					clean.FullName,
					clean.Email,
					clean.SpentCents,
				)

				select {
				case <-ctx.Done():
					return
				case out <- clean:
				}
			}
		}
	}

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	// Close the downstream channel once all transform workers finish
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// formatStrPtr dereferences a string pointer for display, returning "NULL" if nil.
func formatStrPtr(s *string) string {
	if s == nil {
		return "NULL"
	}
	return *s
}

// formatInt64Ptr dereferences an int64 pointer for display, returning "NULL" if nil.
func formatInt64Ptr(i *int64) string {
	if i == nil {
		return "NULL"
	}
	return strconv.FormatInt(*i, 10)
}

// printDataFrame renders a slice of CleanRecord structs as an aligned,
// pandas-style ASCII table with a row/column shape footer.
func printDataFrame(records []CleanRecord) {
	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Fprintln(w, "INDEX\tUNIQUERECORDID\tID\tFULL_NAME\tEMAIL\tSPENT_CENTS\tPROCESSED_AT")
	fmt.Fprintln(w, "-----\t--------------\t--\t---------\t-----\t-----------\t------------")

	for idx, r := range records {
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\t%s\t%s\n",
			idx,
			r.UniqueRecordID,
			r.ID,
			formatStrPtr(r.FullName),
			formatStrPtr(r.Email),
			formatInt64Ptr(r.SpentCents),
			r.ProcessedAt.Format("2006-01-02 15:04:05"),
		)
	}

	w.Flush()
	fmt.Printf("\n[%d rows x 6 columns]\n\n", len(records))
}

// writeParquetFile creates outputDir if needed and writes records to
// <outputDir>/<fileName>.parquet. Existing files with the same name are overwritten.
func writeParquetFile(outputDir string, fileName string, records []CleanRecord) (string, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create %s directory: %w", outputDir, err)
	}

	// Strip directory traversal components and enforce .parquet extension
	cleanName := filepath.Base(strings.TrimSpace(fileName))
	if !strings.HasSuffix(strings.ToLower(cleanName), ".parquet") {
		cleanName += ".parquet"
	}

	fullPath := filepath.Join(outputDir, cleanName)

	// parquet.WriteFile opens with O_CREATE|O_TRUNC, overwriting any previous run
	if err := parquet.WriteFile(fullPath, records); err != nil {
		return "", fmt.Errorf("failed to write parquet file: %w", err)
	}

	return fullPath, nil
}

// load is Stage 3 (LOAD) of the pipeline.
// It collects transformed records in batches of batchSize, sorts the final result
// by ID to restore deterministic ordering after parallel processing, prints the
// DataFrame summary to stdout, and writes the Parquet output file.
func load(ctx context.Context, in <-chan CleanRecord, batchSize int, outDir string, outFileName string) error {
	batch := make([]CleanRecord, 0, batchSize)
	var allLoaded []CleanRecord

	flush := func(records []CleanRecord) error {
		if len(records) == 0 {
			return nil
		}
		allLoaded = append(allLoaded, records...)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-in:
			if !ok {
				if err := flush(batch); err != nil {
					return err
				}
				// Sort by ID so parallel worker results appear in sequential order
				sort.Slice(allLoaded, func(i, j int) bool {
					return allLoaded[i].ID < allLoaded[j].ID
				})
				printDataFrame(allLoaded)
				savedPath, err := writeParquetFile(outDir, outFileName, allLoaded)
				if err != nil {
					return err
				}
				fmt.Printf("Saved Parquet file: %s\n", savedPath)
				return nil
			}
			batch = append(batch, record)
			if len(batch) >= batchSize {
				if err := flush(batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}
}

// createDummyCSV generates a sample CSV file with both valid rows and intentional
// data quality issues (invalid email, invalid float) to demonstrate NULL handling.
func createDummyCSV(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	sampleCSV := strings.Join([]string{
		"id,name,email,spent",
		"1,  alice smith ,ALICE@Example.com,49.99",
		"2,bob jones,invalid-email,15.00",
		"3,charlie brown,charlie@domain.org,120.50",
		"4,diana prince,Diana@Hero.net,300.00",
		"5,bad row,not-a-number@test.io,invalid_float",
		"6,evan wright,evan@test.io,12.75",
	}, "\n")
	return os.WriteFile(path, []byte(sampleCSV), 0644)
}
func main() {
	// Parse CLI configuration flags
	csvPath := flag.String("file", "", "Path to the input CSV file (required unless -demo is set)")
	outDir := flag.String("dir", "processed_files", "Target directory for the Parquet output")
	outName := flag.String("out", "output.parquet", "Output Parquet filename (overwrites if existing)")
	workers := flag.Int("workers", 3, "Number of concurrent transform workers")
	batchSize := flag.Int("batch", 2, "Number of records per load batch")
	demo := flag.Bool("demo", false, "Generate a dummy CSV at test_files/demo_users.csv and run the pipeline")
	flag.Parse()
	inputFile := *csvPath
	outputFile := *outName
	// Handle self-contained -demo mode
	if *demo {
		inputFile = "test_files/demo_users.csv"
		if *outName == "output.parquet" {
			outputFile = "demo_users.parquet"
		}
		fmt.Printf("Creating dummy CSV at %s...\n", inputFile)
		if err := createDummyCSV(inputFile); err != nil {
			log.Fatalf("Failed to create dummy CSV: %v", err)
		}
	}
	if inputFile == "" {
		fmt.Fprintln(os.Stderr, "Error: -file flag is required (or pass -demo to run with dummy data)")
		flag.Usage()
		os.Exit(1)
	}
	// Enforce a 30-second pipeline timeout across all concurrent goroutines
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Wire the three ETL stages together via Go channels
	extracted, extractErrCh := extractCSV(ctx, inputFile)
	transformed := transform(ctx, extracted, *workers)
	if err := load(ctx, transformed, *batchSize, *outDir, outputFile); err != nil {
		log.Fatalf("Load stage failed: %v", err)
	}
	if err := <-extractErrCh; err != nil {
		log.Fatalf("Extract stage failed: %v", err)
	}
	fmt.Println("CSV ETL pipeline completed successfully.")
}
