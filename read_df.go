// Package main provides a lightweight CLI utility to read Apache Parquet files
// into a typed Go DataFrame and render them as an aligned terminal table.
//
// Prerequisites:
//
//	go get github.com/parquet-go/parquet-go
//
// Usage Examples:
//
//  1. Run with built-in dummy data (creates a sample Parquet file and prints it):
//     go run read_df.go -demo
//
//  2. Read an existing Parquet file from your ETL pipeline:
//     go run read_df.go -file processed_files/clean_users.parquet
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/parquet-go/parquet-go"
)

// CleanRecord defines the schema for a single row in the Parquet file.
// Struct tags map Go fields to Parquet column names, nullability, and logical types:
//   - "optional": marks pointer fields (*string, *int64) as nullable in Parquet.
//   - "timestamp(millisecond)": stores Go time.Time as an INT64 millisecond epoch.
type CleanRecord struct {
	UniqueRecordID string    `parquet:"unique_record_id"`
	ID             int       `parquet:"id"`
	FullName       *string   `parquet:"full_name,optional"`
	Email          *string   `parquet:"email,optional"`
	SpentCents     *int64    `parquet:"spent_cents,optional"`
	ProcessedAt    time.Time `parquet:"processed_at,timestamp(millisecond)"`
}

// DataFrame represents an in-memory tabular collection of CleanRecord rows.
// Wrapping the slice in a named type allows us to attach DataFrame-style methods
// like String() for automatic table formatting.
type DataFrame []CleanRecord

// ReadParquet loads all rows from a Parquet file at the given path into a DataFrame.
// This acts as a strongly-typed Go equivalent to Python's `df = spark.read.parquet(filename)`.
//
// Note: Because parquet.ReadFile loads the entire dataset into RAM, use
// parquet.NewGenericReader[CleanRecord] for streaming multi-gigabyte files.
func ReadParquet(filename string) (DataFrame, error) {
	return parquet.ReadFile[CleanRecord](filename)
}

// String implements the fmt.Stringer interface so calling fmt.Println(df)
// automatically renders an aligned, pandas-style ASCII table with NULL handling.
func (df DataFrame) String() string {
	var buf bytes.Buffer

	// Initialize tabwriter with 3 spaces of padding between columns
	w := tabwriter.NewWriter(&buf, 0, 0, 3, ' ', 0)

	// Write table headers and visual divider
	fmt.Fprintln(w, "INDEX\tUNIQUERECORDID\tID\tFULL_NAME\tEMAIL\tSPENT_CENTS\tPROCESSED_AT")
	fmt.Fprintln(w, "-----\t--------------\t--\t---------\t-----\t-----------\t------------")

	for idx, r := range df {
		// Dereference optional pointer fields safely, rendering nil pointers as "NULL"
		name := "NULL"
		if r.FullName != nil {
			name = *r.FullName
		}

		email := "NULL"
		if r.Email != nil {
			email = *r.Email
		}

		spent := "NULL"
		if r.SpentCents != nil {
			spent = strconv.FormatInt(*r.SpentCents, 10)
		}

		fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\t%s\t%s\n",
			idx,
			r.UniqueRecordID,
			r.ID,
			name,
			email,
			spent,
			r.ProcessedAt.Format("2006-01-02 15:04:05"), // Go's reference timestamp layout
		)
	}

	// Flush buffered tab stops to the underlying bytes.Buffer
	w.Flush()

	// Append a pandas/Polars-style shape summary footer
	fmt.Fprintf(&buf, "\n[%d rows x 6 columns]\n", len(df))
	return buf.String()
}

// createDummyParquet writes a small sample Parquet file containing both valid
// and NULL values so you can test ReadParquet immediately without running an ETL job first.
func createDummyParquet(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Helper variables so we can take memory addresses (&) for pointer fields
	aliceName := "ALICE SMITH"
	aliceEmail := "alice@example.com"
	aliceSpent := int64(4999)

	bobName := "BOB JONES"
	bobSpent := int64(1500) // Bob's email will be left as nil (NULL)

	dummyRows := []CleanRecord{
		{
			UniqueRecordID: "8f92c1d4a7e3b05691c4e2d8a1b7f034",
			ID:             1,
			FullName:       &aliceName,
			Email:          &aliceEmail,
			SpentCents:     &aliceSpent,
			ProcessedAt:    time.Now().UTC(),
		},
		{
			UniqueRecordID: "3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f",
			ID:             2,
			FullName:       &bobName,
			Email:          nil, // Demonstrates Parquet NULL serialization
			SpentCents:     &bobSpent,
			ProcessedAt:    time.Now().UTC(),
		},
	}

	return parquet.WriteFile(path, dummyRows)
}

func main() {
	// Define CLI flags
	filename := flag.String("file", "processed_files/clean_users.parquet", "Path to the Parquet file to read")
	demo := flag.Bool("demo", false, "Generate and read a dummy Parquet file at processed_files/demo_users.parquet")
	flag.Parse()

	targetFile := *filename

	// If -demo is passed, generate a self-contained sample file first
	if *demo {
		targetFile = "processed_files/demo_users.parquet"
		fmt.Printf("Generating dummy Parquet file at %s...\n\n", targetFile)
		if err := createDummyParquet(targetFile); err != nil {
			log.Fatalf("Failed to create dummy file: %v", err)
		}
	}

	// Load the Parquet file into our DataFrame type
	df, err := ReadParquet(targetFile)
	if err != nil {
		log.Fatalf("Failed to read Parquet file %q: %v", targetFile, err)
	}

	// Automatically invokes df.String() to print the formatted table
	fmt.Println(df)
}
