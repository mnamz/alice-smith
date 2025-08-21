package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

// processCSV extracts rows where column G contains the word "FAMILY" from the raw export
// and writes them into a new CSV file. This is a helper utility and should be
// invoked manually. Renaming the entry-point from `main` avoids clashing with the
// primary CLI binary built from this package.
func processCSV() {
	// Open the input CSV file
	inputFile, err := os.Open("P1 User July.csv")
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer inputFile.Close()

	// Create CSV reader
	reader := csv.NewReader(inputFile)
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	// Create output CSV file
	outputFile, err := os.Create("column_g_family.csv")
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()

	// Create CSV writer
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"Column G"}); err != nil {
		fmt.Printf("Error writing header: %v\n", err)
		return
	}

	// Process each row
	for {
		record, err := reader.Read()
		if err != nil {
			break // End of file or error
		}

		// Check if we have at least 7 columns (G is the 7th column, index 6)
		if len(record) > 6 {
			columnG := record[6]
			// Check if column G contains "FAMILY" (case insensitive)
			if strings.Contains(strings.ToUpper(columnG), "FAMILY") {
				if err := writer.Write([]string{columnG}); err != nil {
					fmt.Printf("Error writing record: %v\n", err)
					return
				}
			}
		}
	}

	fmt.Println("Processing complete. Results saved to column_g_family.csv")
}
