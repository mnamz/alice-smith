package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

// columnRefToIndex converts a spreadsheet-like column reference to zero-based index.
// Accepts either a number (e.g. "6" -> 6) or a letter (e.g. "G" -> 6).
func columnRefToIndex(ref string) (int, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return 0, fmt.Errorf("empty column reference")
	}

	// If it's a number, parse as index directly
	if ref[0] >= '0' && ref[0] <= '9' {
		var idx int
		_, err := fmt.Sscanf(ref, "%d", &idx)
		if err != nil {
			return 0, fmt.Errorf("invalid numeric column reference %q: %w", ref, err)
		}
		return idx, nil
	}

	// Otherwise treat as letters, like Excel column
	ref = strings.ToUpper(ref)
	value := 0
	for i := 0; i < len(ref); i++ {
		ch := ref[i]
		if ch < 'A' || ch > 'Z' {
			return 0, fmt.Errorf("invalid alphabetic column reference %q", ref)
		}
		value = value*26 + int(ch-'A'+1)
	}
	// Convert 1-based to 0-based index
	return value - 1, nil
}

func main() {
	var (
		inputPath  string
		outputPath string
		columnRef  string
		hasHeader  bool
	)

	flag.StringVar(&inputPath, "in", "export.csv", "Path to input CSV file")
	flag.StringVar(&outputPath, "out", "group_by_G.csv", "Path to output CSV summary file")
	flag.StringVar(&columnRef, "col", "G", "Column to group by (letter like G or zero-based index like 6)")
	flag.BoolVar(&hasHeader, "header", false, "Set true if the CSV has a header row to skip")
	flag.Parse()

	colIndex, err := columnRefToIndex(columnRef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open input file %q: %v\n", inputPath, err)
		os.Exit(1)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	// Optionally skip header
	if hasHeader {
		_, _ = reader.Read()
	}

	counts := make(map[string]int)
	var totalRows int

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		totalRows++
		if colIndex < 0 || colIndex >= len(record) {
			// Skip rows that don't have enough columns
			continue
		}
		key := strings.TrimSpace(record[colIndex])
		counts[key]++
	}

	// Prepare sorted summary
	type kv struct {
		Key   string
		Count int
	}
	var summary []kv
	for k, v := range counts {
		summary = append(summary, kv{Key: k, Count: v})
	}
	sort.Slice(summary, func(i, j int) bool {
		if summary[i].Count == summary[j].Count {
			return summary[i].Key < summary[j].Key
		}
		return summary[i].Count > summary[j].Count
	})

	// Write CSV summary
	out, err := os.Create(outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file %q: %v\n", outputPath, err)
		os.Exit(1)
	}
	defer out.Close()

	w := csv.NewWriter(out)
	_ = w.Write([]string{"Group", "Count"})
	for _, item := range summary {
		_ = w.Write([]string{item.Key, fmt.Sprintf("%d", item.Count)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing CSV: %v\n", err)
		os.Exit(1)
	}

	// Also print a quick human-readable summary
	fmt.Printf("Processed %d rows. Unique groups: %d. Output: %s\n", totalRows, len(summary), outputPath)
	for i, item := range summary {
		if i >= 20 {
			fmt.Printf("... (%d more)\n", len(summary)-i)
			break
		}
		fmt.Printf("%5d  %s\n", item.Count, item.Key)
	}
}

