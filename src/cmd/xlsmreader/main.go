package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/xuri/excelize/v2"
)

func main() {
	defaultPath := "/Users/aliifz/projects/alice-smith/klass-scripts/TAMS EP Students (1).xlsm"
	filePath := flag.String("file", defaultPath, "Path to the .xlsm/.xlsx file to read")
	maxRows := flag.Int("rows", 10, "Maximum number of rows to print per sheet (preview)")
	maxCols := flag.Int("cols", 10, "Maximum number of columns to print per row (preview)")
	flag.Parse()

	if _, err := os.Stat(*filePath); err != nil {
		log.Fatalf("cannot access file %q: %v", *filePath, err)
	}

	f, err := excelize.OpenFile(*filePath)
	if err != nil {
		log.Fatalf("failed to open Excel file: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("warning: failed to close file: %v", err)
		}
	}()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		log.Println("no sheets found")
		return
	}

	fmt.Printf("Opened %q\n", *filePath)
	fmt.Printf("Found %d sheets:\n", len(sheets))
	for i, name := range sheets {
		fmt.Printf("  %d. %s\n", i+1, name)
	}

	fmt.Println()
	for _, sheetName := range sheets {
		rows, err := f.GetRows(sheetName)
		if err != nil {
			log.Printf("failed to read rows for sheet %q: %v", sheetName, err)
			continue
		}

		fmt.Printf("Sheet: %s (total rows: %d)\n", sheetName, len(rows))
		limit := *maxRows
		if limit > len(rows) {
			limit = len(rows)
		}
		for r := 0; r < limit; r++ {
			row := rows[r]
			cLimit := *maxCols
			if cLimit > len(row) {
				cLimit = len(row)
			}
			fmt.Printf("  %4d |", r+1)
			for c := 0; c < cLimit; c++ {
				val := row[c]
				fmt.Printf(" %s", val)
				if c < cLimit-1 {
					fmt.Print(" |")
				}
			}
			fmt.Println()
		}
		fmt.Println()
	}
}
