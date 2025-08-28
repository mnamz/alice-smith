package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"isams_to_sheets/src/common"

	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

// columnLetterToIndex converts an Excel column letter (e.g. "A", "J") to 1-based index.
func columnLetterToIndex(col string) int {
	col = strings.ToUpper(strings.TrimSpace(col))
	if col == "" {
		return 0
	}
	idx := 0
	for i := 0; i < len(col); i++ {
		c := int(col[i]) - 'A' + 1
		idx = idx*26 + c
	}
	return idx
}

func readColumnValues(xlsxPath, sheetName, columnLetter string) (map[string]struct{}, error) {
	f, err := excelize.OpenFile(xlsxPath)
	if err != nil {
		return nil, fmt.Errorf("open xlsx: %w", err)
	}
	defer f.Close()

	// Resolve sheet name: use provided or first sheet
	if strings.TrimSpace(sheetName) == "" {
		list := f.GetSheetList()
		if len(list) == 0 {
			return nil, fmt.Errorf("workbook has no sheets")
		}
		sheetName = list[0]
	}

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("get rows: %w", err)
	}

	colIdx := columnLetterToIndex(columnLetter)
	if colIdx <= 0 {
		return nil, fmt.Errorf("invalid column: %s", columnLetter)
	}

	values := make(map[string]struct{})
	for _, row := range rows {
		if len(row) < colIdx {
			continue
		}
		val := strings.TrimSpace(row[colIdx-1])
		if val == "" {
			continue
		}
		values[val] = struct{}{}
	}
	return values, nil
}

func main() {
	_ = godotenv.Load()

	defaultXlsx := "/Users/aliifz/projects/alice-smith/klass-scripts/trans/27August.xlsx"
	xlsxPath := flag.String("file", defaultXlsx, "Path to the .xlsx file in trans folder")
	sheetName := flag.String("sheet", "", "Sheet name (default: first sheet)")
	column := flag.String("col", "J", "Column letter to read IDs from")
	outPath := flag.String("out", "", "Optional path to write CSV output (default: stdout)")
	missing := flag.Bool("missing", false, "If true, output students NOT present in the Excel column")
	flag.Parse()

	apiKeyUrl := os.Getenv("API_KEY_URL")
	if apiKeyUrl == "" {
		log.Fatal("API_KEY_URL environment variable is not set")
	}

	idsFromXlsx, err := readColumnValues(*xlsxPath, *sheetName, *column)
	if err != nil {
		log.Fatalf("failed reading xlsx: %v", err)
	}
	if len(idsFromXlsx) == 0 {
		log.Fatalf("no IDs found in column %s of %s", *column, *xlsxPath)
	}

	bearerToken, err := common.GetBearerToken(apiKeyUrl)
	if err != nil {
		log.Fatalf("failed to get bearer token: %v", err)
	}
	bearer := "Bearer " + bearerToken

	students, err := common.FetchAllStudents(bearer)
	if err != nil {
		log.Fatalf("failed to fetch students: %v", err)
	}

	// Exclude year groups 7-13 (EP)
	filtered := make([]common.Student, 0, len(students))
	for _, s := range students {
		yg := strings.TrimSpace(fmt.Sprintf("%v", s.YearGroup))
		switch yg {
		case "7", "8", "9", "10", "11", "12", "13":
			continue
		}
		filtered = append(filtered, s)
	}
	students = filtered

	// Select data set: matched or missing
	selected := make([]common.Student, 0)
	if *missing {
		for _, s := range students {
			if _, ok := idsFromXlsx[strings.TrimSpace(s.SchoolId)]; !ok {
				selected = append(selected, s)
			}
		}
	} else {
		for _, s := range students {
			if _, ok := idsFromXlsx[strings.TrimSpace(s.SchoolId)]; ok {
				selected = append(selected, s)
			}
		}
	}

	// Prepare CSV writer (file or stdout)
	var (
		w  *csv.Writer
		cf *os.File
	)
	if *outPath != "" {
		// Ensure directory exists
		dir := filepath.Dir(*outPath)
		if dir != "." && dir != "" {
			_ = os.MkdirAll(dir, 0755)
		}
		cf, err = os.Create(*outPath)
		if err != nil {
			log.Fatalf("create output file: %v", err)
		}
		defer cf.Close()
		// If output extension is .txt, write tab-separated text
		if strings.HasSuffix(strings.ToLower(*outPath), ".txt") {
			bw := bufio.NewWriter(cf)
			defer bw.Flush()
			_, _ = bw.WriteString("schoolId\tfullName\tformGroup\tyearGroup\temail\n")
			for _, s := range selected {
				line := fmt.Sprintf("%s\t%s\t%s\t%v\t%s\n",
					strings.TrimSpace(s.SchoolId),
					strings.TrimSpace(s.FullName),
					strings.TrimSpace(s.FormGroup),
					fmt.Sprintf("%v", s.YearGroup),
					strings.TrimSpace(s.Email),
				)
				_, _ = bw.WriteString(line)
			}
			mode := "matched"
			if *missing {
				mode = "missing"
			}
			log.Printf("transmatch: wrote %d %s students to %s", len(selected), mode, *outPath)
			return
		}
		w = csv.NewWriter(cf)
	} else {
		w = csv.NewWriter(os.Stdout)
	}
	defer w.Flush()

	// Always write CSV header then rows
	_ = w.Write([]string{"schoolId", "fullName", "formGroup", "yearGroup", "email"})
	for _, s := range selected {
		_ = w.Write([]string{
			strings.TrimSpace(s.SchoolId),
			strings.TrimSpace(s.FullName),
			strings.TrimSpace(s.FormGroup),
			fmt.Sprintf("%v", s.YearGroup),
			strings.TrimSpace(s.Email),
		})
	}

	// Log a summary to stderr (won't pollute CSV on stdout)
	mode := "matched"
	if *missing {
		mode = "missing"
	}
	log.Printf("transmatch: wrote %d %s students", len(selected), mode)
}
