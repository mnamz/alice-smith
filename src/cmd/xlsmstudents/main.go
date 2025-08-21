package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

const (
	studentsAPI = "https://alice-smith.isamshosting.cloud/Main/api/students"
)

type bearerTokenResponse struct {
	BearerToken string `json:"bearer_token"`
}

type student struct {
	SchoolId  string      `json:"schoolId"`
	FullName  string      `json:"fullName"`
	FormGroup string      `json:"formGroup"`
	YearGroup interface{} `json:"yearGroup"`
	Email     string      `json:"schoolEmailAddress"`
}

type studentsResponse struct {
	Count    int       `json:"count"`
	Page     int       `json:"page"`
	PageSize int       `json:"pageSize"`
	Students []student `json:"students"`
}

func getBearerToken(apiKeyUrl string) (string, error) {
	resp, err := http.Get(apiKeyUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var tokenResp bearerTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", err
	}
	return tokenResp.BearerToken, nil
}

func fetchAllStudents(bearer string, pageSize int) ([]student, error) {
	var all []student
	page := 1
	for {
		url := fmt.Sprintf("%s?page=%d&pageSize=%d", studentsAPI, page, pageSize)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Authorization", bearer)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var sr studentsResponse
		if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
			return nil, err
		}
		all = append(all, sr.Students...)
		if len(sr.Students) < pageSize {
			break
		}
		page++
	}
	return all, nil
}

func inEP(yearGroup interface{}) bool {
	// EP when YearGroup is one of 7-13
	s := strings.TrimSpace(fmt.Sprintf("%v", yearGroup))
	switch s {
	case "7", "8", "9", "10", "11", "12", "13":
		return true
	default:
		return false
	}
}

func main() {
	_ = godotenv.Load()

	defaultXlsm := "/Users/aliifz/projects/alice-smith/klass-scripts/TAMS EP Students (1).xlsm"
	filePath := flag.String("file", defaultXlsm, "Path to the .xlsm file to update")
	sheetName := flag.String("sheet", "EP Students", "Sheet name to update")
	pageSize := flag.Int("pagesize", 999, "Students API page size")
	flag.Parse()

	apiKeyUrl := os.Getenv("API_KEY_URL")
	if apiKeyUrl == "" {
		log.Fatal("API_KEY_URL environment variable is not set")
	}

	bearerToken, err := getBearerToken(apiKeyUrl)
	if err != nil {
		log.Fatalf("failed to get bearer token: %v", err)
	}
	bearer := "Bearer " + bearerToken

	students, err := fetchAllStudents(bearer, *pageSize)
	if err != nil {
		log.Fatalf("failed to fetch students: %v", err)
	}

	// Open or create workbook
	f, err := excelize.OpenFile(*filePath)
	createdNew := false
	if err != nil {
		if os.IsNotExist(err) {
			f = excelize.NewFile()
			createdNew = true
		} else {
			log.Fatalf("failed to open xlsm file: %v", err)
		}
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("warning: failed to close xlsm file: %v", err)
		}
	}()

	// Ensure sheet exists; create if missing
	idx, idxErr := f.GetSheetIndex(*sheetName)
	if idxErr != nil {
		log.Fatalf("failed to get sheet index for %q: %v", *sheetName, idxErr)
	}
	if idx == -1 {
		newIdx, err := f.NewSheet(*sheetName)
		if err != nil {
			log.Fatalf("failed to create sheet %q: %v", *sheetName, err)
		}
		f.SetActiveSheet(newIdx)
		if createdNew {
			if defIdx, _ := f.GetSheetIndex("Sheet1"); defIdx != -1 && *sheetName != "Sheet1" {
				_ = f.DeleteSheet("Sheet1")
			}
		}
	}

	// Clear existing data rows (keep header)
	rows, err := f.GetRows(*sheetName)
	if err != nil {
		log.Fatalf("failed to read rows: %v", err)
	}
	// Remove from bottom to row 2
	for r := len(rows); r >= 2; r-- {
		if err := f.RemoveRow(*sheetName, r); err != nil {
			log.Fatalf("failed to remove row %d: %v", r, err)
		}
	}

	// Header as per spec
	headers := []interface{}{
		"Member Id",
		"Name",
		"Logon Id",
		"Member Type",
		"Category",
		"Teacher Grade",
		"Email",
		"Smart Card No",
		"Department",
		"Job Description",
		"Is Host",
		"Access Start Date",
		"Access End Date",
		"Join Date",
		"Location",
		"Class",
	}
	if err := f.SetSheetRow(*sheetName, "A1", &headers); err != nil {
		log.Fatalf("failed to write headers: %v", err)
	}

	// Date formatting: dd/MM/YYYY
	now := time.Now()
	startDate := now.Format("02/01/2006")
	endDate := now.AddDate(10, 0, 0).Format("02/01/2006")

	rowIdx := 2
	for _, s := range students {
		location := "Jalan Bellamy"
		if inEP(s.YearGroup) {
			location = "Equine Park"
		}
		row := []interface{}{
			s.SchoolId,  // Member Id
			s.FullName,  // Name
			s.SchoolId,  // Logon Id
			"Student",   // Member Type
			"STUDENT",   // Category
			"",          // Teacher Grade
			s.Email,     // Email
			"",          // Smart Card No
			"",          // Department
			"",          // Job Description
			"",          // Is Host
			startDate,   // Access Start Date
			endDate,     // Access End Date
			startDate,   // Join Date
			location,    // Location
			s.FormGroup, // Class
		}
		cell := fmt.Sprintf("A%d", rowIdx)
		if err := f.SetSheetRow(*sheetName, cell, &row); err != nil {
			log.Fatalf("failed to write row %d: %v", rowIdx, err)
		}
		rowIdx++
	}

	if createdNew {
		if err := f.SaveAs(*filePath); err != nil {
			log.Fatalf("failed to save new workbook: %v", err)
		}
	} else {
		if err := f.Save(); err != nil {
			log.Fatalf("failed to save workbook: %v", err)
		}
	}

	fmt.Printf("Updated %q sheet with %d students.\n", *sheetName, len(students))
}
