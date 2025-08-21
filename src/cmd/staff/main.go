package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"isams_to_sheets/src/common"

	"github.com/joho/godotenv"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const (
	STAFF_API = "https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/Employee_Master_01/view/Active_Employees_Basic_Details/list"
)

type StaffRecord struct {
	Name         string `json:"Name"`
	EmployeeName string `json:"Employee_Name"`
	Designation  string `json:"Designation"`
	Department   string `json:"Department"`
	Email        string `json:"Email_Address"`
	Gender       string `json:"Gender"`
}

type StaffResponse struct {
	Data []StaffRecord `json:"Data"`
}

// cardNoMap holds a mapping of staff ID (without the leading "E") to the
// corresponding proximity card number that will be pushed to Kissflow.
var cardNoMap map[string]string

// loadCardNoMap builds the card number lookup table from the raw export CSV in
// the repository. The function expects the first column (index 0) to contain
// the ID and column J (index 9) to contain the card number.
func loadCardNoMap(csvPath string) (map[string]string, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open card CSV: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // allow variable number of fields per row

	result := make(map[string]string)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row: %w", err)
		}
		if len(record) <= 9 {
			continue // not enough columns
		}
		id := strings.TrimSpace(record[0])
		card := strings.TrimSpace(record[9])
		if id == "" || card == "" {
			continue
		}
		// keep the first card number encountered for an ID
		if _, exists := result[id]; !exists {
			result[id] = card
		}
	}
	return result, nil
}

func fetchAllStaff(accessKeyId, accessKeySecret string) ([]StaffRecord, error) {
	allStaff := []StaffRecord{}
	page := 1
	for {
		url := fmt.Sprintf("%s?page_number=%d&page_size=%d", STAFF_API, page, common.PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("X-Access-Key-Id", accessKeyId)
		req.Header.Set("X-Access-Key-Secret", accessKeySecret)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var sr StaffResponse
		if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
			return nil, err
		}
		allStaff = append(allStaff, sr.Data...)
		if len(sr.Data) < common.PAGE_SIZE {
			break
		}
		page++
	}
	return allStaff, nil
}

func getInactiveUsers(accessKeyId, accessKeySecret string) ([]map[string]string, error) {
	// fetch all staff from fetchAllStaff
	staff, err := fetchAllStaff(accessKeyId, accessKeySecret)
	if err != nil {
		return nil, err
	}

	// filter out staff from fetchAllStaff that are not in User_Master by comparing email
	staffIds := make(map[string]bool)
	for _, s := range staff {
		//remove E from staffId
		staffId := strings.TrimPrefix(s.Name, "E")
		staffIds[staffId] = true
	}

	var recordsToDelete []map[string]string
	page := 1
	for {
		url := fmt.Sprintf("https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/view/Staff/list?page_number=%d&page_size=999&search_field=Name", page)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("X-Access-Key-Id", accessKeyId)
		req.Header.Set("X-Access-Key-Secret", accessKeySecret)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch User_Master records: %w", err)
		}
		defer resp.Body.Close()

		var result struct {
			Data []struct {
				Name string `json:"Name"`
				ID   string `json:"_id"`
			} `json:"Data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode User_Master response: %w", err)
		}

		// If no more records, break the loop
		if len(result.Data) == 0 {
			break
		}

		// Add records that don't exist in parents list
		for _, rec := range result.Data {
			if !staffIds[rec.Name] {
				recordsToDelete = append(recordsToDelete, map[string]string{
					"_id":  rec.ID,
					"Name": rec.Name,
				})
			}
		}

		// If we got less than page size, we're done
		if len(result.Data) < 999 {
			break
		}
		page++
	}

	// return the list of staff that are not in User_Master
	return recordsToDelete, nil
}

func mapStaffToRow(s StaffRecord) []interface{} {
	gender := "2"
	if s.Gender == "M" {
		gender = "1"
	}
	staffId := s.Name
	if len(staffId) > 1 && staffId[0] == 'E' {
		staffId = staffId[1:]
	}

	cardNo := ""
	if cardNoMap != nil {
		cardNo = cardNoMap[staffId]
	}
	return []interface{}{
		staffId,        // staffId (stripped E)
		s.EmployeeName, // Name
		s.Designation,  // jobTitle
		s.Department,   // department
		s.Email,        // IdentityNo
		"3",            // IdentityType
		gender,         // Gender
		cardNo,         // CardNo
	}
}

func mapStaffToUserMasterPayload(s StaffRecord) map[string]interface{} {
	staffId := s.Name
	if len(staffId) > 1 && staffId[0] == 'E' {
		staffId = staffId[1:]
	}

	cardNo := ""
	if cardNoMap != nil {
		cardNo = cardNoMap[staffId]
	}

	return map[string]interface{}{
		"_id":          staffId,
		"Name":         staffId,
		"Name_1":       s.EmployeeName,
		"Type":         "3",
		"Job_Title":    s.Designation,
		"Department":   s.Department,
		"IdentityNo":   "",
		"IdentityType": "3",
		"Status":       "1",
		"Gender":       s.Gender,
		"CardNo":       cardNo,
	}
}

func main() {
	// Build card number map upfront.
	var err error
	cardNoMap, err = loadCardNoMap("P1 User July.csv")
	if err != nil {
		fmt.Println("WARNING: failed to load card number CSV:", err)
	}

	if err := godotenv.Load(); err != nil {
		fmt.Println("WARNING: .env file not loaded:", err)
	} else {
		fmt.Println(".env file loaded successfully")
	}

	accessKeyId := os.Getenv("X_ACCESS_KEY_ID_VALUE")
	if accessKeyId == "" {
		log.Fatal("X_ACCESS_KEY_ID environment variable is not set")
	}

	accessKeySecret := os.Getenv("X_ACCESS_KEY_SECRET_VALUE")
	if accessKeySecret == "" {
		log.Fatal("X_ACCESS_KEY_SECRET environment variable is not set")
	}

	ctx := context.Background()
	b, err := os.ReadFile("api.json")
	if err != nil {
		log.Fatalf("Unable to read service account file: %v", err)
	}
	config, err := google.JWTConfigFromJSON(b, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("Unable to parse service account file: %v", err)
	}
	client := config.Client(ctx)

	staff, err := fetchAllStaff(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Unable to fetch staff: %v", err)
	}

	// get inactive users
	recordsToDelete, err := getInactiveUsers(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to get inactive users: %v", err)
	}

	// log all recordsToDelete
	jsonPayloads, _ := json.MarshalIndent(recordsToDelete, "", "  ")
	fmt.Printf("Records to delete:\n%s\n", string(jsonPayloads))

	// send to txt
	os.WriteFile("staffRecordsToDelete.txt", []byte(string(jsonPayloads)), 0644)

	// terminate
	// os.Exit(0)

	// Delete all User_Master records before sending new ones
	err = common.DeleteAllUserMaster(accessKeyId, accessKeySecret, recordsToDelete)
	if err != nil {
		log.Fatalf("Failed to delete all User_Master records: %v", err)
	}

	// Prepare payloads for User_Master batch
	var payloads []map[string]interface{}
	for _, s := range staff {
		payloads = append(payloads, mapStaffToUserMasterPayload(s))
	}

	// Send to User_Master/batch endpoint
	err = common.SendToUserMasterBatch(payloads, accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to send to User_Master batch endpoint: %v", err)
	}

	headers := []interface{}{"staffId", "Name", "jobTitle", "department", "IdentityNo", "IdentityType", "Gender", "CardNo"}
	values := [][]interface{}{headers}
	for _, s := range staff {
		values = append(values, mapStaffToRow(s))
	}

	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	clearReq := &sheets.ClearValuesRequest{}
	_, err = srv.Spreadsheets.Values.Clear(common.SPREADSHEET_ID, common.SHEET_NAME_STAFF, clearReq).Do()
	if err != nil {
		log.Fatalf("Unable to clear sheet: %v", err)
	}

	vr := &sheets.ValueRange{
		Values: values,
	}
	_, err = srv.Spreadsheets.Values.Update(common.SPREADSHEET_ID, common.SHEET_NAME_STAFF+"!A1", vr).ValueInputOption("RAW").Do()
	if err != nil {
		log.Fatalf("Unable to write to sheet: %v", err)
	}

	fmt.Printf("Done! Wrote %d staff records to the sheet.\n", len(staff))
}
