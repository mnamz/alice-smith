package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"golang.org/x/oauth2/google"
)

const (
	SPREADSHEET_ID = "10hEhyN2-xeDT0b193h236u5lTbjHg5F7CuxjQN7IagA"
	SHEET_NAME     = "Staff"
	STAFF_API      = "https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/Employee_Master_01/view/Active_Employees_Basic_Details/list"
	X_ACCESS_KEY_ID = "X-Access-Key-Id"
	X_ACCESS_KEY_SECRET = "X-Access-Key-Secret"
	X_ACCESS_KEY_ID_VALUE = "Aka0ca96ef-3aa7-4d2d-979b-39abc46de433"
	X_ACCESS_KEY_SECRET_VALUE = "emZhF2r4rOtlnRSX5HvppCg6lMZ609LgAofJz0gKOz8nbcreX2NocVsf81ioFlor9H75cNHuIX5YJ52hGg"
	PAGE_SIZE = 1000
	USER_MASTER_BATCH_API = "https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/batch"
	BATCH_SIZE = 500
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

func fetchAllStaff() ([]StaffRecord, error) {
	allStaff := []StaffRecord{}
	page := 1
	for {
		url := fmt.Sprintf("%s?page_number=%d&page_size=%d", STAFF_API, page, PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set(X_ACCESS_KEY_ID, X_ACCESS_KEY_ID_VALUE)
		req.Header.Set(X_ACCESS_KEY_SECRET, X_ACCESS_KEY_SECRET_VALUE)
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
		if len(sr.Data) < PAGE_SIZE {
			break
		}
		page++
	}
	return allStaff, nil
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
	return []interface{}{
		staffId,         // staffId (stripped E)
		s.EmployeeName,   // Name
		s.Designation,    // jobTitle
		s.Department,     // department
		s.Email,          // IdentityNo
		"3",             // IdentityType
		gender,          // Gender
	}
}

func mapStaffToUserMasterPayload(s StaffRecord) map[string]interface{} {
	staffId := s.Name
	if len(staffId) > 1 && staffId[0] == 'E' {
		staffId = staffId[1:]
	}
	return map[string]interface{}{
		"_id":         staffId,
		"Name":        staffId,
		"Name_1":      s.EmployeeName,
		"Type":        "3",
		"Job_Title":   s.Designation,
		"Department":  s.Department,
		"IdentityNo":  s.Email,
		"IdentityType": "3",
	}
}

func deleteAllUserMaster() error {
	// Fetch all User_Master records
	url := "https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/view/Staff/list"
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set(X_ACCESS_KEY_ID, X_ACCESS_KEY_ID_VALUE)
	req.Header.Set(X_ACCESS_KEY_SECRET, X_ACCESS_KEY_SECRET_VALUE)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch User_Master records: %w", err)
	}
	defer resp.Body.Close()
	var result struct {
		Data []struct {
			Name string `json:"Name"`
			ID   string `json:"_id"`
		} `json:"Data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode User_Master response: %w", err)
	}

	// Delete each record
	deleteURL := "https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master"
	for _, rec := range result.Data {
		payload := map[string]string{
			"Name": rec.Name,
			"_id":  rec.ID,
		}
		jsonPayload, _ := json.Marshal(payload)
		req, _ := http.NewRequest("DELETE", deleteURL, bytes.NewReader(jsonPayload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(X_ACCESS_KEY_ID, X_ACCESS_KEY_ID_VALUE)
		req.Header.Set(X_ACCESS_KEY_SECRET, X_ACCESS_KEY_SECRET_VALUE)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Failed to delete User_Master record %s: %v", rec.ID, err)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := ioutil.ReadAll(resp.Body)
			log.Printf("Failed to delete User_Master record %s: %s", rec.ID, string(body))
			continue
		}
		log.Printf("Deleted User_Master record: %s", rec.ID)
	}
	return nil
}

func sendToUserMasterBatch(staff []StaffRecord) error {
	for i := 0; i < len(staff); i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > len(staff) {
			end = len(staff)
		}
		batch := staff[i:end]
		var payload []map[string]interface{}
		for _, s := range batch {
			payload = append(payload, mapStaffToUserMasterPayload(s))
		}
		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal batch payload: %w", err)
		}
		req, _ := http.NewRequest("POST", USER_MASTER_BATCH_API, bytes.NewReader(jsonPayload))
		req.Header.Set(X_ACCESS_KEY_ID, X_ACCESS_KEY_ID_VALUE)
		req.Header.Set(X_ACCESS_KEY_SECRET, X_ACCESS_KEY_SECRET_VALUE)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("batch API error: %s", string(body))
		}
		log.Printf("Batch %d-%d sent successfully", i+1, end)
	}
	return nil
}

func main() {
	ctx := context.Background()
	b, err := ioutil.ReadFile("api.json")
	if err != nil {
		log.Fatalf("Unable to read service account file: %v", err)
	}
	config, err := google.JWTConfigFromJSON(b, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("Unable to parse service account file: %v", err)
	}
	client := config.Client(ctx)

	staff, err := fetchAllStaff()
	if err != nil {
		log.Fatalf("Unable to fetch staff: %v", err)
	}

	// Delete all User_Master records before sending new ones
	err = deleteAllUserMaster()
	if err != nil {
		log.Fatalf("Failed to delete all User_Master records: %v", err)
	}

	// Send to User_Master/batch endpoint in batches
	err = sendToUserMasterBatch(staff)
	if err != nil {
		log.Fatalf("Failed to send to User_Master batch endpoint: %v", err)
	}

	headers := []interface{}{ "staffId", "Name", "jobTitle", "department", "IdentityNo", "IdentityType", "Gender" }
	values := [][]interface{}{headers}
	for _, s := range staff {
		values = append(values, mapStaffToRow(s))
	}

	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	clearReq := &sheets.ClearValuesRequest{}
	_, err = srv.Spreadsheets.Values.Clear(SPREADSHEET_ID, SHEET_NAME, clearReq).Do()
	if err != nil {
		log.Fatalf("Unable to clear sheet: %v", err)
	}

	vr := &sheets.ValueRange{
		Values: values,
	}
	_, err = srv.Spreadsheets.Values.Update(SPREADSHEET_ID, SHEET_NAME+"!A1", vr).ValueInputOption("RAW").Do()
	if err != nil {
		log.Fatalf("Unable to write to sheet: %v", err)
	}

	fmt.Printf("Done! Wrote %d staff records to the sheet.\n", len(staff))
} 