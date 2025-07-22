package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"isams_to_sheets/src/common"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const (
	STAFF_API           = "https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/Employee_Master_01/view/Active_Employees_Basic_Details/list"
	X_ACCESS_KEY_ID     = "X-Access-Key-Id"
	X_ACCESS_KEY_SECRET = "X-Access-Key-Secret"
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

func fetchAllStaff(accessKeyId, accessKeySecret string) ([]StaffRecord, error) {
	allStaff := []StaffRecord{}
	page := 1
	for {
		url := fmt.Sprintf("%s?page_number=%d&page_size=%d", STAFF_API, page, common.PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set(X_ACCESS_KEY_ID, accessKeyId)
		req.Header.Set(X_ACCESS_KEY_SECRET, accessKeySecret)
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
		staffId,        // staffId (stripped E)
		s.EmployeeName, // Name
		s.Designation,  // jobTitle
		s.Department,   // department
		s.Email,        // IdentityNo
		"3",            // IdentityType
		gender,         // Gender
	}
}

func mapStaffToUserMasterPayload(s StaffRecord) map[string]interface{} {
	staffId := s.Name
	if len(staffId) > 1 && staffId[0] == 'E' {
		staffId = staffId[1:]
	}
	return map[string]interface{}{
		"_id":          staffId,
		"Name":         staffId,
		"Name_1":       s.EmployeeName,
		"Type":         "3",
		"Job_Title":    s.Designation,
		"Department":   s.Department,
		"IdentityNo":   s.Email,
		"IdentityType": "3",
	}
}

func main() {
	accessKeyId := os.Getenv("X_ACCESS_KEY_ID_VALUE")
	if accessKeyId == "" {
		log.Fatal("X_ACCESS_KEY_ID_VALUE environment variable is not set")
	}

	accessKeySecret := os.Getenv("X_ACCESS_KEY_SECRET_VALUE")
	if accessKeySecret == "" {
		log.Fatal("X_ACCESS_KEY_SECRET_VALUE environment variable is not set")
	}

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

	staff, err := fetchAllStaff(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Unable to fetch staff: %v", err)
	}

	// Delete all User_Master records before sending new ones
	err = common.DeleteAllUserMaster(accessKeyId, accessKeySecret, "Staff")
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

	headers := []interface{}{"staffId", "Name", "jobTitle", "department", "IdentityNo", "IdentityType", "Gender"}
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
