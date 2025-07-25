package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	PARENTS_API = "https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/iSAMS_Family_ASIS_EDU_MY_Contacts/list"
)

type ParentRecord struct {
	Name     string `json:"Name"`
	Forename string `json:"Contact_Forename"`
	Surname  string `json:"Contact_Surname"`
	Email    string `json:"Email_Address"`
}

type ParentResponse struct {
	Data []ParentRecord `json:"Data"`
}

func fetchAllParents(accessKeyId, accessKeySecret string) ([]ParentRecord, error) {
	allParents := []ParentRecord{}
	page := 1
	for {
		url := fmt.Sprintf("%s?page_number=%d&page_size=%d", PARENTS_API, page, common.PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("X-Access-Key-Id", accessKeyId)
		req.Header.Set("X-Access-Key-Secret", accessKeySecret)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var sr ParentResponse
		if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
			return nil, err
		}
		allParents = append(allParents, sr.Data...)
		if len(sr.Data) < common.PAGE_SIZE {
			break
		}
		page++
	}
	return allParents, nil
}

func mapParentToRow(s ParentRecord) []interface{} {
	return []interface{}{
		strings.TrimSuffix(s.Email, "@asis.edu.my"), // parentId (stripped E)
		s.Forename + " " + s.Surname,                // Name
		"",                                          // jobTitle
		"Parents",                                   // department
		"",                                          // IdentityNo
		"3",                                         // IdentityType
		"2",                                         // Gender
	}
}

func mapParentToUserMasterPayload(s ParentRecord) map[string]interface{} {
	return map[string]interface{}{
		"_id":          s.Email,
		"Name":         s.Email,
		"Name_1":       s.Forename + " " + s.Surname,
		"Type":         "3",
		"Job_Title":    "",
		"Department":   "Parents",
		"IdentityNo":   s.Email,
		"IdentityType": "3",
		"Status":       "1",
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("WARNING: .env file not loaded:", err)
	} else {
		fmt.Println(".env file loaded successfully")
	}

	accessKeyId := os.Getenv("X_ACCESS_KEY_ID_VALUE")
	if accessKeyId == "" {
		log.Fatal("X_ACCESS_KEY_ID_VALUE environment variable is not set")
	}

	accessKeySecret := os.Getenv("X_ACCESS_KEY_SECRET_VALUE")
	if accessKeySecret == "" {
		log.Fatal("X_ACCESS_KEY_SECRET_VALUE environment variable is not set")
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

	parents, err := fetchAllParents(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Unable to fetch parents: %v", err)
	}

	// Delete all User_Master records before sending new ones
	err = common.DeleteAllUserMaster(accessKeyId, accessKeySecret, "Parents")
	if err != nil {
		log.Fatalf("Failed to delete all User_Master records: %v", err)
	}

	// Prepare payloads for User_Master batch
	var payloads []map[string]interface{}
	for _, s := range parents {
		payloads = append(payloads, mapParentToUserMasterPayload(s))
	}

	// Debug print the payloads
	jsonPayloads, _ := json.MarshalIndent(payloads, "", "  ")
	fmt.Printf("Sending payloads to User_Master batch:\n%s\n", string(jsonPayloads))

	// Send to User_Master/batch endpoint
	err = common.SendToUserMasterBatch(payloads, accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to send to User_Master batch endpoint: %v", err)
	}

	headers := []interface{}{"parentId", "Name", "jobTitle", "department", "IdentityNo", "IdentityType", "Gender"}
	values := [][]interface{}{headers}
	for _, s := range parents {
		values = append(values, mapParentToRow(s))
	}

	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	clearReq := &sheets.ClearValuesRequest{}
	_, err = srv.Spreadsheets.Values.Clear(common.SPREADSHEET_ID, common.SHEET_NAME_PARENTS, clearReq).Do()
	if err != nil {
		log.Fatalf("Unable to clear sheet: %v", err)
	}

	vr := &sheets.ValueRange{
		Values: values,
	}
	_, err = srv.Spreadsheets.Values.Update(common.SPREADSHEET_ID, common.SHEET_NAME_PARENTS+"!A1", vr).ValueInputOption("RAW").Do()
	if err != nil {
		log.Fatalf("Unable to write to sheet: %v", err)
	}

	fmt.Printf("Done! Wrote %d parent records to the sheet.\n", len(parents))
}
