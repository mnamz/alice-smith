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
	Email    string `json:"Contact_EmailAddress"`
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

// Create a function that gets all the records from the User_Master dataset and compare against fetchAllParents. Return a list of records that are not in fetchAllParents but are in User_Master.
func getInactiveUsers(accessKeyId, accessKeySecret string) ([]map[string]string, error) {
	// First get all parents to build comparison set
	parents, err := fetchAllParents(accessKeyId, accessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch parents: %w", err)
	}

	// Create a map of parent emails (without @asis.edu.my) for quick lookup
	parentEmails := make(map[string]bool)
	for _, p := range parents {
		email := strings.TrimSuffix(p.Email, "@asis.edu.my")
		parentEmails[email] = true
	}

	// Fetch all User_Master records with pagination
	var recordsToDelete []map[string]string
	page := 1
	for {
		url := fmt.Sprintf("https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/view/Parents/list?page_number=%d&page_size=999&search_field=Name", page)
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
			if !parentEmails[rec.Name] {
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

	return recordsToDelete, nil
}

func stripSSOSuffix(name string) string {
	return strings.TrimSuffix(name, "- SSO")
}

func mapParentToRow(s ParentRecord) []interface{} {
	return []interface{}{
		strings.TrimSuffix(s.Email, "@asis.edu.my"),                  // parentId
		stripSSOSuffix(s.Forename) + " " + stripSSOSuffix(s.Surname), // Name
		"",        // jobTitle
		"Parents", // department
		"",        // IdentityNo
		"3",       // IdentityType
		"2",       // Gender
	}
}

func mapParentToUserMasterPayload(s ParentRecord) map[string]interface{} {
	return map[string]interface{}{
		"_id":          strings.TrimSuffix(s.Email, "@asis.edu.my"),
		"Name":         strings.TrimSuffix(s.Email, "@asis.edu.my"),
		"Name_1":       stripSSOSuffix(s.Forename) + " " + stripSSOSuffix(s.Surname),
		"Type":         "2", // 2 = Parent, 3 = Student, 1 = Staff
		"Job_Title":    "",
		"Department":   "Parents",
		"IdentityNo":   "",
		"IdentityType": "",
		"Gender":       "2",
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

	// Get records to delete (records in User_Master but not in parents)
	recordsToDelete, err := getInactiveUsers(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to get records to delete: %v", err)
	}

	// Log all recordsToDelete
	jsonPayloads, _ := json.MarshalIndent(recordsToDelete, "", "  ")
	fmt.Printf("Records to delete:\n%s\n", string(jsonPayloads))

	// send to txt
	os.WriteFile("recordsToDelete.txt", []byte(string(jsonPayloads)), 0644)

	//terminate
	// os.Exit(0)

	// Delete records that are not in parents list
	if len(recordsToDelete) > 0 {
		log.Printf("Found %d records to delete", len(recordsToDelete))
		err = common.DeleteAllUserMaster(accessKeyId, accessKeySecret, recordsToDelete)
		if err != nil {
			log.Fatalf("Failed to delete User_Master records: %v", err)
		}
	}

	// Prepare payloads for User_Master batch
	var payloads []map[string]interface{}
	for _, s := range parents {
		payloads = append(payloads, mapParentToUserMasterPayload(s))
	}

	// Debug print the payloads
	// jsonPayloads, _ := json.MarshalIndent(payloads, "", "  ")
	// fmt.Printf("Sending payloads to User_Master batch:\n%s\n", string(jsonPayloads))

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
