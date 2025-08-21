package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"

	"isams_to_sheets/src/common"
)

// fetchAllUserMasterOthers retrieves all Others view records from User_Master so they can be deleted.
func fetchAllUserMasterOthers(accessKeyId, accessKeySecret string) ([]map[string]string, error) {
	var records []map[string]string
	page := 1
	for {
		url := fmt.Sprintf("https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/view/Others/list?page_number=%d&page_size=%d&search_field=Name", page, common.PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("X-Access-Key-Id", accessKeyId)
		req.Header.Set("X-Access-Key-Secret", accessKeySecret)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch User_Master others: %w", err)
		}
		defer resp.Body.Close()

		var result struct {
			Data []struct {
				Name string `json:"Name"`
				ID   string `json:"_id"`
			} `json:"Data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		if len(result.Data) == 0 {
			break
		}

		for _, rec := range result.Data {
			records = append(records, map[string]string{
				"_id":  rec.ID,
				"Name": rec.Name,
			})
		}

		if len(result.Data) < common.PAGE_SIZE {
			break
		}
		page++
	}
	return records, nil
}

func main() {
	// Get the absolute path to the workspace root
	workspaceRoot := "/Users/aliifz/projects/alice-smith/klass-scripts"

	// Load environment variables for Kissflow API access
	if err := godotenv.Load(); err != nil {
		fmt.Println("WARNING: .env file not loaded:", err)
	}

	accessKeyId := os.Getenv("X_ACCESS_KEY_ID_VALUE")
	if accessKeyId == "" {
		log.Fatal("X_ACCESS_KEY_ID_VALUE environment variable is not set")
	}

	accessKeySecret := os.Getenv("X_ACCESS_KEY_SECRET_VALUE")
	if accessKeySecret == "" {
		log.Fatal("X_ACCESS_KEY_SECRET_VALUE environment variable is not set")
	}

	// Fetch existing Others records from User_Master and delete them
	recordsToDelete, err := fetchAllUserMasterOthers(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to fetch 'Others' for deletion: %v", err)
	}
	if len(recordsToDelete) > 0 {
		log.Printf("Deleting %d existing 'Others' records from User_Master...", len(recordsToDelete))
		if err := common.DeleteAllUserMaster(accessKeyId, accessKeySecret, recordsToDelete); err != nil {
			log.Fatalf("Failed to delete User_Master 'Others': %v", err)
		}
	} else {
		log.Println("No existing 'Others' records found to delete.")
	}

	// Open the input CSV file
	inputFile, err := os.Open(filepath.Join(workspaceRoot, "P1_OTHERS.csv"))
	if err != nil {
		log.Fatalf("Error opening input file: %v", err)
	}
	defer inputFile.Close()

	reader := csv.NewReader(inputFile)
	reader.FieldsPerRecord = -1

	// Read header row
	header, err := reader.Read()
	if err != nil {
		log.Fatalf("Error reading CSV header: %v", err)
	}

	var payloads []map[string]interface{}
	rowIndex := 1
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		rowIndex++
		if len(row) == 0 {
			continue
		}

		payload := make(map[string]interface{})
		for i := range header {
			if i < len(row) {
				payload[header[i]] = row[i]
			} else {
				payload[header[i]] = ""
			}
		}
		// Ensure _id is present; set it to the same value as Name
		if nameVal, ok := payload["Name"]; ok {
			payload["_id"] = nameVal
		}
		payloads = append(payloads, payload)
	}

	if len(payloads) > 0 {
		if err := common.SendToUserMasterBatch(payloads, accessKeyId, accessKeySecret); err != nil {
			log.Fatalf("Error sending payloads to User_Master: %v", err)
		}
		fmt.Printf("Successfully sent %d 'Others' payloads to User_Master.\n", len(payloads))
	} else {
		fmt.Println("No payloads generated to send to User_Master for 'Others'.")
	}
}
