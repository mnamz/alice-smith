package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/joho/godotenv"

	"isams_to_sheets/src/common"
)

// processID handles the ID processing according to the rules:
// 1. Get first 5 digits
// 2. Only digits
// 3. If letters appear, exclude digits after letters
// 4. If string starts with letter, get only digits
func processID(id string) string {
	var result strings.Builder

	// Check if starts with letter
	if len(id) > 0 && unicode.IsLetter(rune(id[0])) {
		// If starts with letter, just extract digits
		for _, char := range id {
			if unicode.IsDigit(char) {
				result.WriteRune(char)
				if result.Len() == 5 {
					break
				}
			}
		}
	} else {
		// Normal case: take digits until letter appears
		for _, char := range id {
			if unicode.IsLetter(char) {
				break
			}
			if unicode.IsDigit(char) {
				result.WriteRune(char)
				if result.Len() == 5 {
					break
				}
			}
		}
	}

	return result.String()
}

// fetchAllUserMasterParents retrieves all Parent view records from User_Master so they can be deleted.
func fetchAllUserMasterParents(accessKeyId, accessKeySecret string) ([]map[string]string, error) {
	var records []map[string]string
	page := 1
	for {
		url := fmt.Sprintf("https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/view/Parents/list?page_number=%d&page_size=%d&search_field=Name", page, common.PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("X-Access-Key-Id", accessKeyId)
		req.Header.Set("X-Access-Key-Secret", accessKeySecret)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch User_Master parents: %w", err)
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

	// Fetch existing parent records from User_Master and delete them
	recordsToDelete, err := fetchAllUserMasterParents(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to fetch parents for deletion: %v", err)
	}
	if len(recordsToDelete) > 0 {
		log.Printf("Deleting %d existing parent records from User_Master...", len(recordsToDelete))
		if err := common.DeleteAllUserMaster(accessKeyId, accessKeySecret, recordsToDelete); err != nil {
			log.Fatalf("Failed to delete User_Master parents: %v", err)
		}
	} else {
		log.Println("No existing parent records found to delete.")
	}

	// Open the input CSV file
	inputFile, err := os.Open(filepath.Join(workspaceRoot, "P1 User July.csv"))
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer inputFile.Close()

	// Create CSV reader
	reader := csv.NewReader(inputFile)
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	// Create output CSV file
	outputFile, err := os.Create(filepath.Join(workspaceRoot, "id_family_and_j.csv"))
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()

	// Create CSV writer
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"ID", "Processed ID", "Name", "Department", "Column J", "Parent Membership No", "Active Status"}); err != nil {
		fmt.Printf("Error writing header: %v\n", err)
		return
	}

	// Process each row
	rowCount := 0
	// Map to keep track of how many times each processed ID has appeared
	idCount := make(map[string]int)
	// Slice to accumulate payloads for User_Master batch API
	var payloads []map[string]interface{}
	for {
		record, err := reader.Read()
		if err != nil {
			break // End of file or error
		}

		// Check if we have enough columns (J is the 10th column, index 9)
		if len(record) > 9 {
			id := record[0]              // First column (ID)
			processedID := processID(id) // Process the ID according to rules
			// Increment occurrence count for this processed ID
			idCount[processedID]++
			processedIDWithCount := fmt.Sprintf("%s_%d", processedID, idCount[processedID])
			columnB := record[1] // Column B (Name)
			columnG := record[6] // Column G (Department)
			columnJ := record[9] // Column J

			// Convert to upper case once for both checks
			upperG := strings.ToUpper(columnG)

			// Check if column G contains "FAMILY" but not "EXP"
			if (strings.Contains(upperG, "FAMILY") || strings.Contains(upperG, "DRIVER")) && !strings.Contains(upperG, "FAMILY EXPERIENCE") {
				// Check active status
				parentMembershipNo, isActive, err := common.CheckActiveStatus(processedID)
				activeStatus := "Unknown"
				if err != nil {
					fmt.Printf("Warning: Error checking active status for %s: %v\n", columnB, err)
					parentMembershipNo = "Error"
				} else {
					if isActive {
						activeStatus = "Active"
					} else {
						activeStatus = "Inactive"
					}
				}

				// Determine Kissflow Status value (1 = Active, 2 = Inactive)
				statusVal := "2"
				if isActive {
					statusVal = "1"
				}

				// Build payload for User_Master batch
				payloads = append(payloads, map[string]interface{}{
					"_id":         processedIDWithCount,
					"Name":        processedIDWithCount,
					"Name_1":      strings.ToUpper(columnB),
					"Department":  columnG,
					"CardNo":      columnJ,
					"Type":        "2",
					"Status":      statusVal,
					"IdentityNo":  parentMembershipNo, // no special characters allowed
					"AccessGroup": "FAMILY",
				})

				if err := writer.Write([]string{id, processedIDWithCount, columnB, columnG, columnJ, parentMembershipNo, activeStatus}); err != nil {
					fmt.Printf("Error writing record: %v\n", err)
					return
				}

				rowCount++
				// Add a small delay every 10 rows to avoid overwhelming the API
				if rowCount%50 == 0 {
					time.Sleep(time.Second)
				}
			}
		}
	}

	// After processing CSV, send accumulated payloads to Kissflow User_Master batch API
	if len(payloads) > 0 {
		if err := common.SendToUserMasterBatch(payloads, accessKeyId, accessKeySecret); err != nil {
			fmt.Printf("Error sending payloads to User_Master: %v\n", err)
		} else {
			fmt.Printf("Successfully sent %d payloads to User_Master.\n", len(payloads))
		}
	} else {
		fmt.Println("No payloads generated to send to User_Master.")
	}

	fmt.Printf("Processing complete. Processed %d rows. Results saved to id_family_and_j.csv\n", rowCount)
}
