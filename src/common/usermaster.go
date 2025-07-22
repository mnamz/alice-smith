package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func DeleteAllUserMaster(accessKeyId, accessKeySecret, viewName string) error {
	// Fetch all User_Master records
	url := fmt.Sprintf("https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/view/%s/list", viewName)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Access-Key-Id", accessKeyId)
	req.Header.Set("X-Access-Key-Secret", accessKeySecret)
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
		req.Header.Set("X-Access-Key-Id", accessKeyId)
		req.Header.Set("X-Access-Key-Secret", accessKeySecret)
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

func SendToUserMasterBatch(payloads []map[string]interface{}, accessKeyId, accessKeySecret string) error {
	for i := 0; i < len(payloads); i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > len(payloads) {
			end = len(payloads)
		}
		batch := payloads[i:end]
		jsonPayload, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("failed to marshal batch payload: %w", err)
		}
		req, _ := http.NewRequest("POST", USER_MASTER_BATCH_API, bytes.NewReader(jsonPayload))
		req.Header.Set("X-Access-Key-Id", accessKeyId)
		req.Header.Set("X-Access-Key-Secret", accessKeySecret)
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
