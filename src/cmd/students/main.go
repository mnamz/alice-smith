package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"isams_to_sheets/src/common"

	"github.com/joho/godotenv"
	"golang.org/x/image/draw"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const (
	STUDENTS_API = "https://alice-smith.isamshosting.cloud/Main/api/students"
)

type BearerTokenResponse struct {
	BearerToken string `json:"bearer_token"`
}

type Student struct {
	SchoolId    string      `json:"schoolId"`
	FullName    string      `json:"fullName"`
	DateOfBirth string      `json:"dob"`
	Gender      string      `json:"gender"`
	FormGroup   string      `json:"formGroup"`
	YearGroup   interface{} `json:"yearGroup"`
	Email       string      `json:"schoolEmailAddress"`
}

type StudentsResponse struct {
	Count    int       `json:"count"`
	Page     int       `json:"page"`
	PageSize int       `json:"pageSize"`
	Students []Student `json:"students"`
}

type Photo struct {
	Base64Data     string
	OriginalSize   int
	CompressedSize int
	Status         string
}

func (p *Photo) IsValid() bool {
	return p.Status == "ok" && p.Base64Data != ""
}

func getBearerToken(apiKeyUrl string) (string, error) {
	resp, err := http.Get(apiKeyUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var tokenResp BearerTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", err
	}
	return tokenResp.BearerToken, nil
}

func fetchAllStudents(bearer string) ([]Student, error) {
	students := []Student{}
	page := 1
	for {
		url := fmt.Sprintf("%s?page=%d&pageSize=%d", STUDENTS_API, page, common.PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Authorization", bearer)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var sr StudentsResponse
		if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
			return nil, err
		}
		// Log each fetched student's SchoolId for debugging purposes
		for _, student := range sr.Students {
			log.Printf("Fetched student SchoolId: %s", student.SchoolId)
		}
		students = append(students, sr.Students...)
		if len(sr.Students) < common.PAGE_SIZE {
			break
		}
		page++
	}
	return students, nil
}

func fetchPhoto(schoolId, bearer string) (*Photo, error) {
	url := fmt.Sprintf("https://alice-smith.isamshosting.cloud/Main/api/students/%s/photos/current", schoolId)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", bearer)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return &Photo{Status: "download error"}, err
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "image/") {
		log.Printf("schoolId %s: Unexpected Content-Type: %s", schoolId, contentType)
		return &Photo{Status: "not image"}, nil
	}

	imgBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return &Photo{Status: "read error"}, err
	}

	photo := &Photo{
		OriginalSize: len(imgBytes),
		Status:       "processing",
	}

	var img image.Image
	img, decodeErr := jpeg.Decode(bytes.NewReader(imgBytes))
	if decodeErr != nil {
		img, decodeErr = png.Decode(bytes.NewReader(imgBytes))
	}
	if decodeErr != nil {
		log.Printf("schoolId %s: decode error: %v, Content-Type: %s, first bytes: % x", schoolId, decodeErr, contentType, imgBytes[:min(16, len(imgBytes))])
		filename := fmt.Sprintf("failed_photo_%s.bin", schoolId)
		_ = os.WriteFile(filename, imgBytes, 0644)
		return &Photo{Status: "decode error"}, nil
	}

	var buf bytes.Buffer
	quality := 40
	if photo.OriginalSize > 102400 {
		quality = 40
	}
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return &Photo{Status: "compression error"}, nil
	}

	compressed := buf.Bytes()
	photo.CompressedSize = len(compressed)

	if photo.CompressedSize > 37500 {
		// Resize to max 400x600 and try again
		maxW, maxH := 400, 600
		bounds := img.Bounds()
		w, h := bounds.Dx(), bounds.Dy()
		newW, newH := w, h
		if w > maxW || h > maxH {
			ratioW := float64(maxW) / float64(w)
			ratioH := float64(maxH) / float64(h)
			ratio := ratioW
			if ratioH < ratioW {
				ratio = ratioH
			}
			newW = int(float64(w) * ratio)
			newH = int(float64(h) * ratio)
			if newW < 1 {
				newW = 1
			}
			if newH < 1 {
				newH = 1
			}
		}
		resized := image.NewRGBA(image.Rect(0, 0, newW, newH))
		draw.CatmullRom.Scale(resized, resized.Bounds(), img, bounds, draw.Over, nil)
		buf.Reset()
		if err := jpeg.Encode(&buf, resized, &jpeg.Options{Quality: quality}); err != nil {
			return &Photo{Status: "resize/compression error"}, nil
		}
		compressed = buf.Bytes()
		photo.CompressedSize = len(compressed)
		if photo.CompressedSize > 37500 {
			return &Photo{Status: "too large after resize"}, nil
		}
	}

	photo.Base64Data = base64.StdEncoding.EncodeToString(compressed)
	photo.Status = "ok"
	return photo, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// cardNoMap maps student SchoolId to proximity card number from the CSV export.
var cardNoMap map[string]string

// loadCardNoMap builds a lookup of schoolId -> cardNo from the CSV. It
// considers column 0 as the ID and column 9 as the card number. Card numbers
// beginning with "000000" are ignored as they represent duplicates/invalids.
func loadCardNoMap(csvPath string) (map[string]string, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("open CSV: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	result := make(map[string]string)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV: %w", err)
		}
		if len(rec) <= 9 {
			continue
		}
		id := strings.TrimSpace(rec[0])
		card := strings.TrimSpace(rec[9])
		if id == "" || card == "" || strings.HasPrefix(card, "0000") {
			continue
		}
		if _, ok := result[id]; !ok {
			result[id] = card
		}
	}
	return result, nil
}

func mapStudentToRow(s Student, photo *common.Photo) []interface{} {
	gender := "2"
	if s.Gender == "M" {
		gender = "1"
	}
	yearGroupStr := fmt.Sprintf("%v", s.YearGroup)
	photoData := ""
	origSize := 0
	compSize := 0
	status := "error"

	if photo != nil {
		photoData = photo.Base64Data
		origSize = photo.OriginalSize
		compSize = photo.CompressedSize
		status = photo.Status
	}

	cardNo := ""
	if cardNoMap != nil {
		cardNo = cardNoMap[s.SchoolId]
	}

	return []interface{}{
		s.SchoolId,    // schoolId
		s.FullName,    // Name
		"1",           // type
		"",            // jobTitle
		s.FormGroup,   // department
		"",            // IdentityNo
		s.DateOfBirth, // DateOfBirth
		"1",           // IdentityType
		"1",           // Status
		gender,        // Gender
		s.FormGroup,   // FormGroup
		yearGroupStr,  // YearGroup
		cardNo,        // CardNo
		photoData,     // photo
		origSize,      // photo_original_size
		compSize,      // photo_compressed_size
		status,        // photo_status
	}
}

func writeBatchesToSheet(srv *sheets.Service, values [][]interface{}, batchSize int) error {
	rowStart := 1
	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}
		batch := values[i:end]
		rowEnd := rowStart + len(batch) - 1
		colEnd := string(rune('A' + len(batch[0]) - 1))
		rangeStr := fmt.Sprintf("%s!A%d:%s%d", common.SHEET_NAME_STUDENTS, rowStart, colEnd, rowEnd)
		vr := &sheets.ValueRange{Values: batch}
		_, err := srv.Spreadsheets.Values.Update(common.SPREADSHEET_ID, rangeStr, vr).ValueInputOption("RAW").Do()
		if err != nil {
			return err
		}
		rowStart = rowEnd + 1
	}
	return nil
}

func mapStudentToUserMasterPayload(s Student, photo *common.Photo) map[string]interface{} {
	schoolId := s.SchoolId
	gender := "2"
	if s.Gender == "M" {
		gender = "1"
	}
	yearGroupStr := fmt.Sprintf("%v", s.YearGroup)
	jobTitle := "JB"
	if yearGroupStr == "7" || yearGroupStr == "8" || yearGroupStr == "9" || yearGroupStr == "10" || yearGroupStr == "11" || yearGroupStr == "12" || yearGroupStr == "13" {
		jobTitle = "EP"
	}
	payload := map[string]interface{}{
		"_id":          schoolId,
		"Name":         schoolId,
		"Name_1":       strings.ToUpper(s.FullName),
		"Type":         "1",
		"Job_Title":    jobTitle,
		"Department":   s.FormGroup,
		"IdentityNo":   "",
		"IdentityType": "1",
		"FormGroup":    s.FormGroup,
		"YearGroup":    yearGroupStr,
		"Gender":       gender,
		"DateOfBirth":  "",
		"Status":       "1",
		"AccessGroup":  "STUDENTS",
	}

	if photo != nil && photo.IsValid() {
		payload["image_1"] = photo.Base64Data
	}

	cardNo := ""
	if cardNoMap != nil {
		cardNo = cardNoMap[schoolId]
	}
	payload["CardNo"] = cardNo

	return payload
}

// Add a helper that identifies inactive students currently present in User_Master but not returned by the Students API.
func getInactiveStudents(accessKeyId, accessKeySecret string) ([]map[string]string, error) {
	// Obtain bearer token so we can fetch the full list of students.
	apiKeyUrl := os.Getenv("API_KEY_URL")
	if apiKeyUrl == "" {
		return nil, fmt.Errorf("API_KEY_URL environment variable is not set")
	}

	bearerToken, err := getBearerToken(apiKeyUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to get bearer token: %w", err)
	}
	bearer := "Bearer " + bearerToken

	// Fetch all students from the Students API
	students, err := fetchAllStudents(bearer)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch students: %w", err)
	}

	// Build a set of active student SchoolIds for quick lookup
	activeIds := make(map[string]bool)
	for _, s := range students {
		activeIds[s.SchoolId] = true
	}

	// Iterate through the User_Master dataset and collect records that are not active
	var recordsToDelete []map[string]string
	page := 1
	for {
		url := fmt.Sprintf("https://alice-smith.kissflow.com/dataset/2/AcflcLIlo4aq/User_Master/view/Students/list?page_number=%d&page_size=999&search_field=Name", page)
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

		// Break if no more records are returned
		if len(result.Data) == 0 {
			break
		}

		// Collect records that are not in the active student set
		for _, rec := range result.Data {
			if !activeIds[rec.Name] {
				recordsToDelete = append(recordsToDelete, map[string]string{
					"_id":  rec.ID,
					"Name": rec.Name,
				})
			}
		}

		// If fewer than the page size was returned, we've reached the end
		if len(result.Data) < 999 {
			break
		}
		page++
	}

	return recordsToDelete, nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("WARNING: .env file not loaded:", err)
	} else {
		fmt.Println(".env file loaded successfully")
	}

	apiKeyUrl := os.Getenv("API_KEY_URL")
	if apiKeyUrl == "" {
		log.Fatal("API_KEY_URL environment variable is not set")
	}

	accessKeyId := os.Getenv("X_ACCESS_KEY_ID_VALUE")
	if accessKeyId == "" {
		log.Fatal("X_ACCESS_KEY_ID environment variable is not set")
	}

	accessKeySecret := os.Getenv("X_ACCESS_KEY_SECRET_VALUE")
	if accessKeySecret == "" {
		log.Fatal("X_ACCESS_KEY_SECRET environment variable is not set")
	}

	fmt.Println("DEBUG API_KEY_URL:", apiKeyUrl)
	start := time.Now()
	ctx := context.Background()

	// Build CardNo lookup before further processing
	var err error
	cardNoMap, err = loadCardNoMap("P1 User July.csv")
	if err != nil {
		fmt.Println("WARNING: could not build CardNo lookup:", err)
	}

	// Load service account
	b, err := os.ReadFile("api.json")
	if err != nil {
		log.Fatalf("Unable to read service account file: %v", err)
	}
	config, err := google.JWTConfigFromJSON(b, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("Unable to parse service account file: %v", err)
	}
	client := config.Client(ctx)

	// Get bearer token
	bearer, err := getBearerToken(apiKeyUrl)
	if err != nil {
		log.Fatalf("Unable to get bearer token: %v", err)
	}
	bearer = "Bearer " + bearer

	// Fetch students
	students, err := fetchAllStudents(bearer)
	if err != nil {
		log.Fatalf("Unable to fetch students: %v", err)
	}

	// Prepare payloads for User_Master batch
	var payloads []map[string]interface{}
	for _, s := range students {
		photo, err := common.FetchStudentPhoto(s.SchoolId, bearer)
		if err != nil {
			log.Printf("Warning: could not fetch photo for schoolId %s: %v", s.SchoolId, err)
			photo = nil
		}
		payloads = append(payloads, mapStudentToUserMasterPayload(s, photo))
	}

	// delete inactive students from User_Master
	recordsToDelete, err := getInactiveStudents(accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to get inactive students: %v", err)
	}
	err = common.DeleteAllUserMaster(accessKeyId, accessKeySecret, recordsToDelete)
	if err != nil {
		log.Fatalf("Failed to delete inactive students: %v", err)
	}

	// Send to User_Master/batch endpoint
	err = common.SendToUserMasterBatch(payloads, accessKeyId, accessKeySecret)
	if err != nil {
		log.Fatalf("Failed to send to User_Master batch endpoint: %v", err)
	}

	// Prepare data for sheets
	headers := []interface{}{"schoolId", "Name", "type", "jobTitle", "department", "IdentityNo", "DateOfBirth", "IdentityType", "Status", "Gender", "FormGroup", "YearGroup", "CardNo", "photo", "photo_original_size", "photo_compressed_size", "photo_status"}
	values := [][]interface{}{headers}

	for _, s := range students {
		photo, err := common.FetchStudentPhoto(s.SchoolId, bearer)
		if err != nil {
			log.Printf("Warning: could not fetch photo for schoolId %s: %v", s.SchoolId, err)
			photo = nil
		}
		values = append(values, mapStudentToRow(s, photo))
	}

	// Write to Google Sheets
	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}

	// Clear the sheet first
	clearReq := &sheets.ClearValuesRequest{}
	_, err = srv.Spreadsheets.Values.Clear(common.SPREADSHEET_ID, common.SHEET_NAME_STUDENTS, clearReq).Do()
	if err != nil {
		log.Fatalf("Unable to clear sheet: %v", err)
	}

	batchSize := 50
	err = writeBatchesToSheet(srv, values, batchSize)
	if err != nil {
		log.Fatalf("Unable to write to sheet in batches: %v", err)
	}

	fmt.Printf("Done! Wrote %d students to the sheet in %s.\n", len(students), time.Since(start))
}
