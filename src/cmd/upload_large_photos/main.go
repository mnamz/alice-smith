package main

import (
	"bufio"
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"log"
	"os"
	"path/filepath"
	"strings"

	"isams_to_sheets/src/common"

	"github.com/joho/godotenv"
)

// compressFromCurrent fetches the current photo and returns JPEG bytes
// compressed using the same logic as FetchStudentPhoto but returns the bytes
// rather than base64.
func compressFromCurrent(schoolId string, bearer string) ([]byte, error) {
	raw, ct, err := common.DownloadStudentPhotoBytes(schoolId, bearer)
	if err != nil {
		return nil, fmt.Errorf("download photo failed: %v", err)
	}

	var img image.Image
	// Try decode based on content type first
	if strings.Contains(ct, "jpeg") || strings.Contains(ct, "jpg") {
		img, err = jpeg.Decode(bytes.NewReader(raw))
	} else if strings.Contains(ct, "png") {
		img, err = png.Decode(bytes.NewReader(raw))
	} else {
		// Fallback: try jpeg then png
		img, err = jpeg.Decode(bytes.NewReader(raw))
		if err != nil {
			img, err = png.Decode(bytes.NewReader(raw))
		}
	}
	if err != nil {
		return nil, fmt.Errorf("decode failed: %v", err)
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 40}); err != nil {
		return nil, fmt.Errorf("encode failed: %v", err)
	}
	return buf.Bytes(), nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("WARNING: .env file not loaded:", err)
	}

	apiKeyUrl := os.Getenv("API_KEY_URL")
	if apiKeyUrl == "" {
		log.Fatal("API_KEY_URL environment variable is not set")
	}

	bearer, err := common.GetBearerToken(apiKeyUrl)
	if err != nil {
		log.Fatalf("Unable to get bearer token: %v", err)
	}
	bearer = "Bearer " + bearer

	// Read the list produced by largephotos script
	listPath := filepath.Join("students_large_photos.txt")
	f, err := os.Open(listPath)
	if err != nil {
		log.Fatalf("Unable to open %s: %v", listPath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		line := scanner.Text()
		lineNo++
		if lineNo == 1 {
			// skip header
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) == 0 {
			continue
		}
		schoolId := strings.TrimSpace(parts[0])
		if schoolId == "" {
			continue
		}

		jpegBytes, err := compressFromCurrent(schoolId, bearer)
		if err != nil {
			log.Printf("%s: compress error: %v", schoolId, err)
			continue
		}

		if err := common.UploadStudentPhoto(schoolId, bearer, jpegBytes); err != nil {
			log.Printf("%s: upload failed: %v", schoolId, err)
			continue
		}
		log.Printf("%s: upload ok", schoolId)
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("scan error: %v", err)
	}
}
