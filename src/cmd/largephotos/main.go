package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"isams_to_sheets/src/common"

	"github.com/joho/godotenv"
)

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

	students, err := common.FetchAllStudents(bearer)
	if err != nil {
		log.Fatalf("Unable to fetch students: %v", err)
	}

	thresholdBytes := 150 * 1024 // 150KB
	outputPath := "students_large_photos.txt"
	f, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("Unable to create output file: %v", err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	// Write header
	_, _ = w.WriteString("SchoolId,FullName,OriginalSizeKB,CompressedSizeKB,Status\n")

	start := time.Now()
	count := 0
	for _, s := range students {
		photo, err := common.FetchStudentPhoto(s.SchoolId, bearer)
		if err != nil || photo == nil {
			continue
		}
		if photo.OriginalSize >= thresholdBytes {
			origKB := strconv.FormatFloat(float64(photo.OriginalSize)/1024.0, 'f', 1, 64)
			compKB := strconv.FormatFloat(float64(photo.CompressedSize)/1024.0, 'f', 1, 64)
			line := fmt.Sprintf("%s,%s,%s,%s,%s\n", s.SchoolId, s.FullName, origKB, compKB, photo.Status)
			_, _ = w.WriteString(line)
			count++
		}
	}

	log.Printf("Wrote %d records with photos >=150KB to %s in %s", count, outputPath, time.Since(start))
}
