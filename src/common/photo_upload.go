package common

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// UploadStudentPhoto uploads a JPEG image to the student's photo endpoint.
// The bearer must include the "Bearer " prefix.
func UploadStudentPhoto(schoolId string, bearer string, jpegBytes []byte) error {
	url := fmt.Sprintf("https://alice-smith.isamshosting.cloud/Main/api/students/%s/photos", schoolId)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(jpegBytes))
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Content-Type", "image/jpeg")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed: status=%d, body=%s", resp.StatusCode, string(b))
	}
	return nil
}
