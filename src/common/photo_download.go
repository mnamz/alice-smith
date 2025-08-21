package common

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DownloadStudentPhotoBytes downloads the current student photo as raw bytes without
// any compression or resizing. Returns the bytes and the content type.
func DownloadStudentPhotoBytes(schoolId, bearer string) ([]byte, string, error) {
	url := fmt.Sprintf("https://alice-smith.isamshosting.cloud/Main/api/students/%s/photos/current", schoolId)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", bearer)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "image/") {
		return nil, contentType, fmt.Errorf("unexpected content type: %s", contentType)
	}

	imgBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, contentType, err
	}
	return imgBytes, contentType, nil
}
