package common

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"golang.org/x/image/draw"
)

type Photo struct {
	Base64Data     string
	OriginalSize   int
	CompressedSize int
	Status         string
}

func (p *Photo) IsValid() bool {
	return p.Status == "ok" && p.Base64Data != ""
}

func FetchStudentPhoto(schoolId, bearer string) (*Photo, error) {
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

	imgBytes, err := ioutil.ReadAll(resp.Body)
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
