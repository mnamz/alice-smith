package common

import (
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	studentsAPI = "https://alice-smith.isamshosting.cloud/Main/api/students"
)

type bearerTokenResponse struct {
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

type studentsResponse struct {
	Count    int       `json:"count"`
	Page     int       `json:"page"`
	PageSize int       `json:"pageSize"`
	Students []Student `json:"students"`
}

// GetBearerToken retrieves the bearer token string from the provided API key URL.
func GetBearerToken(apiKeyUrl string) (string, error) {
	resp, err := http.Get(apiKeyUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var tokenResp bearerTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", err
	}
	return tokenResp.BearerToken, nil
}

// FetchAllStudents pages through the Students API and returns all students.
func FetchAllStudents(bearer string) ([]Student, error) {
	students := []Student{}
	page := 1
	for {
		url := fmt.Sprintf("%s?page=%d&pageSize=%d", studentsAPI, page, PAGE_SIZE)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Authorization", bearer)
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var sr studentsResponse
		if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
			return nil, err
		}
		students = append(students, sr.Students...)
		if len(sr.Students) < PAGE_SIZE {
			break
		}
		page++
	}
	return students, nil
}
