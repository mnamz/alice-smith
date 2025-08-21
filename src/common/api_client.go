package common

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Cache to store processed results
var (
	cache     = make(map[string]familyStatus)
	cacheLock sync.RWMutex
)

type familyStatus struct {
	ParentMembershipNo string
	IsActive           bool
}

// extractDigits extracts only the digits from a string
func extractDigits(s string) string {
	var result strings.Builder
	for _, char := range s {
		if char >= '0' && char <= '9' {
			result.WriteRune(char)
		}
	}
	return result.String()
}

// CheckActiveStatus checks if a membership number is active by looking up in the Kissflow export CSV
// Returns:
// - parentMembershipNo: The full membership number (including letters)
// - isActive: true if ANY student in the family has status other than "Former"
// - error: any error that occurred during processing
func CheckActiveStatus(membershipNo string) (string, bool, error) {
	// First check cache
	cacheLock.RLock()
	if status, exists := cache[membershipNo]; exists {
		cacheLock.RUnlock()
		return status.ParentMembershipNo, status.IsActive, nil
	}
	cacheLock.RUnlock()

	// Open the Kissflow export CSV file
	workspaceRoot := "/Users/aliifz/projects/alice-smith/klass-scripts"
	file, err := os.Open(filepath.Join(workspaceRoot, "Kissflow_export.csv"))
	if err != nil {
		return "", false, fmt.Errorf("error opening Kissflow export file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header to find column indices
	headers, err := reader.Read()
	if err != nil {
		return "", false, fmt.Errorf("error reading CSV header: %v", err)
	}

	// Find required column indices
	var parentMembershipNoIdx, enrollmentStatusIdx int
	for i, header := range headers {
		switch header {
		case "parentMembershipNo":
			parentMembershipNoIdx = i
		case "enrollmentStatus":
			enrollmentStatusIdx = i
		}
	}

	// Group records by parent membership number (digits only)
	familyGroups := make(map[string][]string)    // map[membershipDigits][]enrollmentStatus
	fullMembershipNos := make(map[string]string) // map[membershipDigits]fullMembershipNo

	// Process each row
	for {
		record, err := reader.Read()
		if err != nil {
			break // End of file or error
		}

		if len(record) <= parentMembershipNoIdx || len(record) <= enrollmentStatusIdx {
			continue // Skip invalid rows
		}

		parentMembershipNo := record[parentMembershipNoIdx]
		enrollmentStatus := record[enrollmentStatusIdx]

		// Extract digits for comparison
		membershipDigits := extractDigits(parentMembershipNo)
		searchDigits := extractDigits(membershipNo)

		if membershipDigits == searchDigits {
			familyGroups[membershipDigits] = append(familyGroups[membershipDigits], enrollmentStatus)
			fullMembershipNos[membershipDigits] = parentMembershipNo
		}
	}

	// Process results
	searchDigits := extractDigits(membershipNo)
	if statuses, found := familyGroups[searchDigits]; found {
		fullMembershipNo := fullMembershipNos[searchDigits]

		// Check if all statuses are "Former"
		allFormer := true
		for _, status := range statuses {
			if status != "Former" {
				allFormer = false
				break
			}
		}

		// Cache the result
		cacheLock.Lock()
		cache[membershipNo] = familyStatus{
			ParentMembershipNo: fullMembershipNo,
			IsActive:           !allFormer,
		}
		cacheLock.Unlock()

		return fullMembershipNo, !allFormer, nil
	}

	return "", false, nil // No matching records found
}
