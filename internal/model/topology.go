package model

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

const LaneCount = 16

// LaneForAcceptance hashes the decoded UUID bytes, not an SDK event ID.
func LaneForAcceptance(id string) (int, error) {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return 0, errors.New("acceptance ID must be UUIDv4")
	}
	value, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil || len(value) != 16 || value[6]>>4 != 4 || value[8]>>6 != 2 || id != strings.ToLower(id) {
		return 0, errors.New("acceptance ID must be canonical UUIDv4")
	}
	hash := sha256.Sum256(value)
	return int(hash[0]) % LaneCount, nil
}

// JournalBatch is a physical object containing whole requests from one lane.
// Requests may belong to different projects of the same tenant.
type JournalBatch struct {
	BatchID  string
	TenantID int64
	LaneID   int
	Requests []NormalizedRequest
}
