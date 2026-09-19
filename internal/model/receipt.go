package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

const ReceiptSelectionVersion = 1

type ReceiptClass uint8

const (
	ReceiptAccepted ReceiptClass = iota + 1
	ReceiptDuplicate
	ReceiptConflict
)

type ReceiptRange [2]int

type ReceiptSelection struct {
	Version   int            `json:"version"`
	Accepted  []ReceiptRange `json:"accepted"`
	Duplicate []ReceiptRange `json:"duplicate"`
	Conflict  []ReceiptRange `json:"conflict"`
}

// JournalRequestIndex is trusted only after the complete journal write or
// replay succeeds. Ordinals are global positions in that physical journal.
type JournalRequestIndex struct {
	AcceptanceID     string
	ProjectID        int64
	OrdinalFirst     int
	OrdinalLast      int
	RecordCount      int
	ContentSHA256    string
	Outcomes         []Outcome
	UnsupportedItems []UnsupportedItem
}

// NewReceiptSelection converts one class per request-local record position
// into the exact, maximally coalesced v1 receipt representation.
func NewReceiptSelection(classes []ReceiptClass) (ReceiptSelection, error) {
	selection := ReceiptSelection{
		Version:   ReceiptSelectionVersion,
		Accepted:  make([]ReceiptRange, 0),
		Duplicate: make([]ReceiptRange, 0),
		Conflict:  make([]ReceiptRange, 0),
	}
	for position, class := range classes {
		var ranges *[]ReceiptRange
		switch class {
		case ReceiptAccepted:
			ranges = &selection.Accepted
		case ReceiptDuplicate:
			ranges = &selection.Duplicate
		case ReceiptConflict:
			ranges = &selection.Conflict
		default:
			return ReceiptSelection{}, errors.New("invalid receipt class")
		}
		if len(*ranges) > 0 && (*ranges)[len(*ranges)-1][1]+1 == position {
			(*ranges)[len(*ranges)-1][1] = position
		} else {
			*ranges = append(*ranges, ReceiptRange{position, position})
		}
	}
	return selection, nil
}

func (selection ReceiptSelection) CanonicalJSON(recordCount int) ([]byte, error) {
	if err := selection.Validate(recordCount); err != nil {
		return nil, err
	}
	return json.Marshal(selection)
}

func (selection ReceiptSelection) SHA256(recordCount int) (string, error) {
	encoded, err := selection.CanonicalJSON(recordCount)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (selection ReceiptSelection) Validate(recordCount int) error {
	if selection.Version != ReceiptSelectionVersion || recordCount < 0 || selection.Accepted == nil || selection.Duplicate == nil || selection.Conflict == nil {
		return errors.New("invalid receipt selection envelope")
	}
	classes := make([]ReceiptClass, recordCount)
	for class, ranges := range map[ReceiptClass][]ReceiptRange{
		ReceiptAccepted: selection.Accepted, ReceiptDuplicate: selection.Duplicate, ReceiptConflict: selection.Conflict,
	} {
		previousLast := -2
		for _, span := range ranges {
			if span[0] < 0 || span[1] < span[0] || span[1] >= recordCount || span[0] <= previousLast+1 {
				return errors.New("receipt ranges are invalid or not maximally coalesced")
			}
			for position := span[0]; position <= span[1]; position++ {
				if classes[position] != 0 {
					return errors.New("receipt ranges overlap")
				}
				classes[position] = class
			}
			previousLast = span[1]
		}
	}
	for _, class := range classes {
		if class == 0 {
			return errors.New("receipt ranges do not partition records")
		}
	}
	return nil
}
