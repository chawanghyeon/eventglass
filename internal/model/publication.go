package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

const (
	OutputManifestVersion  = 1
	MaxManifestHeaderBytes = 64 << 10
	MaxManifestPartBytes   = 64 << 10
	MaxManifestBytes       = 32 << 20
	FileBlockBytes         = 1 << 20
)

type OutputManifestHeader struct {
	Version                 int    `json:"version"`
	OutputID                string `json:"output_id"`
	JobID                   string `json:"job_id"`
	TenantID                int64  `json:"tenant_id"`
	LaneID                  int    `json:"lane_id"`
	BatchSeq                int64  `json:"batch_seq"`
	JournalSHA256           string `json:"journal_sha256"`
	ReceiptSetSHA256        string `json:"receipt_set_sha256"`
	SelectedIdentitySHA256  string `json:"selected_identity_sha256"`
	OccurrenceSummarySHA256 string `json:"occurrence_summary_sha256"`
	GroupingVersion         int    `json:"grouping_version"`
	SelectedRecordCount     int    `json:"selected_record_count"`
	SelectedErrorCount      int    `json:"selected_error_count"`
	BundleCount             int    `json:"bundle_count"`
}

type OutputManifestPartRef struct {
	Index  int    `json:"index"`
	SHA256 string `json:"sha256"`
}

type OutputManifestRoot struct {
	Version int                     `json:"version"`
	Header  OutputManifestHeader    `json:"header"`
	Parts   []OutputManifestPartRef `json:"parts"`
}

type OutputManifestPart struct {
	Version int              `json:"version"`
	Index   int              `json:"index"`
	Bundles []BundleManifest `json:"bundles"`
}

type BundleManifest struct {
	BundleID       string       `json:"bundle_id"`
	EventDay       string       `json:"event_day"`
	Kind           Kind         `json:"kind"`
	InputSeqMin    int64        `json:"input_seq_min"`
	InputSeqMax    int64        `json:"input_seq_max"`
	RowCount       int64        `json:"row_count"`
	IdentitySHA256 string       `json:"identity_sha256"`
	ProjectIDs     []int64      `json:"project_ids"`
	Analytics      FileManifest `json:"analytics"`
	Payload        FileManifest `json:"payload"`
}

type FileManifest struct {
	FileID            string              `json:"file_id"`
	IntentID          string              `json:"intent_id"`
	Role              string              `json:"role"`
	Bytes             int64               `json:"bytes"`
	SHA256            string              `json:"sha256"`
	RowCount          int64               `json:"row_count"`
	MinEventTimeUS    int64               `json:"min_event_time_us"`
	MaxEventTimeUS    int64               `json:"max_event_time_us"`
	MinReceivedTimeUS int64               `json:"min_received_time_us"`
	MaxReceivedTimeUS int64               `json:"max_received_time_us"`
	MinBatchSeq       int64               `json:"min_batch_seq"`
	MaxBatchSeq       int64               `json:"max_batch_seq"`
	Blocks            []FileBlockManifest `json:"blocks"`
}

type FileBlockManifest struct {
	Index  int    `json:"index"`
	SHA256 string `json:"sha256"`
}

type IssueOccurrenceSummary struct {
	RecordID          string  `json:"record_id"`
	ProjectID         int64   `json:"project_id"`
	AcceptanceID      string  `json:"acceptance_id"`
	LaneID            int     `json:"lane_id"`
	BatchSeq          int64   `json:"batch_seq"`
	Ordinal           int     `json:"ordinal"`
	EventTimeUS       int64   `json:"event_time_us"`
	EventNS           uint16  `json:"event_ns"`
	ReceivedTimeUS    int64   `json:"received_time_us"`
	ReleaseJSON       *string `json:"release_json"`
	IssueID           string  `json:"issue_id"`
	GroupingVersion   int     `json:"grouping_version"`
	FingerprintSHA256 string  `json:"fingerprint_sha256"`
	TitleJSON         string  `json:"title_json"`
}

func (root OutputManifestRoot) CanonicalJSON() ([]byte, error) {
	if root.Version != OutputManifestVersion || root.Header.Version != OutputManifestVersion || root.Header.OutputID == "" || root.Header.JobID == "" || root.Header.TenantID <= 0 || root.Header.LaneID < 0 || root.Header.LaneID >= LaneCount || root.Header.BatchSeq <= 0 || root.Header.GroupingVersion <= 0 || root.Header.SelectedRecordCount < 0 || root.Header.SelectedErrorCount < 0 || root.Header.SelectedErrorCount > root.Header.SelectedRecordCount || root.Header.BundleCount < 0 {
		return nil, errors.New("invalid output manifest header")
	}
	for _, value := range []string{root.Header.JournalSHA256, root.Header.ReceiptSetSHA256, root.Header.SelectedIdentitySHA256, root.Header.OccurrenceSummarySHA256} {
		if !validManifestSHA(value) {
			return nil, errors.New("invalid output manifest digest")
		}
	}
	if root.Header.BundleCount == 0 && (root.Header.SelectedRecordCount != 0 || len(root.Parts) != 0) {
		return nil, errors.New("empty output manifest has records or parts")
	}
	for index, part := range root.Parts {
		if part.Index != index || !validManifestSHA(part.SHA256) {
			return nil, errors.New("manifest part references must be contiguous and hashed")
		}
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(root); err != nil {
		return nil, err
	}
	encoded := bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'})
	if len(encoded) > MaxManifestHeaderBytes {
		return nil, errors.New("output manifest root exceeds header limit")
	}
	return encoded, nil
}

func (root OutputManifestRoot) SHA256() (string, error) {
	encoded, err := root.CanonicalJSON()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validManifestSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == bytesToLowerASCII([]byte(value))
}

func bytesToLowerASCII(value []byte) string {
	for index, character := range value {
		if character >= 'A' && character <= 'F' {
			value[index] = character + ('a' - 'A')
		}
	}
	return string(value)
}
