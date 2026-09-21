package engine

import (
	"errors"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const (
	ConversionProtocolVersion     = 1
	CompactionProtocolVersion     = 1
	QueryExecutionProtocolVersion = 1
	DefaultNativeMemoryBytes      = int64(256 << 20)
	DefaultNativeSpillBytes       = int64(2 << 30)
	MaxBundleFileBytes            = model.MaxBundleFileBytes
)

type CompactionInput struct {
	BundleID       string `json:"bundle_id"`
	AnalyticsPath  string `json:"analytics_path"`
	PayloadPath    string `json:"payload_path"`
	IdentitySHA256 string `json:"identity_sha256"`
}

type CompactionRequest struct {
	Version           int               `json:"version"`
	TenantID          int64             `json:"tenant_id"`
	LaneID            int               `json:"lane_id"`
	SchemaVersion     int               `json:"schema_version"`
	GroupingVersion   int               `json:"grouping_version"`
	EventDay          string            `json:"event_day"`
	Kind              model.Kind        `json:"kind"`
	MinReceivedTimeUS int64             `json:"min_received_time_us,omitempty"`
	Inputs            []CompactionInput `json:"inputs"`
	OutputDirectory   string            `json:"output_directory"`
	SpillDirectory    string            `json:"spill_directory"`
	NativeMemoryBytes int64             `json:"native_memory_bytes"`
	NativeSpillBytes  int64             `json:"native_spill_bytes"`
}

type CompactionResult struct {
	DuckDBVersion string          `json:"duckdb_version"`
	Bundle        ConvertedBundle `json:"bundle"`
}

// StageRecord is the private supervisor-to-child format. It is disposable,
// versioned, and contains only records selected by durable receipt state.
type StageRecord struct {
	Version           int          `json:"version"`
	GlobalOrdinal     int          `json:"global_ordinal"`
	BatchID           string       `json:"batch_id"`
	LaneID            int          `json:"lane_id"`
	BatchSeq          int64        `json:"batch_seq"`
	ReceivedTimeUS    int64        `json:"received_time_us"`
	GroupingVersion   int          `json:"grouping_version"`
	IssueID           string       `json:"issue_id,omitempty"`
	FingerprintSHA256 string       `json:"fingerprint_sha256,omitempty"`
	IssueTitle        string       `json:"issue_title,omitempty"`
	Record            model.Record `json:"record"`
}

type ConversionRequest struct {
	Version           int    `json:"version"`
	StagePath         string `json:"stage_path"`
	OutputDirectory   string `json:"output_directory"`
	SpillDirectory    string `json:"spill_directory"`
	TenantID          int64  `json:"tenant_id"`
	LaneID            int    `json:"lane_id"`
	BatchSeq          int64  `json:"batch_seq"`
	BatchID           string `json:"batch_id"`
	SelectedRecords   int    `json:"selected_records"`
	SelectedErrors    int    `json:"selected_errors"`
	NativeMemoryBytes int64  `json:"native_memory_bytes"`
	NativeSpillBytes  int64  `json:"native_spill_bytes"`
}

func (request *ConversionRequest) applyDefaults() {
	if request.NativeMemoryBytes == 0 {
		request.NativeMemoryBytes = DefaultNativeMemoryBytes
	}
	if request.NativeSpillBytes == 0 {
		request.NativeSpillBytes = DefaultNativeSpillBytes
	}
}

func (request ConversionRequest) validate() error {
	if request.Version != ConversionProtocolVersion || request.TenantID <= 0 || request.LaneID < 0 || request.LaneID >= model.LaneCount || request.BatchSeq <= 0 || request.BatchID == "" || request.SelectedRecords < 0 || request.SelectedErrors < 0 || request.SelectedErrors > request.SelectedRecords {
		return errors.New("invalid conversion request scope or counts")
	}
	if !filepath.IsAbs(request.StagePath) || !filepath.IsAbs(request.OutputDirectory) || !filepath.IsAbs(request.SpillDirectory) || request.OutputDirectory == request.SpillDirectory {
		return errors.New("conversion paths must be distinct absolute paths")
	}
	if request.NativeMemoryBytes < 32<<20 || request.NativeMemoryBytes > DefaultNativeMemoryBytes || request.NativeSpillBytes < 64<<20 || request.NativeSpillBytes > DefaultNativeSpillBytes {
		return errors.New("conversion native limits are outside the supported range")
	}
	return nil
}

type ConvertedFile struct {
	Path              string               `json:"path"`
	Evidence          storage.FileEvidence `json:"evidence"`
	RowCount          int64                `json:"row_count"`
	MinEventTimeUS    int64                `json:"min_event_time_us"`
	MaxEventTimeUS    int64                `json:"max_event_time_us"`
	MinReceivedTimeUS int64                `json:"min_received_time_us"`
	MaxReceivedTimeUS int64                `json:"max_received_time_us"`
	MinBatchSeq       int64                `json:"min_batch_seq"`
	MaxBatchSeq       int64                `json:"max_batch_seq"`
}

type ConvertedBundle struct {
	Index          int           `json:"index"`
	EventDay       string        `json:"event_day"`
	Kind           model.Kind    `json:"kind"`
	RowCount       int64         `json:"row_count"`
	IdentitySHA256 string        `json:"identity_sha256"`
	ProjectIDs     []int64       `json:"project_ids"`
	Analytics      ConvertedFile `json:"analytics"`
	Payload        ConvertedFile `json:"payload"`
}

type ConversionSummary struct {
	DuckDBVersion          string `json:"duckdb_version"`
	SelectedRecordCount    int    `json:"selected_record_count"`
	SelectedErrorCount     int    `json:"selected_error_count"`
	BundleCount            int    `json:"bundle_count"`
	SelectedIdentitySHA256 string `json:"selected_identity_sha256"`
}

type ConversionMessage struct {
	Version int                `json:"version"`
	Type    string             `json:"type"`
	Bundle  *ConvertedBundle   `json:"bundle,omitempty"`
	Summary *ConversionSummary `json:"summary,omitempty"`
}
