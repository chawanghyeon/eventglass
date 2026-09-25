package model

import "encoding/json"

const (
	SchemaVersion       = 1
	NormalizerVersion   = 1
	ScrubVersion        = 1
	MaxCanonicalRecords = 10_000
)

type Kind string

const (
	KindError       Kind = "error"
	KindLog         Kind = "log"
	KindTransaction Kind = "transaction"
)

type Attribute struct {
	Namespace    string          `json:"namespace"`
	Path         string          `json:"path"`
	ValueType    string          `json:"value_type"`
	StringValue  *string         `json:"string_value,omitempty"`
	IntegerValue *string         `json:"integer_value,omitempty"`
	DoubleValue  *float64        `json:"double_value,omitempty"`
	BooleanValue *bool           `json:"boolean_value,omitempty"`
	JSONValue    json.RawMessage `json:"json_value,omitempty"`
	Unit         *string         `json:"unit,omitempty"`
}

type Record struct {
	TenantID             int64           `json:"tenant_id"`
	ProjectID            int64           `json:"project_id"`
	RecordID             string          `json:"record_id"`
	AcceptanceID         string          `json:"acceptance_id"`
	ItemOrdinal          int             `json:"item_ordinal"`
	RecordOrdinal        int             `json:"record_ordinal"`
	Kind                 Kind            `json:"kind"`
	EventTimeUS          int64           `json:"event_time_us"`
	EventTimeNSRemainder uint16          `json:"event_time_ns_remainder"`
	TimestampSource      string          `json:"timestamp_source"`
	TimestampOriginal    string          `json:"timestamp_original,omitempty"`
	ArrivalTimeUS        int64           `json:"arrival_time_us"`
	SourceEventID        *string         `json:"source_event_id,omitempty"`
	TraceID              *string         `json:"trace_id,omitempty"`
	SpanID               *string         `json:"span_id,omitempty"`
	Level                string          `json:"level"`
	OriginalLevel        string          `json:"original_level,omitempty"`
	SeverityNumber       *int16          `json:"severity_number,omitempty"`
	Message              string          `json:"message"`
	MessageTemplate      *string         `json:"message_template,omitempty"`
	Service              *string         `json:"service,omitempty"`
	Environment          *string         `json:"environment,omitempty"`
	Release              *string         `json:"release,omitempty"`
	Logger               *string         `json:"logger,omitempty"`
	SDKName              *string         `json:"sdk_name,omitempty"`
	SDKVersion           *string         `json:"sdk_version,omitempty"`
	EnvelopeSDKJSON      json.RawMessage `json:"envelope_sdk_json,omitempty"`
	SentAtJSON           json.RawMessage `json:"sent_at_json,omitempty"`
	Platform             *string         `json:"platform,omitempty"`
	ServerName           *string         `json:"server_name,omitempty"`
	Attrs                []Attribute     `json:"attrs"`
	SearchValues         []string        `json:"search_values"`
	Raw                  json.RawMessage `json:"raw"`
	Warnings             []string        `json:"warnings"`
	SchemaVersion        int             `json:"schema_version"`
	NormalizerVersion    int             `json:"normalizer_version"`
	ScrubVersion         int             `json:"scrub_version"`
}

type Outcome struct {
	ItemOrdinal int    `json:"item_ordinal"`
	Category    string `json:"category"`
	Reason      string `json:"reason"`
	Quantity    int64  `json:"quantity"`
	Approximate bool   `json:"approximate"`
}

type UnsupportedItem struct {
	ItemOrdinal int    `json:"item_ordinal"`
	Type        string `json:"type"`
	Bytes       int    `json:"bytes"`
}

// NormalizedRequest is one validated, scrubbed HTTP request. It contains no
// credentials or transport headers and is not a storage microbatch.
type NormalizedRequest struct {
	TenantID         int64             `json:"tenant_id"`
	ProjectID        int64             `json:"project_id"`
	AcceptanceID     string            `json:"acceptance_id"`
	Records          []Record          `json:"records"`
	Outcomes         []Outcome         `json:"outcomes"`
	Unsupported      int               `json:"unsupported"`
	UnsupportedItems []UnsupportedItem `json:"unsupported_items"`
}
