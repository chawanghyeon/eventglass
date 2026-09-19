package query

import "github.com/chawanghyeon/eventglass/internal/model"

type RequestMode string

const (
	ModeAuto  RequestMode = "auto"
	ModeSync  RequestMode = "sync"
	ModeAsync RequestMode = "async"
)

type PublicDataset struct {
	Spec         model.DatasetSpec
	Filter       *Node
	SHA256       string
	EncodedBytes []byte
}

type PublicSearchRequest struct {
	Dataset   PublicDataset
	ReadToken string
	Cursor    string
	Limit     int
	Sort      string
	Mode      RequestMode
}

type AggregateOrder struct {
	Metric    string
	Direction string
}

type PublicAggregateRequest struct {
	Dataset   PublicDataset
	ReadToken string
	Mode      RequestMode
	GroupBy   []GroupDimension
	Metrics   []AggregateMetric
	Histogram *AggregateHistogram
	Top       int
	Order     AggregateOrder
}
