package model

type QueryTimeBasis string

const (
	QueryTimeEvent    QueryTimeBasis = "event"
	QueryTimeReceived QueryTimeBasis = "received"
)

type DatasetSpec struct {
	TenantID   int64
	ProjectIDs []int64
	Kinds      []Kind
	TimeBasis  QueryTimeBasis
	StartUS    int64
	EndUS      int64
	Filter     []byte
}

type QueryPlan struct {
	Version       int
	DatasetSHA256 string
	Dataset       DatasetSpec
	Snapshot      SnapshotScope
}

type SnapshotScope struct {
	RetentionFloorUS int64
	LaneCuts         [LaneCount]int64
}
