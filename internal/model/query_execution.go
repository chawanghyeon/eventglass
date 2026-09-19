package model

type QueryTaskStage string

const (
	QueryTaskScan   QueryTaskStage = "scan"
	QueryTaskReduce QueryTaskStage = "reduce"
)

type QueryTaskKey struct {
	Stage       QueryTaskStage `json:"stage"`
	Level       int            `json:"level"`
	PartitionID int            `json:"partition_id"`
}

type QueryTaskInput struct {
	Consumer QueryTaskKey `json:"consumer"`
	Ordinal  int          `json:"ordinal"`
	Producer QueryTaskKey `json:"producer"`
}

type QueryPlannedTask struct {
	Key          QueryTaskKey
	Manifest     []byte
	InputOrdinal []QueryTaskInput
}
