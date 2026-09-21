package query

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
)

const (
	QueryProtocolVersion  = 1
	MaxPlanFiles          = 32768
	MaxScanPartitions     = 4096
	MaxPlanBytes          = 16 << 20
	MaxTaskManifestBytes  = 1 << 20
	MaxFilesPerScan       = engine.MaxQueryInputFiles
	MaxSyncFiles          = 32
	MaxDetailFilesPerScan = 8
	TargetScanBytes       = int64(64 << 20)
	MaxTaskOutputBytes    = int64(64 << 20)
	ReduceFanIn           = 8
)

var ErrQueryLimit = errors.New("query limit exceeded")

type PlanScope struct {
	QueryID       string
	TenantID      int64
	SnapshotID    string
	Generation    int64
	OperationHash string
	Operation     []byte
	DeadlineUS    int64
}

type TaskManifest struct {
	Version       int                    `json:"version"`
	QueryID       string                 `json:"query_id"`
	TenantID      int64                  `json:"tenant_id"`
	SnapshotID    string                 `json:"snapshot_id"`
	Generation    int64                  `json:"generation"`
	OperationHash string                 `json:"operation_hash"`
	Operation     []byte                 `json:"operation"`
	DeadlineUS    int64                  `json:"deadline_us"`
	Task          model.QueryTaskKey     `json:"task"`
	Files         []model.CatalogFile    `json:"files,omitempty"`
	Inputs        []model.QueryTaskInput `json:"inputs,omitempty"`
}

type ExecutionPlan struct {
	Tasks         []model.QueryPlannedTask
	ScanCount     int
	ReducerCount  int
	ManifestBytes int
	SHA256        string
}

func BuildExecutionPlan(scope PlanScope, files []model.CatalogFile) (ExecutionPlan, error) {
	if err := validatePlanScope(scope); err != nil {
		return ExecutionPlan{}, err
	}
	partitions, err := partitionCatalog(files, scanFileLimit(scope.Operation))
	if err != nil {
		return ExecutionPlan{}, err
	}
	plan := ExecutionPlan{ScanCount: len(partitions)}
	if len(partitions) == 0 {
		key := model.QueryTaskKey{Stage: model.QueryTaskReduce, Level: 1, PartitionID: 0}
		task, err := makePlannedTask(scope, key, nil, nil)
		if err != nil {
			return ExecutionPlan{}, err
		}
		plan.Tasks = append(plan.Tasks, task)
		plan.ReducerCount = 1
	}
	for partitionID, partition := range partitions {
		key := model.QueryTaskKey{Stage: model.QueryTaskScan, Level: 0, PartitionID: partitionID}
		task, err := makePlannedTask(scope, key, partition, nil)
		if err != nil {
			return ExecutionPlan{}, err
		}
		plan.Tasks = append(plan.Tasks, task)
	}
	previous := make([]model.QueryTaskKey, len(partitions))
	for index := range partitions {
		previous[index] = model.QueryTaskKey{Stage: model.QueryTaskScan, Level: 0, PartitionID: index}
	}
	for level := 1; len(previous) > 1; level++ {
		next := make([]model.QueryTaskKey, 0, (len(previous)+ReduceFanIn-1)/ReduceFanIn)
		for start := 0; start < len(previous); start += ReduceFanIn {
			end := min(start+ReduceFanIn, len(previous))
			key := model.QueryTaskKey{Stage: model.QueryTaskReduce, Level: level, PartitionID: len(next)}
			inputs := make([]model.QueryTaskInput, end-start)
			for ordinal, producer := range previous[start:end] {
				inputs[ordinal] = model.QueryTaskInput{Consumer: key, Ordinal: ordinal, Producer: producer}
			}
			task, err := makePlannedTask(scope, key, nil, inputs)
			if err != nil {
				return ExecutionPlan{}, err
			}
			plan.Tasks = append(plan.Tasks, task)
			plan.ReducerCount++
			next = append(next, key)
		}
		previous = next
	}
	for _, task := range plan.Tasks {
		if plan.ManifestBytes > MaxPlanBytes-len(task.Manifest) {
			return ExecutionPlan{}, ErrQueryLimit
		}
		plan.ManifestBytes += len(task.Manifest)
	}
	encoded, err := canonicalPlanBytes(plan.Tasks)
	if err != nil || len(encoded) > MaxPlanBytes {
		return ExecutionPlan{}, errors.Join(ErrQueryLimit, err)
	}
	digest := sha256.Sum256(encoded)
	plan.SHA256 = hex.EncodeToString(digest[:])
	return plan, nil
}

func partitionCatalog(files []model.CatalogFile, maxFiles int) ([][]model.CatalogFile, error) {
	if len(files) > MaxPlanFiles {
		return nil, ErrQueryLimit
	}
	if maxFiles < 1 || maxFiles > MaxFilesPerScan {
		return nil, errors.New("invalid scan file limit")
	}
	partitions := make([][]model.CatalogFile, 0)
	var current []model.CatalogFile
	var currentBytes int64
	for index, file := range files {
		if file.FileID == "" || file.Bytes <= 0 || index > 0 && files[index-1].FileID >= file.FileID {
			return nil, errors.New("catalog files must be valid, sorted and unique")
		}
		if len(current) > 0 && (len(current) == maxFiles || file.Bytes > math.MaxInt64-currentBytes || currentBytes+file.Bytes > TargetScanBytes) {
			partitions = append(partitions, current)
			current, currentBytes = nil, 0
		}
		current = append(current, file)
		currentBytes += file.Bytes
	}
	if len(current) > 0 {
		partitions = append(partitions, current)
	}
	if len(partitions) > MaxScanPartitions {
		return nil, ErrQueryLimit
	}
	return partitions, nil
}

func scanFileLimit(operation []byte) int {
	var value struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(operation, &value) == nil && value.Kind == "detail" {
		return MaxDetailFilesPerScan
	}
	return MaxFilesPerScan
}

func makePlannedTask(scope PlanScope, key model.QueryTaskKey, files []model.CatalogFile, inputs []model.QueryTaskInput) (model.QueryPlannedTask, error) {
	manifest := TaskManifest{
		Version: QueryProtocolVersion, QueryID: scope.QueryID, TenantID: scope.TenantID, SnapshotID: scope.SnapshotID,
		Generation: scope.Generation, OperationHash: scope.OperationHash, Operation: append([]byte(nil), scope.Operation...),
		DeadlineUS: scope.DeadlineUS, Task: key, Files: append([]model.CatalogFile(nil), files...), Inputs: append([]model.QueryTaskInput(nil), inputs...),
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return model.QueryPlannedTask{}, err
	}
	if len(encoded) < 2 || len(encoded) > MaxTaskManifestBytes {
		return model.QueryPlannedTask{}, ErrQueryLimit
	}
	return model.QueryPlannedTask{Key: key, Manifest: encoded, InputOrdinal: append([]model.QueryTaskInput(nil), inputs...)}, nil
}

func canonicalPlanBytes(tasks []model.QueryPlannedTask) ([]byte, error) {
	canonical := make([]struct {
		Key      model.QueryTaskKey `json:"key"`
		Manifest json.RawMessage    `json:"manifest"`
	}, len(tasks))
	for index, task := range tasks {
		canonical[index].Key, canonical[index].Manifest = task.Key, task.Manifest
	}
	return json.Marshal(canonical)
}

func validatePlanScope(scope PlanScope) error {
	if uuid.Validate(scope.QueryID) != nil || uuid.Validate(scope.SnapshotID) != nil || scope.TenantID <= 0 || scope.Generation <= 0 || !validDigest(scope.OperationHash) || len(scope.Operation) < 2 || len(scope.Operation) > 65536 || !json.Valid(scope.Operation) || scope.DeadlineUS <= 0 {
		return errors.New("invalid query plan scope")
	}
	var canonical any
	if err := json.Unmarshal(scope.Operation, &canonical); err != nil {
		return err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil || string(encoded) != string(scope.Operation) {
		return errors.New("query operation must use canonical JSON")
	}
	return nil
}

func SortCatalogFiles(files []model.CatalogFile) {
	sort.Slice(files, func(i, j int) bool { return files[i].FileID < files[j].FileID })
}
