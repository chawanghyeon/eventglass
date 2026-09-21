package maintenance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/google/uuid"
)

type outputControl interface {
	RegisterMaintenanceIntent(context.Context, control.MaintenanceIntentRegistration) error
	MarkMaintenanceIntentUploaded(context.Context, control.MaintenanceAuthority, control.IntentAuthority) error
}

func uploadBundle(ctx context.Context, controlPlane outputControl, store CompactionStore, installationID string, authority control.MaintenanceAuthority, partition control.CompactionPartition, converted engine.ConvertedBundle) (model.BundleManifest, error) {
	bundleID := uuid.NewString()
	upload := func(role string, file engine.ConvertedFile) (model.FileManifest, error) {
		fileID, intentID := uuid.NewString(), uuid.NewString()
		key := fmt.Sprintf("v1/%s/maintenance/%d/%d/%s/%d/%s/%s.parquet", installationID, authority.TenantID, authority.LaneID, authority.TaskID, authority.Fence, bundleID, role)
		intent := control.IntentAuthority{IntentID: intentID, Owner: authority.Owner, Fence: authority.Fence, Bytes: file.Evidence.Bytes, SHA256: file.Evidence.SHA256}
		if err := controlPlane.RegisterMaintenanceIntent(ctx, control.MaintenanceIntentRegistration{Authority: authority, Role: role, ObjectKey: key, Intent: intent}); err != nil {
			return model.FileManifest{}, err
		}
		reader, err := os.Open(file.Path)
		if err != nil {
			return model.FileManifest{}, err
		}
		_, uploadErr := store.PutStream(ctx, key, reader, intent.Bytes, intent.SHA256)
		closeErr := reader.Close()
		if uploadErr != nil || closeErr != nil {
			return model.FileManifest{}, errors.Join(uploadErr, closeErr)
		}
		if err := controlPlane.MarkMaintenanceIntentUploaded(ctx, authority, intent); err != nil {
			return model.FileManifest{}, err
		}
		blocks := make([]model.FileBlockManifest, len(file.Evidence.BlockSHA256))
		for index, checksum := range file.Evidence.BlockSHA256 {
			blocks[index] = model.FileBlockManifest{Index: index, SHA256: checksum}
		}
		return model.FileManifest{FileID: fileID, IntentID: intentID, Role: role, Bytes: file.Evidence.Bytes, SHA256: file.Evidence.SHA256, RowCount: file.RowCount, MinEventTimeUS: file.MinEventTimeUS, MaxEventTimeUS: file.MaxEventTimeUS, MinReceivedTimeUS: file.MinReceivedTimeUS, MaxReceivedTimeUS: file.MaxReceivedTimeUS, MinBatchSeq: file.MinBatchSeq, MaxBatchSeq: file.MaxBatchSeq, Blocks: blocks}, nil
	}
	analytics, err := upload("analytics", converted.Analytics)
	if err != nil {
		return model.BundleManifest{}, err
	}
	payload, err := upload("payload", converted.Payload)
	if err != nil {
		return model.BundleManifest{}, err
	}
	bundle := model.BundleManifest{BundleID: bundleID, EventDay: partition.EventDay, Kind: partition.Kind, InputSeqMin: analytics.MinBatchSeq, InputSeqMax: analytics.MaxBatchSeq, RowCount: converted.RowCount, IdentitySHA256: converted.IdentitySHA256, ProjectIDs: converted.ProjectIDs, Analytics: analytics, Payload: payload}
	return bundle, nil
}

func reserveDisk(budget *resource.Budget, work control.CompactionWork) (*resource.Permit, error) {
	if budget == nil {
		return nil, errors.New("maintenance disk budget is required")
	}
	bytes := engine.DefaultNativeSpillBytes + 2*engine.MaxBundleFileBytes
	for _, input := range work.Inputs {
		for _, size := range []int64{input.Analytics.Bytes, input.Payload.Bytes} {
			if size <= 0 || size > engine.MaxBundleFileBytes || bytes > 4<<30 {
				return nil, resource.ErrLimited
			}
			bytes += size
		}
	}
	return budget.Acquire(bytes)
}

func downloadInputs(ctx context.Context, store CompactionStore, directory string, work control.CompactionWork) ([]engine.CompactionInput, error) {
	inputs := make([]engine.CompactionInput, len(work.Inputs))
	for index, input := range work.Inputs {
		inputDir := filepath.Join(directory, fmt.Sprintf("input-%03d", index))
		if err := os.Mkdir(inputDir, 0o700); err != nil {
			return nil, err
		}
		analytics, payload := filepath.Join(inputDir, "analytics.parquet"), filepath.Join(inputDir, "payload.parquet")
		if err := store.DownloadToFile(ctx, input.Analytics.ObjectKey, analytics, input.Analytics.Bytes, input.Analytics.SHA256, engine.MaxBundleFileBytes); err != nil {
			return nil, err
		}
		if err := store.DownloadToFile(ctx, input.Payload.ObjectKey, payload, input.Payload.Bytes, input.Payload.SHA256, engine.MaxBundleFileBytes); err != nil {
			return nil, err
		}
		inputs[index] = engine.CompactionInput{BundleID: input.BundleID, AnalyticsPath: analytics, PayloadPath: payload, IdentitySHA256: input.IdentitySHA256}
	}
	return inputs, nil
}
