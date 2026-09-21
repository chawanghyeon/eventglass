package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/google/uuid"
)

type publicationControl interface {
	LoadConversion(context.Context, control.JobAuthority) (control.ConversionWork, error)
	RegisterOutputIntent(context.Context, control.OutputIntentRegistration) error
	MarkOutputIntentUploaded(context.Context, control.JobAuthority, int64, control.IntentAuthority) error
	Prepare(context.Context, control.PrepareCommand) error
}

type publicationStore interface {
	DownloadToFile(context.Context, string, string, int64, string, int64) error
	PutStream(context.Context, string, io.ReadSeeker, int64, string) (storage.ObjectInfo, error)
	VerifyObject(context.Context, string, int64, string) error
}

type DurableConversionWorkflow struct {
	Control        publicationControl
	Store          publicationStore
	Runner         ConversionRunner
	InstallationID string
	ScratchDir     string
}

type publicationCommitControl interface {
	LoadPublicationObjects(context.Context, control.JobAuthority, string) ([]control.PublicationObject, error)
	Publish(context.Context, control.PublishCommand) (control.PublishResult, error)
}

type DurablePublicationWorkflow struct {
	Control publicationCommitControl
	Store   publicationStore
}

func (workflow DurablePublicationWorkflow) VerifyAndPublish(ctx context.Context, job control.PublicationJob) (control.PublishResult, error) {
	if workflow.Control == nil || workflow.Store == nil || job.OutputID == "" || job.ManifestSHA == "" {
		return control.PublishResult{}, errors.New("invalid durable publication workflow")
	}
	objects, err := workflow.Control.LoadPublicationObjects(ctx, job.Authority, job.OutputID)
	if err != nil {
		return control.PublishResult{}, err
	}
	for _, object := range objects {
		if err := workflow.Store.VerifyObject(ctx, object.ObjectKey, object.Bytes, object.SHA256); err != nil {
			return control.PublishResult{}, err
		}
	}
	return workflow.Control.Publish(ctx, control.PublishCommand{
		Authority: job.Authority, TenantID: job.TenantID, LaneID: job.LaneID, BatchSeq: job.BatchSeq,
		OutputID: job.OutputID, ManifestSHA: job.ManifestSHA,
	})
}

func (workflow DurableConversionWorkflow) ConvertAndPrepare(ctx context.Context, job control.ConversionJob) error {
	if workflow.Control == nil || workflow.Store == nil || workflow.Runner == nil || workflow.InstallationID == "" || workflow.ScratchDir == "" || job.Authority.InstallationID != workflow.InstallationID {
		return errors.New("invalid durable conversion workflow")
	}
	if err := storage.EnsurePrivateDirectory(workflow.ScratchDir); err != nil {
		return err
	}
	taskDirectory, err := os.MkdirTemp(workflow.ScratchDir, "conversion-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(taskDirectory)
	work, err := workflow.Control.LoadConversion(ctx, job.Authority)
	if err != nil {
		return err
	}
	journalPath := filepath.Join(taskDirectory, "journal.jsonl.zst")
	if err := workflow.Store.DownloadToFile(ctx, work.JournalObjectKey, journalPath, work.JournalBytes, work.JournalSHA256, storage.MaxJournalBytes); err != nil {
		return err
	}
	receipts := make([]ConversionReceipt, len(work.Receipts))
	for index, item := range work.Receipts {
		receipts[index] = ConversionReceipt{Receipt: item.Receipt, GroupingVersion: item.GroupingVersion}
	}
	input := ConversionInput{
		JournalPath: journalPath, Journal: storage.JournalInfo{Bytes: work.JournalBytes, SHA256: work.JournalSHA256},
		TenantID: work.TenantID, LaneID: work.LaneID, BatchSeq: work.BatchSeq, BatchID: work.BatchID, Receipts: receipts,
		TaskDir: filepath.Join(taskDirectory, "stage"),
	}
	if err := os.Mkdir(input.TaskDir, 0o700); err != nil {
		return err
	}
	manifestDirectory := filepath.Join(taskDirectory, "manifest")
	pager, err := storage.NewOutputManifestPager(manifestDirectory)
	if err != nil {
		return err
	}
	defer pager.Cleanup()
	outputID, err := randomUUID()
	if err != nil {
		return err
	}
	worker := ConversionWorker{Runner: workflow.Runner}
	summary, artifacts, err := worker.Execute(ctx, input, func(converted engine.ConvertedBundle) error {
		bundle, err := workflow.uploadBundle(ctx, job, work, converted)
		if err != nil {
			return err
		}
		return pager.Append(bundle)
	})
	if err != nil {
		return err
	}
	defer artifacts.Cleanup()
	header := model.OutputManifestHeader{
		Version: model.OutputManifestVersion, OutputID: outputID, JobID: job.Authority.JobID,
		TenantID: work.TenantID, LaneID: work.LaneID, BatchSeq: work.BatchSeq,
		JournalSHA256: artifacts.JournalSHA256, ReceiptSetSHA256: artifacts.ReceiptSetSHA256,
		SelectedIdentitySHA256: summary.SelectedIdentitySHA256, OccurrenceSummarySHA256: artifacts.OccurrenceSHA256,
		GroupingVersion: commonGroupingVersion(work.Receipts), SelectedRecordCount: artifacts.SelectedRecordCount,
		SelectedErrorCount: artifacts.SelectedErrorCount, BundleCount: summary.BundleCount,
	}
	if header.GroupingVersion <= 0 {
		return errors.New("conversion receipts have inconsistent grouping versions")
	}
	manifest, err := pager.Finish(header)
	if err != nil {
		return err
	}
	parts := make([]control.PreparedPartInput, len(manifest.Parts))
	for index, part := range manifest.Parts {
		parts[index] = control.PreparedPartInput{Index: part.Index, Path: part.Path, Bytes: part.Bytes, SHA256: part.SHA256}
	}
	return workflow.Control.Prepare(ctx, control.PrepareCommand{
		Authority: job.Authority, TenantID: work.TenantID, LaneID: work.LaneID, BatchSeq: work.BatchSeq,
		Root: manifest.Root, Parts: parts, OccurrencePath: artifacts.OccurrencePath,
		OccurrenceBytes: artifacts.OccurrenceBytes, OccurrenceSHA: artifacts.OccurrenceSHA256,
	})
}

func (workflow DurableConversionWorkflow) uploadBundle(ctx context.Context, job control.ConversionJob, work control.ConversionWork, converted engine.ConvertedBundle) (model.BundleManifest, error) {
	bundleID, err := randomUUID()
	if err != nil {
		return model.BundleManifest{}, err
	}
	upload := func(role string, convertedFile engine.ConvertedFile) (model.FileManifest, error) {
		fileID, err := randomUUID()
		if err != nil {
			return model.FileManifest{}, err
		}
		intentID, err := randomUUID()
		if err != nil {
			return model.FileManifest{}, err
		}
		key := fmt.Sprintf("v1/%s/bundles/%d/%d/%s/%d/%s/%s.parquet", workflow.InstallationID, work.TenantID, work.LaneID, job.Authority.JobID, job.Authority.Fence, bundleID, role)
		intent := control.IntentAuthority{IntentID: intentID, Owner: job.Authority.Owner, Fence: job.Authority.Fence, Bytes: convertedFile.Evidence.Bytes, SHA256: convertedFile.Evidence.SHA256}
		if err := workflow.Control.RegisterOutputIntent(ctx, control.OutputIntentRegistration{Authority: job.Authority, TenantID: work.TenantID, Role: role, ObjectKey: key, Intent: intent}); err != nil {
			return model.FileManifest{}, err
		}
		file, err := os.Open(convertedFile.Path)
		if err != nil {
			return model.FileManifest{}, err
		}
		_, uploadErr := workflow.Store.PutStream(ctx, key, file, intent.Bytes, intent.SHA256)
		closeErr := file.Close()
		if uploadErr != nil || closeErr != nil {
			return model.FileManifest{}, errors.Join(uploadErr, closeErr)
		}
		if err := workflow.Control.MarkOutputIntentUploaded(ctx, job.Authority, work.TenantID, intent); err != nil {
			return model.FileManifest{}, err
		}
		blocks := make([]model.FileBlockManifest, len(convertedFile.Evidence.BlockSHA256))
		for index, sha := range convertedFile.Evidence.BlockSHA256 {
			blocks[index] = model.FileBlockManifest{Index: index, SHA256: sha}
		}
		return model.FileManifest{
			FileID: fileID, IntentID: intentID, Role: role, Bytes: intent.Bytes, SHA256: intent.SHA256, RowCount: convertedFile.RowCount,
			MinEventTimeUS: convertedFile.MinEventTimeUS, MaxEventTimeUS: convertedFile.MaxEventTimeUS,
			MinReceivedTimeUS: convertedFile.MinReceivedTimeUS, MaxReceivedTimeUS: convertedFile.MaxReceivedTimeUS,
			MinBatchSeq: convertedFile.MinBatchSeq, MaxBatchSeq: convertedFile.MaxBatchSeq, Blocks: blocks,
		}, nil
	}
	analytics, err := upload("analytics", converted.Analytics)
	if err != nil {
		return model.BundleManifest{}, err
	}
	payload, err := upload("payload", converted.Payload)
	if err != nil {
		return model.BundleManifest{}, err
	}
	return model.BundleManifest{
		BundleID: bundleID, EventDay: converted.EventDay, Kind: converted.Kind, InputSeqMin: work.BatchSeq, InputSeqMax: work.BatchSeq,
		RowCount: converted.RowCount, IdentitySHA256: converted.IdentitySHA256, ProjectIDs: converted.ProjectIDs,
		Analytics: analytics, Payload: payload,
	}, nil
}

func commonGroupingVersion(receipts []control.DurableConversionReceipt) int {
	version := 0
	for _, receipt := range receipts {
		if version == 0 {
			version = receipt.GroupingVersion
		}
		if receipt.GroupingVersion != version {
			return 0
		}
	}
	return version
}

func randomUUID() (string, error) { value, err := uuid.NewRandom(); return value.String(), err }
