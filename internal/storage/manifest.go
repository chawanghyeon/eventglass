package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
)

type ManifestPartArtifact struct {
	Index  int
	Path   string
	Bytes  int64
	SHA256 string
}

type PreparedOutputManifest struct {
	Root       model.OutputManifestRoot
	RootJSON   []byte
	RootSHA256 string
	Parts      []ManifestPartArtifact
}

type OutputManifestPager struct {
	directory string
	parts     []ManifestPartArtifact
	current   []model.BundleManifest
	total     int64
	bundles   int
	rows      int64
	errorRows int64
	closed    bool
}

func NewOutputManifestPager(directory string) (*OutputManifestPager, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("manifest directory must be absolute")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("manifest directory must be private")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		return nil, errors.New("manifest directory must start empty")
	}
	return &OutputManifestPager{directory: directory, current: make([]model.BundleManifest, 0)}, nil
}

func (pager *OutputManifestPager) Append(bundle model.BundleManifest) error {
	if pager.closed {
		return errors.New("manifest pager is closed")
	}
	if err := validateBundleManifest(bundle); err != nil {
		return err
	}
	candidate := append(pager.current, bundle)
	encoded, err := encodeManifestPart(len(pager.parts), candidate)
	if err != nil {
		return err
	}
	if len(encoded) <= model.MaxManifestPartBytes {
		pager.current = candidate
		pager.bundles++
		pager.rows += bundle.RowCount
		if bundle.Kind == model.KindError {
			pager.errorRows += bundle.RowCount
		}
		return nil
	}
	if len(pager.current) == 0 {
		return errors.New("one bundle exceeds manifest part limit")
	}
	if err := pager.flush(); err != nil {
		return err
	}
	pager.current = append(pager.current, bundle)
	encoded, err = encodeManifestPart(len(pager.parts), pager.current)
	if err != nil || len(encoded) > model.MaxManifestPartBytes {
		return errors.Join(errors.New("one bundle exceeds manifest part limit"), err)
	}
	pager.bundles++
	pager.rows += bundle.RowCount
	if bundle.Kind == model.KindError {
		pager.errorRows += bundle.RowCount
	}
	return nil
}

func (pager *OutputManifestPager) Finish(header model.OutputManifestHeader) (PreparedOutputManifest, error) {
	if pager.closed {
		return PreparedOutputManifest{}, errors.New("manifest pager is closed")
	}
	pager.closed = true
	if len(pager.current) != 0 {
		if err := pager.flush(); err != nil {
			return PreparedOutputManifest{}, err
		}
	}
	if header.BundleCount != pager.bundles || int64(header.SelectedRecordCount) != pager.rows || int64(header.SelectedErrorCount) != pager.errorRows || header.BundleCount == 0 && len(pager.parts) != 0 || header.BundleCount > 0 && len(pager.parts) == 0 {
		return PreparedOutputManifest{}, errors.New("manifest bundle and part counts disagree")
	}
	refs := make([]model.OutputManifestPartRef, len(pager.parts))
	for index, part := range pager.parts {
		refs[index] = model.OutputManifestPartRef{Index: part.Index, SHA256: part.SHA256}
	}
	root := model.OutputManifestRoot{Version: model.OutputManifestVersion, Header: header, Parts: refs}
	rootJSON, err := root.CanonicalJSON()
	if err != nil {
		return PreparedOutputManifest{}, err
	}
	digest := sha256.Sum256(rootJSON)
	return PreparedOutputManifest{Root: root, RootJSON: rootJSON, RootSHA256: hex.EncodeToString(digest[:]), Parts: append([]ManifestPartArtifact(nil), pager.parts...)}, nil
}

func (pager *OutputManifestPager) Cleanup() error {
	var result error
	for _, part := range pager.parts {
		result = errors.Join(result, os.Remove(part.Path))
	}
	pager.parts = nil
	pager.current = nil
	return result
}

func (pager *OutputManifestPager) flush() error {
	index := len(pager.parts)
	encoded, err := encodeManifestPart(index, pager.current)
	if err != nil {
		return err
	}
	if len(encoded) > model.MaxManifestPartBytes || pager.total+int64(len(encoded)) > model.MaxManifestBytes {
		return errors.New("output manifest exceeds durable metadata limit")
	}
	path := filepath.Join(pager.directory, fmt.Sprintf("manifest-part-%06d.json", index))
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	pager.parts = append(pager.parts, ManifestPartArtifact{Index: index, Path: path, Bytes: int64(len(encoded)), SHA256: hex.EncodeToString(digest[:])})
	pager.total += int64(len(encoded))
	pager.current = make([]model.BundleManifest, 0)
	return nil
}

func encodeManifestPart(index int, bundles []model.BundleManifest) ([]byte, error) {
	if index < 0 || len(bundles) == 0 {
		return nil, errors.New("manifest part must contain ordered bundles")
	}
	return json.Marshal(model.OutputManifestPart{Version: model.OutputManifestVersion, Index: index, Bundles: bundles})
}

func validateBundleManifest(bundle model.BundleManifest) error {
	if bundle.BundleID == "" || !validEventDay(bundle.EventDay) || (bundle.Kind != model.KindError && bundle.Kind != model.KindLog && bundle.Kind != model.KindTransaction) || bundle.InputSeqMin <= 0 || bundle.InputSeqMax < bundle.InputSeqMin || bundle.RowCount <= 0 || !validLowerSHA(bundle.IdentitySHA256) || len(bundle.ProjectIDs) == 0 {
		return errors.New("invalid bundle manifest")
	}
	previousProjectID := int64(0)
	for _, projectID := range bundle.ProjectIDs {
		if projectID <= previousProjectID {
			return errors.New("bundle project IDs must be positive, sorted, and unique")
		}
		previousProjectID = projectID
	}
	if err := validateFileManifest(bundle.Analytics, "analytics", bundle.RowCount); err != nil {
		return err
	}
	return validateFileManifest(bundle.Payload, "payload", bundle.RowCount)
}

func validateFileManifest(file model.FileManifest, role string, rows int64) error {
	if file.FileID == "" || file.IntentID == "" || file.Role != role || file.Bytes <= 0 || !validLowerSHA(file.SHA256) || file.RowCount != rows || file.MaxEventTimeUS < file.MinEventTimeUS || file.MaxReceivedTimeUS < file.MinReceivedTimeUS || file.MinBatchSeq <= 0 || file.MaxBatchSeq < file.MinBatchSeq || len(file.Blocks) != int((file.Bytes+model.FileBlockBytes-1)/model.FileBlockBytes) {
		return errors.New("invalid file manifest")
	}
	for index, block := range file.Blocks {
		if block.Index != index || !validLowerSHA(block.SHA256) {
			return errors.New("file block manifests must be contiguous")
		}
	}
	return nil
}

func validLowerSHA(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validEventDay(value string) bool {
	parsed, err := time.Parse(time.DateOnly, value)
	return err == nil && parsed.Format(time.DateOnly) == value
}
