package storage

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/klauspost/compress/zstd"
)

const (
	JournalFormatVersion = 1
	MaxJournalBytes      = 24 << 20
	maxJournalLine       = (1 << 20) + (16 << 10)
)

type JournalHeader struct {
	FormatVersion     int    `json:"format_version"`
	SchemaVersion     int    `json:"schema_version"`
	NormalizerVersion int    `json:"normalizer_version"`
	ScrubVersion      int    `json:"scrub_version"`
	TenantID          int64  `json:"tenant_id"`
	LaneID            int    `json:"lane_id"`
	BatchID           string `json:"batch_id"`
	RequestCount      int    `json:"request_count"`
}

// Ordinals are global record positions within this object, not SDK item
// ordinals. Empty requests have Last = First - 1.
type JournalRequest struct {
	AcceptanceID string `json:"acceptance_id"`
	ProjectID    int64  `json:"project_id"`
	First        int    `json:"first"`
	Last         int    `json:"last"`
	Records      int    `json:"records"`
	Outcomes     int    `json:"outcomes"`
	Unsupported  int    `json:"unsupported"`
}

type JournalInfo struct {
	Bytes  int64
	SHA256 string
	Index  JournalIndex
}

type JournalIndex struct {
	Header   JournalHeader
	Requests []model.JournalRequestIndex
}

type journalLine struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type journalCount struct {
	Records     int    `json:"records"`
	Outcomes    int    `json:"outcomes"`
	Unsupported int    `json:"unsupported"`
	SHA256      string `json:"sha256"`
}

type digestWriter struct {
	io.Writer
	hash hash.Hash
	size int64
}

func (w *digestWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.hash.Write(p[:n])
	w.size += int64(n)
	return n, err
}

// WriteJournal uses one encoded line plus the bounded zstd window as scratch.
// The caller owns the immutable input and a quota-reserved sanitized spool.
// A failed write leaves a partial spool that must never be uploaded/accepted.
func WriteJournal(output io.Writer, batch model.JournalBatch) (JournalInfo, error) {
	header := JournalHeader{JournalFormatVersion, model.SchemaVersion, model.NormalizerVersion, model.ScrubVersion, batch.TenantID, batch.LaneID, batch.BatchID, len(batch.Requests)}
	if err := validateJournalHeader(header); err != nil {
		return JournalInfo{}, err
	}
	digest := &digestWriter{Writer: output, hash: sha256.New()}
	encoder, err := zstd.NewWriter(digest, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)), zstd.WithWindowSize(1<<20), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		return JournalInfo{}, err
	}
	defer encoder.Close()
	plainBytes := 0
	write := func(kind string, value any, requestHash hash.Hash) error {
		payload, err := json.Marshal(value)
		if err != nil {
			return err
		}
		line, err := json.Marshal(journalLine{Type: kind, Value: payload})
		if err != nil {
			return err
		}
		line = append(line, '\n')
		plainBytes += len(line)
		if len(line) > maxJournalLine || plainBytes > MaxJournalBytes {
			return errors.New("journal size limit exceeded")
		}
		if requestHash != nil {
			requestHash.Write(line)
		}
		_, err = encoder.Write(line)
		return err
	}
	if err := write("header", header, nil); err != nil {
		return JournalInfo{}, err
	}
	seen := make(map[string]bool)
	index := JournalIndex{Header: header, Requests: make([]model.JournalRequestIndex, 0, len(batch.Requests))}
	position := 0
	for _, request := range batch.Requests {
		meta := JournalRequest{request.AcceptanceID, request.ProjectID, position, position + len(request.Records) - 1, len(request.Records), len(request.Outcomes), len(request.UnsupportedItems)}
		if request.TenantID != batch.TenantID || request.Unsupported != meta.Unsupported {
			return JournalInfo{}, errors.New("request scope or counts mismatch")
		}
		if err := validateJournalRequest(header, meta, position, seen); err != nil {
			return JournalInfo{}, err
		}
		h := requestDigest(meta)
		if err := write("request", meta, nil); err != nil {
			return JournalInfo{}, err
		}
		ordinals := make(map[[2]int]bool)
		for _, record := range request.Records {
			if err := validateJournalRecord(header, meta, record, ordinals); err != nil {
				return JournalInfo{}, err
			}
			if err := write("record", record, h); err != nil {
				return JournalInfo{}, err
			}
		}
		for _, outcome := range request.Outcomes {
			if outcome.ItemOrdinal < 0 || outcome.Quantity < 0 {
				return JournalInfo{}, errors.New("invalid outcome")
			}
			if err := write("outcome", outcome, h); err != nil {
				return JournalInfo{}, err
			}
		}
		for _, item := range request.UnsupportedItems {
			if item.ItemOrdinal < 0 || item.Bytes < 0 {
				return JournalInfo{}, errors.New("invalid unsupported item")
			}
			if err := write("unsupported", item, h); err != nil {
				return JournalInfo{}, err
			}
		}
		footer := journalCount{meta.Records, meta.Outcomes, meta.Unsupported, hex.EncodeToString(h.Sum(nil))}
		if err := write("end_request", footer, nil); err != nil {
			return JournalInfo{}, err
		}
		index.Requests = append(index.Requests, model.JournalRequestIndex{
			AcceptanceID: meta.AcceptanceID, ProjectID: meta.ProjectID, OrdinalFirst: meta.First, OrdinalLast: meta.Last,
			RecordCount: meta.Records, ContentSHA256: footer.SHA256,
			Outcomes:         append(make([]model.Outcome, 0, len(request.Outcomes)), request.Outcomes...),
			UnsupportedItems: append(make([]model.UnsupportedItem, 0, len(request.UnsupportedItems)), request.UnsupportedItems...),
		})
		position += len(request.Records)
	}
	if err := write("end", len(batch.Requests), nil); err != nil {
		return JournalInfo{}, err
	}
	if err := encoder.Close(); err != nil {
		return JournalInfo{}, err
	}
	if digest.size > MaxJournalBytes {
		return JournalInfo{}, errors.New("compressed journal exceeds size limit")
	}
	return JournalInfo{Bytes: digest.size, SHA256: hex.EncodeToString(digest.hash.Sum(nil)), Index: index}, nil
}

// ReplayJournal verifies the object checksum before invoking callbacks, then
// streams bounded lines. input must be a private immutable, quota-owned spool.
// Callbacks stage work only: structural validation can still fail later and
// only a nil return authorizes the caller to prepare outputs. No renormalizing.
// Outcome/unsupported entries remain in the journal and are counted/validated;
// their durable SQL representation is owned by Accept.
func ReplayJournal(input io.ReadSeeker, expected JournalInfo, consume func(JournalRequest, int, model.Record) error) (JournalIndex, error) {
	fail := func(err error) (JournalIndex, error) { return JournalIndex{}, err }
	if expected.Bytes <= 0 || expected.Bytes > MaxJournalBytes || len(expected.SHA256) != 64 {
		return fail(errors.New("invalid journal manifest"))
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(input, expected.Bytes+1))
	if err != nil {
		return fail(err)
	}
	if n != expected.Bytes || hex.EncodeToString(h.Sum(nil)) != expected.SHA256 {
		return fail(errors.New("journal object checksum mismatch"))
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	decoder, err := zstd.NewReader(input, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(8<<20))
	if err != nil {
		return fail(err)
	}
	defer decoder.Close()
	scanner := bufio.NewScanner(io.LimitReader(decoder, MaxJournalBytes+1))
	scanner.Buffer(make([]byte, 64<<10), maxJournalLine)
	var header JournalHeader
	var meta JournalRequest
	var counts journalCount
	var requestHash hash.Hash
	index := JournalIndex{Requests: make([]model.JournalRequestIndex, 0)}
	var outcomes []model.Outcome
	var unsupportedItems []model.UnsupportedItem
	seen := make(map[string]bool)
	var ordinals map[[2]int]bool
	position, requests, plainBytes := 0, 0, 0
	headerSeen, ended := false, false
	for scanner.Scan() {
		raw := scanner.Bytes()
		plainBytes += len(raw) + 1
		if plainBytes > MaxJournalBytes || ended {
			return fail(errors.New("journal size limit or trailing data"))
		}
		var line journalLine
		if err := strictJournalJSON(raw, &line); err != nil {
			return fail(err)
		}
		if !headerSeen && line.Type != "header" {
			return fail(errors.New("journal header missing"))
		}
		switch line.Type {
		case "header":
			if headerSeen {
				return fail(errors.New("duplicate journal header"))
			}
			if err := strictJournalJSON(line.Value, &header); err != nil {
				return fail(err)
			}
			if err := validateJournalHeader(header); err != nil {
				return fail(err)
			}
			headerSeen = true
			index.Header = header
			index.Requests = make([]model.JournalRequestIndex, 0, header.RequestCount)
		case "request":
			if requestHash != nil || requests >= header.RequestCount {
				return fail(errors.New("request framing mismatch"))
			}
			if err := strictJournalJSON(line.Value, &meta); err != nil {
				return fail(err)
			}
			if err := validateJournalRequest(header, meta, position, seen); err != nil {
				return fail(err)
			}
			requestHash = requestDigest(meta)
			counts = journalCount{}
			ordinals = make(map[[2]int]bool)
			outcomes = make([]model.Outcome, 0, meta.Outcomes)
			unsupportedItems = make([]model.UnsupportedItem, 0, meta.Unsupported)
		case "record":
			if requestHash == nil || counts.Records >= meta.Records || counts.Outcomes != 0 || counts.Unsupported != 0 {
				return fail(errors.New("record framing mismatch"))
			}
			var record model.Record
			if err := strictJournalJSON(line.Value, &record); err != nil {
				return fail(err)
			}
			if err := validateJournalRecord(header, meta, record, ordinals); err != nil {
				return fail(err)
			}
			if consume != nil {
				if err := consume(meta, position, record); err != nil {
					return fail(err)
				}
			}
			counts.Records++
			position++
		case "outcome":
			if requestHash == nil || counts.Records != meta.Records || counts.Outcomes >= meta.Outcomes || counts.Unsupported != 0 {
				return fail(errors.New("outcome framing mismatch"))
			}
			var outcome model.Outcome
			if err := strictJournalJSON(line.Value, &outcome); err != nil {
				return fail(err)
			}
			if outcome.ItemOrdinal < 0 || outcome.Quantity < 0 {
				return fail(errors.New("invalid outcome"))
			}
			outcomes = append(outcomes, outcome)
			counts.Outcomes++
		case "unsupported":
			if requestHash == nil || counts.Records != meta.Records || counts.Outcomes != meta.Outcomes || counts.Unsupported >= meta.Unsupported {
				return fail(errors.New("unsupported framing mismatch"))
			}
			var item model.UnsupportedItem
			if err := strictJournalJSON(line.Value, &item); err != nil {
				return fail(err)
			}
			if item.ItemOrdinal < 0 || item.Bytes < 0 {
				return fail(errors.New("invalid unsupported item"))
			}
			unsupportedItems = append(unsupportedItems, item)
			counts.Unsupported++
		case "end_request":
			if requestHash == nil {
				return fail(errors.New("unexpected request footer"))
			}
			counts.SHA256 = hex.EncodeToString(requestHash.Sum(nil))
			var footer journalCount
			if err := strictJournalJSON(line.Value, &footer); err != nil {
				return fail(err)
			}
			if footer != counts || counts.Records != meta.Records || counts.Outcomes != meta.Outcomes || counts.Unsupported != meta.Unsupported {
				return fail(errors.New("request checksum or counts mismatch"))
			}
			index.Requests = append(index.Requests, model.JournalRequestIndex{
				AcceptanceID: meta.AcceptanceID, ProjectID: meta.ProjectID, OrdinalFirst: meta.First, OrdinalLast: meta.Last,
				RecordCount: meta.Records, ContentSHA256: counts.SHA256,
				Outcomes: outcomes, UnsupportedItems: unsupportedItems,
			})
			requestHash = nil
			requests++
		case "end":
			var count int
			if err := strictJournalJSON(line.Value, &count); err != nil {
				return fail(err)
			}
			if requestHash != nil || requests != header.RequestCount || count != requests {
				return fail(errors.New("journal request count mismatch"))
			}
			ended = true
		default:
			return fail(errors.New("unknown journal entry"))
		}
		if requestHash != nil && line.Type != "request" {
			requestHash.Write(raw)
			requestHash.Write([]byte{'\n'})
		}
	}
	if err := scanner.Err(); err != nil {
		return fail(err)
	}
	if !ended {
		return fail(errors.New("incomplete journal"))
	}
	return index, nil
}

func strictJournalJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing journal JSON")
	}
	return nil
}

// Physical placement is excluded so rebuilding a mixed batch preserves each
// request's receipt checksum. Counts, identity and all content remain covered.
func requestDigest(meta JournalRequest) hash.Hash {
	meta.First, meta.Last = 0, meta.Records-1
	value, _ := json.Marshal(meta)
	line, _ := json.Marshal(journalLine{Type: "request", Value: value})
	h := sha256.New()
	h.Write(line)
	h.Write([]byte{'\n'})
	return h
}

func validateJournalHeader(h JournalHeader) error {
	if h.FormatVersion != JournalFormatVersion || h.SchemaVersion != model.SchemaVersion || h.NormalizerVersion != model.NormalizerVersion || h.ScrubVersion != model.ScrubVersion {
		return errors.New("unsupported journal version")
	}
	if _, err := model.LaneForAcceptance(h.BatchID); err != nil {
		return err
	}
	if h.TenantID <= 0 || h.LaneID < 0 || h.LaneID >= model.LaneCount || h.RequestCount <= 0 || h.RequestCount > 1000 {
		return errors.New("invalid journal scope or request count")
	}
	return nil
}

func validateJournalRequest(h JournalHeader, r JournalRequest, position int, seen map[string]bool) error {
	lane, err := model.LaneForAcceptance(r.AcceptanceID)
	if err != nil {
		return err
	}
	if lane != h.LaneID || seen[r.AcceptanceID] || r.ProjectID <= 0 || r.First != position || r.Records < 0 || r.Records > 10000-position || r.Last != r.First+r.Records-1 || r.Outcomes < 0 || r.Outcomes > 10000 || r.Unsupported < 0 || r.Unsupported > 1000 {
		return errors.New("invalid journal request scope, range or count")
	}
	seen[r.AcceptanceID] = true
	return nil
}

func validateJournalRecord(h JournalHeader, request JournalRequest, record model.Record, ordinals map[[2]int]bool) error {
	pair := [2]int{record.ItemOrdinal, record.RecordOrdinal}
	if record.TenantID != h.TenantID || record.ProjectID != request.ProjectID || record.AcceptanceID != request.AcceptanceID || record.ItemOrdinal < 0 || record.RecordOrdinal < 0 || ordinals[pair] {
		return errors.New("journal record scope or ordinal mismatch")
	}
	if record.SchemaVersion != h.SchemaVersion || record.NormalizerVersion != h.NormalizerVersion || record.ScrubVersion != h.ScrubVersion {
		return errors.New("journal record version mismatch")
	}
	if record.Kind != model.KindError && record.Kind != model.KindLog && record.Kind != model.KindTransaction {
		return errors.New("invalid journal record kind")
	}
	if len(record.RecordID) != 64 {
		return fmt.Errorf("invalid journal record identity")
	}
	if _, err := hex.DecodeString(record.RecordID); err != nil {
		return errors.New("invalid journal record identity")
	}
	ordinals[pair] = true
	return nil
}
