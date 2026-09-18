package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	baseUS       int64 = 1_788_825_600_000_000
	batchRecords       = 1_000
	chunkRecords       = 128
	chunkBytes         = 512 * 1024
	batchBytes         = 4 * 1024 * 1024
)

type kind string

const (
	logKind   kind = "log"
	errorKind kind = "error"
)

type record struct {
	RecordID           string         `json:"record_id"`
	Kind               kind           `json:"kind"`
	ProjectID          int64          `json:"project_id"`
	SourceEventID      *string        `json:"source_event_id"`
	IngestSeq          int64          `json:"ingest_seq"`
	ReceivedAtUS       int64          `json:"received_at_us"`
	TimestampUS        int64          `json:"timestamp_us"`
	Service            string         `json:"service"`
	Environment        *string        `json:"environment"`
	Release            *string        `json:"release"`
	Level              string         `json:"level"`
	Logger             *string        `json:"logger"`
	Message            string         `json:"message"`
	TraceID            *string        `json:"trace_id"`
	SpanID             *string        `json:"span_id"`
	RequestID          *string        `json:"request_id"`
	UserID             *string        `json:"user_id"`
	UserEmail          *string        `json:"user_email"`
	IssueID            *string        `json:"issue_id"`
	FingerprintVersion *uint32        `json:"fingerprint_version"`
	Fingerprint        *string        `json:"fingerprint"`
	Attributes         map[string]any `json:"attributes"`
	SearchText         string         `json:"search_text"`
	RawJSON            map[string]any `json:"raw_json"`
	NormalizerVersion  uint32         `json:"normalizer_version"`
	IndexingWarnings   []string       `json:"indexing_warnings"`
}

type inboxPayload struct {
	Version uint32   `json:"version"`
	Records []record `json:"records"`
}

type inboxChunk struct {
	id          int64
	firstSeq    int64
	lastSeq     int64
	recordCount int
	receivedAt  int64
	bytes       []byte
	payload     inboxPayload
}

type stagedChunk struct {
	firstSeq   int64
	lastSeq    int64
	receivedAt int64
	count      int
	bytes      []byte
}

type indexerPerformance struct {
	Batches          uint64 `json:"batches"`
	Records          uint64 `json:"records"`
	PrepareUS        uint64 `json:"prepare_us"`
	NativeCommitUS   uint64 `json:"native_commit_us"`
	SQLiteFinalizeUS uint64 `json:"sqlite_finalize_us"`
	ReaderPublishUS  uint64 `json:"reader_publish_us"`
	ActiveSizeUS     uint64 `json:"active_size_us"`
	MaxBatchUS       uint64 `json:"max_batch_us"`
	MaxBatchRecords  uint64 `json:"max_batch_records"`
}

type distribution struct {
	Samples int    `json:"samples"`
	P50     uint64 `json:"p50"`
	P95     uint64 `json:"p95"`
	P99     uint64 `json:"p99"`
	Max     uint64 `json:"max"`
}

type inboxSample struct {
	ElapsedMS int64 `json:"elapsed_ms"`
	Bytes     int64 `json:"bytes"`
}

type report struct {
	FormatVersion                   int                `json:"format_version"`
	DurationUnit                    string             `json:"duration_unit"`
	SizeUnit                        string             `json:"size_unit"`
	Backend                         string             `json:"backend"`
	Profile                         string             `json:"profile"`
	TotalElapsedMS                  int64              `json:"total_elapsed_ms"`
	Records                         int                `json:"records"`
	Logs                            int                `json:"logs"`
	Errors                          int                `json:"errors"`
	LateRecords                     int                `json:"late_records"`
	AttributesPerRecord             int                `json:"attributes_per_record"`
	HighCardinalityFields           []string           `json:"high_cardinality_fields"`
	RecordBytes                     distribution       `json:"record_bytes"`
	AcceptanceBatchRecords          int                `json:"acceptance_batch_records"`
	AcceptanceLatency               distribution       `json:"acceptance_latency"`
	SeedElapsedMS                   int64              `json:"seed_elapsed_ms"`
	SeedRecordsPerSecond            float64            `json:"seed_records_per_second"`
	VisibilityLagMS                 int64              `json:"visibility_lag_ms"`
	IndexerPerformance              indexerPerformance `json:"indexer_performance"`
	ActiveSegments                  int                `json:"active_segments"`
	WarmupDurationMS                int64              `json:"warmup_duration_ms"`
	SustainedDurationMS             int64              `json:"sustained_duration_ms"`
	DrainDurationMS                 int64              `json:"drain_duration_ms"`
	SustainedTargetRecordsPerSecond int                `json:"sustained_target_records_per_second"`
	SustainedRecords                int                `json:"sustained_records"`
	SustainedAcceptanceLatency      distribution       `json:"sustained_acceptance_latency"`
	SustainedStructuredSearch       distribution       `json:"sustained_structured_search_latency"`
	SustainedTextSearch             distribution       `json:"sustained_text_search_latency"`
	SustainedHistogram              distribution       `json:"sustained_histogram_latency"`
	InboxBytesAtSustainedStart      int64              `json:"inbox_bytes_at_sustained_start"`
	InboxBytesPeak                  int64              `json:"inbox_bytes_peak"`
	InboxBytesTimeline              []inboxSample      `json:"inbox_bytes_timeline"`
	StructuredSearch                distribution       `json:"structured_search_latency"`
	TextSearch                      distribution       `json:"text_search_latency"`
	Histogram                       distribution       `json:"histogram_latency"`
	SearchedShards                  int                `json:"searched_shards"`
	SQLiteBytes                     uint64             `json:"sqlite_bytes"`
	WALBytes                        uint64             `json:"wal_bytes"`
	IndexBytes                      uint64             `json:"index_bytes"`
	DataDirBytes                    uint64             `json:"data_dir_bytes"`
	DiskBudgetBytes                 *uint64            `json:"disk_budget_bytes"`
	InboxRecordsAfterDrain          int64              `json:"inbox_records_after_drain"`
	InboxBytesAfterDrain            int64              `json:"inbox_bytes_after_drain"`
	IssueCount                      int64              `json:"issue_count"`
	IssueOccurrenceCount            int64              `json:"issue_occurrence_count"`
	Cgroup                          map[string]string  `json:"cgroup"`
	Process                         map[string]string  `json:"process"`
	Revision                        string             `json:"revision"`
	GoVersion                       string             `json:"go_version"`
	GoArch                          string             `json:"go_arch"`
	SQLiteDriver                    string             `json:"sqlite_driver"`
	NativeFormat                    string             `json:"native_format"`
	Limitations                     []string           `json:"limitations"`
}

type options struct {
	records       int
	reportPath    string
	dataDir       string
	profile       string
	warmupSeconds int
	sustainedSecs int
	drainSeconds  int
	targetRPS     int
	repetitions   int
}

type indexer struct {
	db       *sql.DB
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	mu       sync.Mutex
	perf     indexerPerformance
	err      error
	lastWake time.Time
}

func main() {
	var opts options
	flag.IntVar(&opts.records, "records", envInt("EVENTGLASS_BENCH_RECORDS", 10_000), "number of seed records")
	flag.StringVar(&opts.reportPath, "report", os.Getenv("EVENTGLASS_BENCH_REPORT"), "JSON report path")
	flag.StringVar(&opts.dataDir, "data-dir", os.Getenv("EVENTGLASS_BENCH_DATA_DIR"), "benchmark data directory")
	flag.StringVar(&opts.profile, "profile", envString("EVENTGLASS_BENCH_PROFILE", "capacity"), "report profile name")
	flag.IntVar(&opts.warmupSeconds, "warmup-seconds", envInt("EVENTGLASS_BENCH_WARMUP_SECONDS", 0), "warmup duration")
	flag.IntVar(&opts.sustainedSecs, "sustained-seconds", envInt("EVENTGLASS_BENCH_SUSTAINED_SECONDS", 0), "sustained duration")
	flag.IntVar(&opts.drainSeconds, "drain-seconds", envInt("EVENTGLASS_BENCH_DRAIN_SECONDS", 0), "drain duration")
	flag.IntVar(&opts.targetRPS, "records-per-second", envInt("EVENTGLASS_BENCH_RECORDS_PER_SECOND", 0), "sustained target rate")
	flag.IntVar(&opts.repetitions, "query-repetitions", 30, "final query repetitions")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintf(os.Stderr, "go benchmark failed: %v\n", err)
		os.Exit(1)
	}
}

func run(opts options) error {
	totalStarted := time.Now()
	if opts.records <= 0 {
		return errors.New("records must be positive")
	}
	if opts.repetitions <= 0 {
		return errors.New("query-repetitions must be positive")
	}
	if opts.dataDir == "" {
		directory, err := os.MkdirTemp("", "eventglass-go-benchmark-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(directory)
		opts.dataDir = directory
	} else if err := os.MkdirAll(opts.dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	db, err := openDatabase(opts.dataDir)
	if err != nil {
		return err
	}
	defer db.Close()

	index := startIndexer(db)
	defer func() {
		index.stopIndexer()
	}()

	receivedBase := time.Now().UnixMicro()
	acceptanceLatencies := make([]uint64, 0, (opts.records+batchRecords-1)/batchRecords)
	recordSizes := make([]uint64, 0, min(opts.records, 100_000))
	accepted := 0
	seedStarted := time.Now()
	for accepted < opts.records {
		end := min(accepted+batchRecords, opts.records)
		batch := make([]record, 0, end-accepted)
		for i := accepted; i < end; i++ {
			value := makeRecord(i, receivedBase)
			encoded, encodeErr := json.Marshal(value)
			if encodeErr != nil {
				return fmt.Errorf("marshal record %d: %w", i, encodeErr)
			}
			if len(recordSizes) < cap(recordSizes) {
				recordSizes = append(recordSizes, uint64(len(encoded)))
			}
			batch = append(batch, value)
		}
		started := time.Now()
		if err := accept(db, batch, fmt.Sprintf("benchmark-%d", accepted/batchRecords)); err != nil {
			return err
		}
		acceptanceLatencies = append(acceptanceLatencies, elapsedUS(started))
		accepted = end
		index.wakeIndexer()
		if accepted%10_000 == 0 {
			if err := waitForInboxBudget(index, 128*1024*1024); err != nil {
				return err
			}
		}
	}
	seedElapsed := time.Since(seedStarted)
	visibilityStarted := time.Now()
	if err := waitForVisibility(index, opts.records); err != nil {
		return err
	}
	visibilityLag := time.Since(visibilityStarted)

	if err := index.lastError(); err != nil {
		return err
	}
	warmupElapsed, sustainedElapsed, drainElapsed, sustainedRecords, sustainedAcceptance, sustainedStructured, sustainedText, sustainedHistogram, inboxStart, inboxPeak, timeline, err := runSustained(index, db, opts, receivedBase, opts.records)
	if err != nil {
		return err
	}
	totalRecords := opts.records + sustainedRecords
	if err := waitForVisibility(index, totalRecords); err != nil {
		return err
	}

	if err := verifyQueries(db, totalRecords); err != nil {
		return err
	}
	structured, err := measureQueries(opts.repetitions, func() error { return structuredSearch(db, totalRecords) })
	if err != nil {
		return err
	}
	textSearch, err := measureQueries(opts.repetitions, func() error { return textSearchQuery(db, totalRecords) })
	if err != nil {
		return err
	}
	histogram, err := measureQueries(opts.repetitions, func() error { return histogramQuery(db, totalRecords) })
	if err != nil {
		return err
	}

	inboxRecords, inboxBytes, err := inboxState(db)
	if err != nil {
		return err
	}
	issues, err := scalar(db, "SELECT count(*) FROM issues")
	if err != nil {
		return err
	}
	occurrences, err := scalar(db, "SELECT count(*) FROM issue_occurrences")
	if err != nil {
		return err
	}
	if inboxRecords != 0 || inboxBytes != 0 {
		return fmt.Errorf("drain left Inbox state records=%d bytes=%d", inboxRecords, inboxBytes)
	}
	if occurrences != int64(totalRecords/21+(totalRecords%21+20)/21) {
		return fmt.Errorf("unexpected issue occurrence count: got %d", occurrences)
	}

	sqliteBytes := fileSize(filepath.Join(opts.dataDir, "meta.db"))
	walBytes := fileSize(filepath.Join(opts.dataDir, "meta.db-wal"))
	dataDirBytes := treeSize(opts.dataDir)
	var diskBudget *uint64
	if raw := os.Getenv("EVENTGLASS_BENCH_DISK_BUDGET_BYTES"); raw != "" {
		value, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil {
			return fmt.Errorf("invalid EVENTGLASS_BENCH_DISK_BUDGET_BYTES: %w", parseErr)
		}
		diskBudget = &value
		if dataDirBytes > value {
			return fmt.Errorf("data directory exceeds disk budget: %d > %d", dataDirBytes, value)
		}
	}

	perf := index.performance()
	limitations := []string{
		"HTTP wire ACK latency is not included; the production durable operation is called directly",
		"Go uses SQLite FTS5 for the native-search comparison; this is not a Tantivy format or engine comparison",
		"S3 checkpoint, cold hydration, replay, alerts, and UI are outside this first core-path benchmark",
	}
	if opts.warmupSeconds == 0 && opts.sustainedSecs == 0 && opts.drainSeconds == 0 {
		limitations = append(limitations, "capacity profile omits the sustained mixed workload")
	}

	result := report{
		FormatVersion:                   1,
		DurationUnit:                    "microseconds",
		SizeUnit:                        "bytes",
		Backend:                         "go-sqlite-fts5",
		Profile:                         opts.profile,
		TotalElapsedMS:                  time.Since(totalStarted).Milliseconds(),
		Records:                         totalRecords,
		Logs:                            totalRecords - (totalRecords+20)/21,
		Errors:                          (totalRecords + 20) / 21,
		LateRecords:                     (totalRecords + 96) / 97,
		AttributesPerRecord:             15,
		HighCardinalityFields:           []string{"host", "customer_id", "request_id"},
		RecordBytes:                     makeDistribution(recordSizes),
		AcceptanceBatchRecords:          batchRecords,
		AcceptanceLatency:               makeDistribution(acceptanceLatencies),
		SeedElapsedMS:                   seedElapsed.Milliseconds(),
		SeedRecordsPerSecond:            float64(opts.records) / maxFloat(seedElapsed.Seconds(), 0.000001),
		VisibilityLagMS:                 visibilityLag.Milliseconds(),
		IndexerPerformance:              perf,
		ActiveSegments:                  1,
		WarmupDurationMS:                warmupElapsed,
		SustainedDurationMS:             sustainedElapsed,
		DrainDurationMS:                 drainElapsed,
		SustainedTargetRecordsPerSecond: opts.targetRPS,
		SustainedRecords:                sustainedRecords,
		SustainedAcceptanceLatency:      makeDistribution(sustainedAcceptance),
		SustainedStructuredSearch:       makeDistribution(sustainedStructured),
		SustainedTextSearch:             makeDistribution(sustainedText),
		SustainedHistogram:              makeDistribution(sustainedHistogram),
		InboxBytesAtSustainedStart:      inboxStart,
		InboxBytesPeak:                  inboxPeak,
		InboxBytesTimeline:              timeline,
		StructuredSearch:                makeDistribution(structured),
		TextSearch:                      makeDistribution(textSearch),
		Histogram:                       makeDistribution(histogram),
		SearchedShards:                  1,
		SQLiteBytes:                     sqliteBytes,
		WALBytes:                        walBytes,
		IndexBytes:                      0,
		DataDirBytes:                    dataDirBytes,
		DiskBudgetBytes:                 diskBudget,
		InboxRecordsAfterDrain:          inboxRecords,
		InboxBytesAfterDrain:            inboxBytes,
		IssueCount:                      issues,
		IssueOccurrenceCount:            occurrences,
		Cgroup:                          cgroupEvidence(),
		Process:                         processEvidence(),
		Revision:                        envString("EVENTGLASS_BENCH_REVISION", "working-tree"),
		GoVersion:                       runtime.Version(),
		GoArch:                          runtime.GOOS + "/" + runtime.GOARCH,
		SQLiteDriver:                    "github.com/mattn/go-sqlite3",
		NativeFormat:                    "sqlite-fts5",
		Limitations:                     limitations,
	}
	if opts.reportPath == "" {
		encoded, marshalErr := json.MarshalIndent(result, "", "  ")
		if marshalErr != nil {
			return marshalErr
		}
		fmt.Println(string(encoded))
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(opts.reportPath), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(opts.reportPath, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("Go benchmark evidence: %s\n", opts.reportPath)
	fmt.Printf("Passed: %d records; peak: %s; seed: %.1f records/s\n", totalRecords, result.Cgroup["memory.peak"], result.SeedRecordsPerSecond)
	return nil
}

func openDatabase(dataDir string) (*sql.DB, error) {
	path := filepath.Join(dataDir, "meta.db")
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	pragmas := []string{
		"PRAGMA auto_vacuum=INCREMENTAL",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA cache_size=-4096",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create benchmark schema: %w", err)
	}
	if _, err := db.Exec("INSERT INTO projects(id,slug,name) VALUES(1,'benchmark','Benchmark'); INSERT INTO project_keys(id,project_id,public_key) VALUES(1,1,'benchmark-public-key'); INSERT INTO runtime_state(singleton,next_ingest_seq) VALUES(1,1);"); err != nil {
		db.Close()
		return nil, fmt.Errorf("seed benchmark metadata: %w", err)
	}
	return db, nil
}

const schema = `
CREATE TABLE projects (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL);
CREATE TABLE project_keys (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, public_key TEXT NOT NULL UNIQUE);
CREATE TABLE runtime_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    next_ingest_seq INTEGER NOT NULL,
    last_applied_inbox_id INTEGER NOT NULL DEFAULT 0,
    last_applied_ingest_seq INTEGER NOT NULL DEFAULT 0,
    inbox_bytes INTEGER NOT NULL DEFAULT 0,
    inbox_records INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    acceptance_id TEXT NOT NULL,
    chunk_no INTEGER NOT NULL,
    first_ingest_seq INTEGER NOT NULL,
    last_ingest_seq INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    received_at_us INTEGER NOT NULL,
    payload BLOB NOT NULL,
    UNIQUE(acceptance_id, chunk_no)
);
CREATE TABLE records (
    ingest_seq INTEGER PRIMARY KEY,
    record_id TEXT NOT NULL UNIQUE,
    project_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    timestamp_us INTEGER NOT NULL,
    received_at_us INTEGER NOT NULL,
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    environment TEXT,
    release TEXT,
    logger TEXT,
    issue_id TEXT,
    fingerprint TEXT,
    region TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    search_text TEXT NOT NULL
);
CREATE INDEX records_project_time ON records(project_id, timestamp_us, ingest_seq);
CREATE INDEX records_region ON records(project_id, region, timestamp_us, ingest_seq);
CREATE VIRTUAL TABLE records_fts USING fts5(ingest_seq UNINDEXED, project_id UNINDEXED, timestamp_us UNINDEXED, search_text, tokenize='unicode61');
CREATE TABLE issues (
    id TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    title TEXT NOT NULL,
    level TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    UNIQUE(project_id, fingerprint)
);
CREATE TABLE issue_occurrences (
    event_key TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL,
    issue_id TEXT NOT NULL,
    record_id TEXT NOT NULL UNIQUE,
    ingest_seq INTEGER NOT NULL UNIQUE,
    occurred_at_us INTEGER NOT NULL
);
`

func accept(db *sql.DB, values []record, acceptanceID string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin acceptance: %w", err)
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		return cause
	}
	var next int64
	if err := tx.QueryRow("SELECT next_ingest_seq FROM runtime_state WHERE singleton=1").Scan(&next); err != nil {
		return rollback(fmt.Errorf("read ingest sequence: %w", err))
	}
	chunks := make([]stagedChunk, 0, (len(values)+chunkRecords-1)/chunkRecords)
	current := make([][]byte, 0, chunkRecords)
	var currentFirst, currentLast, currentReceived int64
	approximateBytes := 0
	totalBytes := 0
	for _, input := range values {
		if input.ProjectID != 1 || input.IngestSeq != 0 || input.NormalizerVersion != 1 {
			return rollback(errors.New("invalid normalized record for acceptance"))
		}
		value := input
		value.IngestSeq = next
		next++
		encoded, err := json.Marshal(value)
		if err != nil {
			return rollback(fmt.Errorf("marshal accepted record: %w", err))
		}
		if len(encoded) > 1024*1024 {
			return rollback(errors.New("record exceeds hard limit"))
		}
		if len(current) > 0 && (len(current) >= chunkRecords || approximateBytes+len(encoded)+32 > chunkBytes) {
			chunk := serializeChunk(current, currentFirst, currentLast, currentReceived)
			totalBytes += len(chunk.bytes)
			chunks = append(chunks, chunk)
			current = current[:0]
			approximateBytes = 0
		}
		if len(current) == 0 {
			currentFirst = value.IngestSeq
			currentReceived = value.ReceivedAtUS
		}
		currentLast = value.IngestSeq
		approximateBytes += len(encoded) + 1
		current = append(current, encoded)
	}
	if len(current) > 0 {
		chunk := serializeChunk(current, currentFirst, currentLast, currentReceived)
		totalBytes += len(chunk.bytes)
		chunks = append(chunks, chunk)
	}
	if totalBytes > 20*1024*1024 {
		return rollback(errors.New("acceptance exceeds decoded byte limit"))
	}
	insert, err := tx.Prepare("INSERT INTO inbox(project_id,acceptance_id,chunk_no,first_ingest_seq,last_ingest_seq,record_count,received_at_us,payload) VALUES(?,?,?,?,?,?,?,?)")
	if err != nil {
		return rollback(err)
	}
	for index, chunk := range chunks {
		if _, err := insert.Exec(1, acceptanceID, index, chunk.firstSeq, chunk.lastSeq, chunk.count, chunk.receivedAt, chunk.bytes); err != nil {
			insert.Close()
			return rollback(fmt.Errorf("insert Inbox chunk: %w", err))
		}
	}
	if err := insert.Close(); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec("UPDATE runtime_state SET next_ingest_seq=?, inbox_bytes=inbox_bytes+?, inbox_records=inbox_records+? WHERE singleton=1", next, totalBytes, len(values)); err != nil {
		return rollback(fmt.Errorf("update Inbox counters: %w", err))
	}
	var authorizedProject int64
	if err := tx.QueryRow("SELECT p.id FROM projects p JOIN project_keys k ON k.project_id=p.id WHERE p.id=1 AND p.slug='benchmark' AND k.public_key='benchmark-public-key'").Scan(&authorizedProject); err != nil {
		return rollback(fmt.Errorf("authorize project: %w", err))
	}
	if authorizedProject != 1 {
		return rollback(errors.New("benchmark project authorization failed"))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit acceptance: %w", err)
	}
	return nil
}

func serializeChunk(records [][]byte, first, last, received int64) stagedChunk {
	size := len(`{"version":1,"records":[`) + len(`]}`)
	for index, record := range records {
		size += len(record)
		if index > 0 {
			size++
		}
	}
	bytes := make([]byte, 0, size)
	bytes = append(bytes, `{"version":1,"records":[`...)
	for index, record := range records {
		if index > 0 {
			bytes = append(bytes, ',')
		}
		bytes = append(bytes, record...)
	}
	bytes = append(bytes, ']', '}')
	return stagedChunk{firstSeq: first, lastSeq: last, receivedAt: received, count: len(records), bytes: bytes}
}

func startIndexer(db *sql.DB) *indexer {
	value := &indexer{db: db, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	go value.loop()
	return value
}

func (i *indexer) loop() {
	defer close(i.done)
	for {
		select {
		case <-i.wake:
			for {
				processed, err := i.drainOnce()
				if err != nil {
					i.mu.Lock()
					i.err = err
					i.mu.Unlock()
					return
				}
				if !processed {
					break
				}
			}
		case <-i.stop:
			return
		}
	}
}

func (i *indexer) wakeIndexer() {
	i.mu.Lock()
	i.lastWake = time.Now()
	i.mu.Unlock()
	select {
	case i.wake <- struct{}{}:
	default:
	}
}

func (i *indexer) stopIndexer() {
	select {
	case <-i.done:
		return
	default:
	}
	close(i.stop)
	<-i.done
}

func (i *indexer) lastError() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.err
}

func (i *indexer) performance() indexerPerformance {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.perf
}

func (i *indexer) addPerformance(prepare, native, finalize, publish, active time.Duration, records int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.perf.Batches++
	i.perf.Records += uint64(records)
	i.perf.PrepareUS += uint64(prepare.Microseconds())
	i.perf.NativeCommitUS += uint64(native.Microseconds())
	i.perf.SQLiteFinalizeUS += uint64(finalize.Microseconds())
	i.perf.ReaderPublishUS += uint64(publish.Microseconds())
	i.perf.ActiveSizeUS += uint64(active.Microseconds())
	batchUS := uint64((prepare + native + finalize + publish + active).Microseconds())
	if batchUS > i.perf.MaxBatchUS {
		i.perf.MaxBatchUS = batchUS
	}
	if uint64(records) > i.perf.MaxBatchRecords {
		i.perf.MaxBatchRecords = uint64(records)
	}
}

func (i *indexer) drainOnce() (bool, error) {
	prepareStarted := time.Now()
	chunks, err := loadChunks(i.db)
	prepareElapsed := time.Since(prepareStarted)
	if err != nil {
		return false, err
	}
	if len(chunks) == 0 {
		return false, nil
	}
	nativeStarted := time.Now()
	if err := i.nativeCommit(chunks); err != nil {
		return false, err
	}
	nativeElapsed := time.Since(nativeStarted)
	finalizeStarted := time.Now()
	if err := i.finalize(chunks); err != nil {
		return false, err
	}
	finalizeElapsed := time.Since(finalizeStarted)
	// FTS5 searches use the committed SQLite index directly. There is no reader
	// reload or filesystem-size scan analogous to a Tantivy active shard.
	i.addPerformance(prepareElapsed, nativeElapsed, finalizeElapsed, 0, 0, countChunkRecords(chunks))
	return true, nil
}

func loadChunks(db *sql.DB) ([]inboxChunk, error) {
	var applied int64
	if err := db.QueryRow("SELECT last_applied_inbox_id FROM runtime_state WHERE singleton=1").Scan(&applied); err != nil {
		return nil, err
	}
	rows, err := db.Query("SELECT id,first_ingest_seq,last_ingest_seq,record_count,received_at_us,payload FROM inbox WHERE id>? ORDER BY id LIMIT 1000", applied)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	chunks := make([]inboxChunk, 0, 8)
	count := 0
	bytesTotal := 0
	for rows.Next() {
		var chunk inboxChunk
		if err := rows.Scan(&chunk.id, &chunk.firstSeq, &chunk.lastSeq, &chunk.recordCount, &chunk.receivedAt, &chunk.bytes); err != nil {
			return nil, err
		}
		if err := decodePayload(chunk.bytes, &chunk.payload); err != nil {
			return nil, fmt.Errorf("decode Inbox chunk %d: %w", chunk.id, err)
		}
		canonical, err := json.Marshal(chunk.payload)
		if err != nil {
			return nil, fmt.Errorf("re-encode Inbox chunk %d: %w", chunk.id, err)
		}
		if !bytes.Equal(canonical, chunk.bytes) {
			return nil, fmt.Errorf("Inbox chunk %d is not canonical JSON", chunk.id)
		}
		if chunk.payload.Version != 1 || len(chunk.payload.Records) == 0 || len(chunk.payload.Records) != chunk.recordCount {
			return nil, fmt.Errorf("invalid Inbox chunk %d metadata", chunk.id)
		}
		if chunk.payload.Records[0].IngestSeq != chunk.firstSeq || chunk.payload.Records[len(chunk.payload.Records)-1].IngestSeq != chunk.lastSeq {
			return nil, fmt.Errorf("Inbox chunk %d boundary mismatch", chunk.id)
		}
		for index := 1; index < len(chunk.payload.Records); index++ {
			if chunk.payload.Records[index].IngestSeq != chunk.payload.Records[index-1].IngestSeq+1 {
				return nil, fmt.Errorf("Inbox chunk %d sequence mismatch", chunk.id)
			}
		}
		if count+len(chunk.payload.Records) > batchRecords || bytesTotal+len(chunk.bytes) > batchBytes {
			if len(chunks) == 0 {
				return nil, fmt.Errorf("first Inbox chunk exceeds batch budget")
			}
			break
		}
		count += len(chunk.payload.Records)
		bytesTotal += len(chunk.bytes)
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func decodePayload(raw []byte, payload *inboxPayload) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(payload)
}

func (i *indexer) nativeCommit(chunks []inboxChunk) error {
	tx, err := i.db.Begin()
	if err != nil {
		return err
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		return cause
	}
	recordInsert, err := tx.Prepare("INSERT INTO records(ingest_seq,record_id,project_id,kind,timestamp_us,received_at_us,service,level,environment,release,logger,issue_id,fingerprint,region,attributes_json,search_text) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		return rollback(err)
	}
	ftsInsert, err := tx.Prepare("INSERT INTO records_fts(rowid,ingest_seq,project_id,timestamp_us,search_text) VALUES(?,?,?,?,?)")
	if err != nil {
		recordInsert.Close()
		return rollback(err)
	}
	for _, chunk := range chunks {
		for _, value := range chunk.payload.Records {
			attributes, err := json.Marshal(value.Attributes)
			if err != nil {
				ftsInsert.Close()
				recordInsert.Close()
				return rollback(err)
			}
			region, ok := value.Attributes["region"].(string)
			if !ok {
				ftsInsert.Close()
				recordInsert.Close()
				return rollback(fmt.Errorf("record %d has no region", value.IngestSeq))
			}
			args := []any{value.IngestSeq, value.RecordID, value.ProjectID, string(value.Kind), value.TimestampUS, value.ReceivedAtUS, value.Service, value.Level, nullableString(value.Environment), nullableString(value.Release), nullableString(value.Logger), nullableString(value.IssueID), nullableString(value.Fingerprint), region, string(attributes), value.SearchText}
			if _, err := recordInsert.Exec(args...); err != nil {
				ftsInsert.Close()
				recordInsert.Close()
				return rollback(fmt.Errorf("insert native record %d: %w", value.IngestSeq, err))
			}
			if _, err := ftsInsert.Exec(value.IngestSeq, value.IngestSeq, value.ProjectID, value.TimestampUS, value.SearchText); err != nil {
				ftsInsert.Close()
				recordInsert.Close()
				return rollback(fmt.Errorf("insert FTS record %d: %w", value.IngestSeq, err))
			}
		}
	}
	if err := ftsInsert.Close(); err != nil {
		recordInsert.Close()
		return rollback(err)
	}
	if err := recordInsert.Close(); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (i *indexer) finalize(chunks []inboxChunk) error {
	last := chunks[len(chunks)-1]
	totalRecords := countChunkRecords(chunks)
	totalBytes := countChunkBytes(chunks)
	tx, err := i.db.Begin()
	if err != nil {
		return err
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		return cause
	}
	issueInsert, err := tx.Prepare(`INSERT INTO issues(id,project_id,fingerprint,title,level,occurrence_count,first_seen_us,last_seen_us)
VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(project_id,fingerprint) DO UPDATE SET occurrence_count=occurrence_count+1,last_seen_us=excluded.last_seen_us,level=excluded.level`)
	if err != nil {
		return rollback(err)
	}
	occurrenceInsert, err := tx.Prepare("INSERT INTO issue_occurrences(event_key,project_id,issue_id,record_id,ingest_seq,occurred_at_us) VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING")
	if err != nil {
		issueInsert.Close()
		return rollback(err)
	}
	for _, chunk := range chunks {
		for _, value := range chunk.payload.Records {
			if value.Kind != errorKind {
				continue
			}
			if _, err := issueInsert.Exec(nullableString(value.IssueID), value.ProjectID, nullableString(value.Fingerprint), value.Message, value.Level, 1, value.TimestampUS, value.TimestampUS); err != nil {
				occurrenceInsert.Close()
				issueInsert.Close()
				return rollback(err)
			}
			if _, err := occurrenceInsert.Exec(value.RecordID, value.ProjectID, nullableString(value.IssueID), value.RecordID, value.IngestSeq, value.TimestampUS); err != nil {
				occurrenceInsert.Close()
				issueInsert.Close()
				return rollback(err)
			}
		}
	}
	if err := occurrenceInsert.Close(); err != nil {
		issueInsert.Close()
		return rollback(err)
	}
	if err := issueInsert.Close(); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec("UPDATE runtime_state SET last_applied_inbox_id=?, last_applied_ingest_seq=?, inbox_bytes=inbox_bytes-?, inbox_records=inbox_records-? WHERE singleton=1", last.id, last.lastSeq, totalBytes, totalRecords); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec("DELETE FROM inbox WHERE id<=? AND id>?", last.id, chunks[0].id-1); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func runSustained(index *indexer, db *sql.DB, opts options, receivedBase int64, seedRecords int) (int64, int64, int64, int, []uint64, []uint64, []uint64, []uint64, int64, int64, []inboxSample, error) {
	if opts.warmupSeconds == 0 && opts.sustainedSecs == 0 && opts.drainSeconds == 0 {
		return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, nil
	}
	if opts.warmupSeconds != 300 || opts.sustainedSecs != 1800 || opts.drainSeconds != 600 || opts.targetRPS != 105 {
		return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, errors.New("sustained benchmark requires 300s warmup, 1800s sustained, 600s drain, and 105 records/s")
	}
	warmupStarted := time.Now()
	for time.Since(warmupStarted) < time.Duration(opts.warmupSeconds)*time.Second {
		if _, _, _, err := queryMix(db, seedRecords); err != nil {
			return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, err
		}
		sleepUntilSecond(warmupStarted)
	}
	warmupElapsed := time.Since(warmupStarted).Milliseconds()
	inboxStart, err := inboxBytesOnly(db)
	if err != nil {
		return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, err
	}
	inboxPeak := inboxStart
	timeline := []inboxSample{{ElapsedMS: 0, Bytes: inboxStart}}
	var accepted int
	var acceptance, structured, textValues, histogram []uint64
	sustainedStarted := time.Now()
	maximum := opts.sustainedSecs * opts.targetRPS
	for time.Since(sustainedStarted) < time.Duration(opts.sustainedSecs)*time.Second {
		scheduled := min(int(time.Since(sustainedStarted).Seconds()*float64(opts.targetRPS)), maximum)
		if scheduled > accepted {
			start := seedRecords + accepted
			end := seedRecords + scheduled
			batch := make([]record, 0, end-start)
			for value := start; value < end; value++ {
				batch = append(batch, makeRecord(value, receivedBase))
			}
			started := time.Now()
			if err := accept(db, batch, fmt.Sprintf("benchmark-sustained-%d", accepted)); err != nil {
				return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, err
			}
			acceptance = append(acceptance, elapsedUS(started))
			accepted = scheduled
			index.wakeIndexer()
		}
		values, _, _, err := queryMix(db, seedRecords+accepted)
		if err != nil {
			return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, err
		}
		structured = append(structured, values[0])
		textValues = append(textValues, values[1])
		histogram = append(histogram, values[2])
		pending, err := inboxBytesOnly(db)
		if err != nil {
			return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, err
		}
		inboxPeak = maxInt64(inboxPeak, pending)
		timeline = append(timeline, inboxSample{ElapsedMS: time.Since(sustainedStarted).Milliseconds(), Bytes: pending})
		sleepUntilSecond(sustainedStarted)
	}
	if accepted < maximum {
		start := seedRecords + accepted
		end := seedRecords + maximum
		batch := make([]record, 0, end-start)
		for value := start; value < end; value++ {
			batch = append(batch, makeRecord(value, receivedBase))
		}
		started := time.Now()
		if err := accept(db, batch, fmt.Sprintf("benchmark-sustained-%d", accepted)); err != nil {
			return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, err
		}
		acceptance = append(acceptance, elapsedUS(started))
		accepted = maximum
		index.wakeIndexer()
	}
	sustainedElapsed := time.Since(sustainedStarted).Milliseconds()
	drainStarted := time.Now()
	for time.Since(drainStarted) < time.Duration(opts.drainSeconds)*time.Second {
		index.wakeIndexer()
		pending, err := inboxBytesOnly(db)
		if err != nil {
			return 0, 0, 0, 0, nil, nil, nil, nil, 0, 0, nil, err
		}
		inboxPeak = maxInt64(inboxPeak, pending)
		timeline = append(timeline, inboxSample{ElapsedMS: int64(opts.sustainedSecs*1000) + time.Since(drainStarted).Milliseconds(), Bytes: pending})
		sleepUntilSecond(drainStarted)
	}
	drainElapsed := time.Since(drainStarted).Milliseconds()
	return warmupElapsed, sustainedElapsed, drainElapsed, accepted, acceptance, structured, textValues, histogram, inboxStart, inboxPeak, timeline, nil
}

func queryMix(db *sql.DB, records int) ([3]uint64, [3]bool, [3]bool, error) {
	var durations [3]uint64
	var ok [3]bool
	var present [3]bool
	started := time.Now()
	if err := structuredSearch(db, records); err != nil {
		return durations, ok, present, err
	}
	durations[0] = elapsedUS(started)
	ok[0], present[0] = true, true
	started = time.Now()
	if err := textSearchQuery(db, records); err != nil {
		return durations, ok, present, err
	}
	durations[1] = elapsedUS(started)
	ok[1], present[1] = true, true
	started = time.Now()
	if err := histogramQueryAtLeast(db, records); err != nil {
		return durations, ok, present, err
	}
	durations[2] = elapsedUS(started)
	ok[2], present[2] = true, true
	return durations, ok, present, nil
}

func structuredSearch(db *sql.DB, records int) error {
	endUS := baseUS + int64(records)*10_000 + 60*60*1_000_000
	rows, err := db.Query("SELECT ingest_seq FROM records WHERE project_id=1 AND timestamp_us>=? AND timestamp_us<? AND ingest_seq<=? AND region=? ORDER BY timestamp_us DESC, ingest_seq DESC LIMIT 100", baseUS-24*60*60*1_000_000, endUS, records, "ap-northeast-2")
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count == 0 {
		return errors.New("structured search returned no rows")
	}
	return nil
}

func textSearchQuery(db *sql.DB, records int) error {
	// FTS rowid is explicitly the ingest sequence, so SQLite can use the FTS
	// MATCH plan and then the ordinary table's primary-key lookup for the same
	// project, timestamp, and watermark scope as the Rust benchmark.
	endUS := baseUS + int64(records)*10_000 + 60*60*1_000_000
	rows, err := db.Query("SELECT r.ingest_seq FROM records_fts f CROSS JOIN records r ON r.ingest_seq=f.rowid WHERE records_fts MATCH ? AND r.project_id=? AND r.timestamp_us>=? AND r.timestamp_us<? AND r.ingest_seq<=? ORDER BY r.timestamp_us DESC, r.ingest_seq DESC LIMIT 100", "checkout latency", 1, baseUS-24*60*60*1_000_000, endUS, records)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count == 0 {
		return errors.New("text search returned no rows")
	}
	return nil
}

func histogramQuery(db *sql.DB, records int) error {
	return histogramQueryExpectation(db, records, true)
}

func histogramQueryAtLeast(db *sql.DB, records int) error {
	return histogramQueryExpectation(db, records, false)
}

func histogramQueryExpectation(db *sql.DB, records int, exact bool) error {
	endUS := baseUS + int64(records)*10_000 + 60*60*1_000_000
	rows, err := db.Query("SELECT timestamp_us/60000000, count(*) FROM records WHERE project_id=1 AND timestamp_us>=? AND timestamp_us<? AND ingest_seq<=? GROUP BY timestamp_us/60000000 ORDER BY timestamp_us/60000000", baseUS-24*60*60*1_000_000, endUS, records)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := int64(0)
	for rows.Next() {
		var bucket, bucketCount int64
		if err := rows.Scan(&bucket, &bucketCount); err != nil {
			return err
		}
		count += bucketCount
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if exact && count != int64(records) {
		return fmt.Errorf("histogram count mismatch: got %d want %d", count, records)
	}
	if !exact && count == 0 {
		return errors.New("histogram returned no records")
	}
	return nil
}

func verifyQueries(db *sql.DB, records int) error {
	for index := 0; index < 5; index++ {
		if err := structuredSearch(db, records); err != nil {
			return err
		}
		if err := textSearchQuery(db, records); err != nil {
			return err
		}
		if err := histogramQuery(db, records); err != nil {
			return err
		}
	}
	return nil
}

func waitForInboxBudget(index *indexer, maximum int64) error {
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if err := index.lastError(); err != nil {
			return err
		}
		bytes, err := inboxBytesOnly(index.db)
		if err != nil {
			return err
		}
		if bytes <= maximum {
			return nil
		}
		index.wakeIndexer()
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("timed out waiting for Inbox budget")
}

func waitForVisibility(index *indexer, target int) error {
	deadline := time.Now().Add(3600 * time.Second)
	for time.Now().Before(deadline) {
		if err := index.lastError(); err != nil {
			return err
		}
		var applied int64
		if err := index.db.QueryRow("SELECT last_applied_ingest_seq FROM runtime_state WHERE singleton=1").Scan(&applied); err != nil {
			return err
		}
		if applied >= int64(target) {
			return nil
		}
		index.wakeIndexer()
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for visibility target %d", target)
}

func inboxState(db *sql.DB) (int64, int64, error) {
	var records, bytes int64
	if err := db.QueryRow("SELECT inbox_records,inbox_bytes FROM runtime_state WHERE singleton=1").Scan(&records, &bytes); err != nil {
		return 0, 0, err
	}
	return records, bytes, nil
}

func inboxBytesOnly(db *sql.DB) (int64, error) {
	var bytes int64
	if err := db.QueryRow("SELECT inbox_bytes FROM runtime_state WHERE singleton=1").Scan(&bytes); err != nil {
		return 0, err
	}
	return bytes, nil
}

func scalar(db *sql.DB, query string) (int64, error) {
	var value int64
	if err := db.QueryRow(query).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func measureQueries(samples int, operation func() error) ([]uint64, error) {
	values := make([]uint64, 0, samples)
	for index := 0; index < samples; index++ {
		started := time.Now()
		if err := operation(); err != nil {
			return nil, err
		}
		values = append(values, elapsedUS(started))
	}
	return values, nil
}

func makeRecord(index int, receivedBase int64) record {
	isError := index%21 == 0
	received := receivedBase + int64(index/batchRecords)*10_000
	eventTimestamp := baseUS + int64(index)*10_000
	timestamp := eventTimestamp
	if index%97 == 0 {
		timestamp -= 6 * 60 * 60 * 1_000_000
	}
	recordID := digest(fmt.Sprintf("eventglass-benchmark-record-%d", index))
	var issueID, sourceEventID, fingerprint *string
	var fingerprintVersion *uint32
	if isError {
		value := digest(fmt.Sprintf("issue-%d", index%100))
		issueID = &value
		event := fmt.Sprintf("%032x", index)
		sourceEventID = &event
		fp := digest(fmt.Sprintf("checkout-timeout-%d", index%100))
		fingerprint = &fp
		version := uint32(1)
		fingerprintVersion = &version
	}
	levels := []string{"trace", "debug", "info", "warn", "error", "fatal"}
	regions := []string{"ap-northeast-2", "us-east-1", "eu-west-1", "ap-south-1"}
	routes := []string{"/checkout", "/catalog", "/profile", "/payments"}
	region := regions[index%len(regions)]
	route := routes[index%len(routes)]
	message := ""
	if isError {
		message = fmt.Sprintf("checkout latency exceeded while charging order %08d", index)
	} else {
		message = fmt.Sprintf("checkout latency observation for request %08d on %s", index, route)
	}
	request := fmt.Sprintf("req-%016x", index)
	method := []string{"GET", "POST", "PUT", "DELETE"}[index%4]
	payloadClass := []string{"small", "medium", "large"}[index%3]
	attributes := map[string]any{
		"region": region, "host": fmt.Sprintf("node-%08d", index%50_000),
		"customer_id": fmt.Sprintf("customer-%012d", index), "route": route,
		"status_code": chooseInt(isError, 503, 200+index%5),
		"duration_ms": json.Number(fmt.Sprintf("%.1f", 5.0+float64(index%20_000)/10.0)),
		"retry":       index%13 == 0, "response_bytes": 512 + index%65_536,
		"method": method, "build": fmt.Sprintf("2026.09.%02d", index%30+1),
		"feature": fmt.Sprintf("experiment-%d", index%32), "tenant": fmt.Sprintf("tenant-%d", index%10_000),
		"shard_hint": index % 128, "attempt": index % 4, "payload_class": payloadClass,
	}
	rawJSON := map[string]any{}
	if isError {
		rawJSON = map[string]any{
			"event_id": fmt.Sprintf("%032x", index), "message": message,
			"exception": map[string]any{"values": []any{map[string]any{"type": "CheckoutTimeout", "value": "payment provider exceeded deadline", "stacktrace": map[string]any{"frames": []any{
				map[string]any{"filename": "src/http/checkout.rs", "function": "charge", "lineno": 87, "in_app": true},
				map[string]any{"filename": "src/services/payment.rs", "function": "request", "lineno": 214, "in_app": true},
			}}}}},
			"breadcrumbs": []any{map[string]any{"category": "db", "message": "loaded cart"}, map[string]any{"category": "http", "message": "called payment provider"}},
			"contexts":    map[string]any{"runtime": map[string]any{"name": "rust", "version": "1.97.1"}},
			"tags":        map[string]any{"region": region, "route": route},
		}
	} else {
		rawJSON = map[string]any{"message": message, "attributes": attributes, "request_id": request}
	}
	environment := []string{"production", "staging"}[index%2]
	release := fmt.Sprintf("2026.09.%d", index%30+1)
	logger := fmt.Sprintf("eventglass.benchmark.%d", index%16)
	service := fmt.Sprintf("checkout-%d", index%8)
	trace := fmt.Sprintf("%032x", index/4)
	span := fmt.Sprintf("%016x", index)
	user := fmt.Sprintf("user-%d", index%200_000)
	return record{
		RecordID: recordID, Kind: chooseKind(isError), ProjectID: 1, SourceEventID: sourceEventID,
		ReceivedAtUS: received, TimestampUS: timestamp, Service: service, Environment: &environment,
		Release: &release, Level: chooseLevel(isError, levels[index%len(levels)]), Logger: &logger,
		Message: message, TraceID: &trace, SpanID: &span, RequestID: &request, UserID: &user,
		IssueID: issueID, FingerprintVersion: fingerprintVersion, Fingerprint: fingerprint,
		Attributes: attributes, SearchText: message + " " + region + " " + route, RawJSON: rawJSON,
		NormalizerVersion: 1, IndexingWarnings: []string{},
	}
}

func chooseKind(errorValue bool) kind {
	if errorValue {
		return errorKind
	}
	return logKind
}

func chooseLevel(errorValue bool, fallback string) string {
	if errorValue {
		return "error"
	}
	return fallback
}

func chooseInt(condition bool, whenTrue, whenFalse int) int {
	if condition {
		return whenTrue
	}
	return whenFalse
}

func digest(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func makeDistribution(values []uint64) distribution {
	if len(values) == 0 {
		return distribution{}
	}
	sorted := append([]uint64(nil), values...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	return distribution{
		Samples: len(sorted),
		P50:     percentile(sorted, 50),
		P95:     percentile(sorted, 95),
		P99:     percentile(sorted, 99),
		Max:     sorted[len(sorted)-1],
	}
}

func percentile(values []uint64, percent int) uint64 {
	index := (len(values)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}

func countChunkRecords(chunks []inboxChunk) int {
	count := 0
	for _, chunk := range chunks {
		count += len(chunk.payload.Records)
	}
	return count
}

func countChunkBytes(chunks []inboxChunk) int {
	count := 0
	for _, chunk := range chunks {
		count += len(chunk.bytes)
	}
	return count
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func elapsedUS(started time.Time) uint64 {
	return uint64(time.Since(started).Microseconds())
}

func sleepUntilSecond(started time.Time) {
	elapsed := time.Since(started)
	next := time.Duration(elapsed/time.Second+1) * time.Second
	if next > elapsed {
		time.Sleep(next - elapsed)
	}
}

func fileSize(path string) uint64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return uint64(info.Size())
}

func treeSize(path string) uint64 {
	var total uint64
	_ = filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr == nil {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

func cgroupEvidence() map[string]string {
	result := map[string]string{}
	for _, name := range []string{"cpu.max", "cpu.stat", "memory.max", "memory.swap.max", "memory.peak", "memory.events"} {
		if value, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", name)); err == nil {
			result[name] = strings.TrimSpace(string(value))
		}
	}
	return result
}

func processEvidence() map[string]string {
	result := map[string]string{}
	value, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return result
	}
	for _, line := range strings.Split(string(value), "\n") {
		for _, field := range []string{"VmRSS:", "VmHWM:"} {
			if strings.HasPrefix(line, field) {
				result[strings.TrimSuffix(field, ":")] = strings.TrimSpace(strings.TrimPrefix(line, field))
			}
		}
	}
	return result
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
