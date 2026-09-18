package sdk_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/testkit"
)

type fixtureMetadata struct {
	Case    string `json:"case"`
	Capture string `json:"capture"`
	SDK     struct {
		Package string `json:"package"`
		Version string `json:"version"`
	} `json:"sdk"`
	Runtime  string `json:"runtime"`
	Requests []struct {
		File            string   `json:"file"`
		ContentEncoding string   `json:"content_encoding"`
		WireBytes       int      `json:"wire_bytes"`
		SHA256          string   `json:"sha256"`
		ItemTypes       []string `json:"item_types"`
	} `json:"requests"`
}

type fixtureHeader struct {
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

type expectedFile struct {
	Records        []expectedRecord `json:"records"`
	MustNotContain []string         `json:"must_not_contain"`
}

type expectedRecord struct {
	Kind        string `json:"kind"`
	ProjectID   int64  `json:"project_id"`
	Environment string `json:"environment"`
	Release     string `json:"release"`
	Level       string `json:"level"`
	Logger      string `json:"logger"`
	Message     string `json:"message"`
}

func TestCapturedSDKFixturesNormalizeThroughGoAPI(t *testing.T) {
	fixtureRoot := filepath.Join("..", "fixtures", "sentry")
	directories, err := os.ReadDir(fixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(directories) != 9 {
		t.Fatalf("captured fixture cases=%d, want 9", len(directories))
	}
	versions := map[string]string{
		"sentry-sdk": "2.69.0", "@sentry/node": "10.73.0", "@sentry/browser": "10.73.0", "github.com/getsentry/sentry-go": "0.49.0",
	}
	for _, directory := range directories {
		if !directory.IsDir() {
			continue
		}
		t.Run(directory.Name(), func(t *testing.T) {
			path := filepath.Join(fixtureRoot, directory.Name())
			var metadata fixtureMetadata
			readJSON(t, filepath.Join(path, "metadata.json"), &metadata)
			var headers []fixtureHeader
			readJSON(t, filepath.Join(path, "headers.json"), &headers)
			var expected expectedFile
			readJSON(t, filepath.Join(path, "expected.normalized.json"), &expected)
			if metadata.Capture != "real_sdk_to_localhost_http" || versions[metadata.SDK.Package] != metadata.SDK.Version {
				t.Fatalf("unverified SDK metadata: %#v", metadata.SDK)
			}
			if metadata.SDK.Package == "github.com/getsentry/sentry-go" && metadata.Runtime != "go version go1.26.5 darwin/arm64" {
				t.Fatalf("Go fixture runtime is not pinned: %q", metadata.Runtime)
			}
			if len(headers) != len(metadata.Requests) {
				t.Fatal("request/header manifest mismatch")
			}
			sink := &testkit.MemorySink{}
			handler, err := api.NewIngestHandler(api.Config{
				TenantID: 1, ProjectID: 1, PublicKey: "fixturePublicKey", Sink: sink,
				AllowedOrigins: []string{"http://127.0.0.1:PORT"},
				Now:            func() time.Time { return time.Unix(1_767_323_045, 0) },
			})
			if err != nil {
				t.Fatal(err)
			}
			allWire := make([]byte, 0)
			for index, requestInfo := range metadata.Requests {
				wire, err := os.ReadFile(filepath.Join(path, requestInfo.File))
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(wire)
				if len(wire) != requestInfo.WireBytes || hex.EncodeToString(digest[:]) != requestInfo.SHA256 {
					t.Fatal("wire fixture checksum mismatch")
				}
				request := httptest.NewRequest(http.MethodPost, headers[index].Path, bytes.NewReader(wire))
				for key, value := range headers[index].Headers {
					switch key {
					case "host", "content-length", "connection":
						continue
					}
					request.Header.Set(key, value)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					t.Fatalf("request %d returned %d: %s", index, response.Code, response.Body.String())
				}
				allWire = append(allWire, wire...)
			}
			actual := flattenRecords(sink.Batches())
			if diff := compareExpected(actual, expected.Records); diff != "" {
				t.Fatal(diff)
			}
			for _, forbidden := range expected.MustNotContain {
				if bytes.Contains(allWire, []byte(forbidden)) {
					t.Fatalf("forbidden SDK output %q present", forbidden)
				}
			}
			if directory.Name() == "go-events" {
				assertGoStructuredLog(t, actual)
			}
			if directory.Name() == "node-client-report" {
				outcomes := 0
				for _, batch := range sink.Batches() {
					outcomes += len(batch.Outcomes)
				}
				if outcomes == 0 {
					t.Fatal("real Node client-report item produced no outcome")
				}
			}
		})
	}
}

func assertGoStructuredLog(t *testing.T, records []model.Record) {
	t.Helper()
	for _, record := range records {
		if record.Kind != model.KindLog {
			continue
		}
		if record.MessageTemplate == nil || *record.MessageTemplate != "go structured order %s" || record.Logger == nil || *record.Logger != "fixture.go" {
			t.Fatalf("Go structured log promotions missing: %#v", record)
		}
		for _, attribute := range record.Attrs {
			if attribute.Path == "/order_id" && attribute.IntegerValue != nil && *attribute.IntegerValue == "9223372036854775807" {
				return
			}
		}
	}
	t.Fatal("Go structured log or exact int64 attribute missing")
}

func flattenRecords(batches []model.NormalizedRequest) []model.Record {
	var records []model.Record
	for _, batch := range batches {
		records = append(records, batch.Records...)
	}
	return records
}

func compareExpected(actual []model.Record, expected []expectedRecord) string {
	actualKeys := make([]string, 0, len(actual))
	expectedKeys := make([]string, 0, len(expected))
	for _, record := range actual {
		logger, environment, release := "", "", ""
		if record.Logger != nil {
			logger = *record.Logger
		}
		if record.Environment != nil {
			environment = *record.Environment
		}
		if record.Release != nil {
			release = *record.Release
		}
		actualKeys = append(actualKeys, key(string(record.Kind), record.ProjectID, environment, release, record.Level, logger, record.Message))
	}
	for _, record := range expected {
		expectedKeys = append(expectedKeys, key(record.Kind, record.ProjectID, record.Environment, record.Release, record.Level, record.Logger, record.Message))
	}
	sort.Strings(actualKeys)
	sort.Strings(expectedKeys)
	if len(actualKeys) != len(expectedKeys) {
		return "record count mismatch: actual=" + stringsJSON(actualKeys) + " expected=" + stringsJSON(expectedKeys)
	}
	for index := range actualKeys {
		if actualKeys[index] != expectedKeys[index] {
			return "records differ: actual=" + stringsJSON(actualKeys) + " expected=" + stringsJSON(expectedKeys)
		}
	}
	return ""
}

func key(kind string, project int64, environment, release, level, logger, message string) string {
	encoded, _ := json.Marshal([]any{kind, project, environment, release, level, logger, message})
	return string(encoded)
}

func stringsJSON(values []string) string { encoded, _ := json.Marshal(values); return string(encoded) }
func readJSON(t *testing.T, path string, target any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatal(err)
	}
}
