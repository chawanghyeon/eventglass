//go:build duckdb_use_static_lib

package integration

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestPublicQueryEndToEndHTTPResumeWithIndependentWorker(t *testing.T) {
	env := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET", "EVENTGLASS_TEST_BINARY")
	f := setupAcceptFixture(t, 981)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ops, oldToken := setupQueryPrincipal(t, f)
	secret := sha256.Sum256([]byte("http query integration secret"))
	token := sha256.Sum256(secret[:])
	mac := hmac.New(sha256.New, secret[:])
	mac.Write([]byte("eventglass-csrf-v1"))
	csrf := sha256.Sum256([]byte(base64.RawURLEncoding.EncodeToString(mac.Sum(nil))))
	if _, err := f.pool.Exec(ctx, `UPDATE sessions SET token_hash=$1,csrf_hash=$2 WHERE token_hash=$3`, token[:], csrf[:], oldToken[:]); err != nil {
		t.Fatal(err)
	}
	store := integrationStore(t, "query-live-981")
	insertPublicQueryBundle(t, ctx, f, store, 202)
	codec, err := query.NewTokenCodec(query.SigningKey{ID: "http", Secret: sha256.Sum256([]byte("http integration signing"))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	disk := resource.NewBudget(4 << 30)
	cache, err := storage.NewBlockCache(filepath.Join(t.TempDir(), "cache"), storage.DefaultCacheBytes, disk)
	if err != nil {
		t.Fatal(err)
	}
	worker := &query.Workflow{Disk: disk, Cache: cache, Control: ops, Store: store, Runner: app.ProcessQueryRunner{BinaryPath: env["EVENTGLASS_TEST_BINARY"]}, InstallationID: acceptInstallationID, ScratchDir: filepath.Join(t.TempDir(), "worker")}
	startIndependentQueryWorker(t, ctx, ops, worker)
	service := &api.QueryAdapter{Control: ops, Store: store, Tokens: codec, Exporter: app.ProcessQueryExportRunner{BinaryPath: env["EVENTGLASS_TEST_BINARY"]}, ScratchDir: filepath.Join(t.TempDir(), "api"), InstallationID: acceptInstallationID, StorageGeneration: 1}
	auth, err := control.NewAuthOperations(f.pool)
	if err != nil {
		t.Fatal(err)
	}
	passwords, err := api.NewPasswordHasher(resource.NewBudget(64 << 20))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := api.NewManagementHandler(api.ManagementConfig{Auth: auth, Passwords: passwords, PublicOrigin: "http://127.0.0.1", LoginBucketKey: secret, BuildMarker: app.InstallationMarker, StoreMarker: func(context.Context, string, []byte, string) error { return nil }, Queries: reportedLive{PublicQueryService: service, t: t}})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, _ := query.CanonicalFilter(filter)
	scope, _ := query.LiveScopeHash(query.LiveScope{TenantID: f.tenantID, Projects: []int64{f.projectID}, Kinds: []model.Kind{model.KindLog}, Filter: canonical})
	positions := query.InitialLivePositions([model.LaneCount]int64{}, true)
	resume, err := codec.SignLive(1, query.PrincipalHash(f.tenantID*100+1, token), scope, positions, time.Now().Add(10*time.Minute).UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("%s/v1/live?tenant_id=%d&project_ids=%d&kinds=log", server.URL, f.tenantID, f.projectID)
	open := func(checkpoint string) *http.Response {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
		req.Header.Set("Last-Event-ID", checkpoint)
		req.AddCookie(&http.Cookie{Name: "eventglass_dev_session", Value: base64.RawURLEncoding.EncodeToString(secret[:])})
		r, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode != 200 {
			body, _ := io.ReadAll(r.Body)
			r.Body.Close()
			t.Fatalf("Live status=%d body=%s", r.StatusCode, body)
		}
		return r
	}
	initialResume := resume
	for replay := range 5 {
		resume = initialResume
		seen := map[string]bool{}
		// Close after each page; reconnect with the actual SSE ID, including a
		// checkpoint inside one 205-row batch. All delayed (>15 min) rows survive.
		for page := 0; page < 3; page++ {
			t.Logf("replay %d resuming page %d", replay, page)
			r := open(resume)
			scan := bufio.NewScanner(r.Body)
			scan.Buffer(make([]byte, 4096), query.LiveMaximumPending+8192)
			kind, id := "", ""
			got := 0
			for scan.Scan() {
				line := scan.Text()
				if strings.HasPrefix(line, "id: ") {
					id = strings.TrimPrefix(line, "id: ")
				}
				if strings.HasPrefix(line, "event: ") {
					kind = strings.TrimPrefix(line, "event: ")
				}
				if kind == "rows" && strings.HasPrefix(line, "data: ") {
					var data struct {
						Rows []struct {
							ID string `json:"record_id"`
						} `json:"rows"`
					}
					if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
						t.Fatal(err)
					}
					for _, row := range data.Rows {
						if seen[row.ID] {
							t.Fatalf("duplicate resumed row %s", row.ID)
						}
						seen[row.ID] = true
					}
					got = len(data.Rows)
					resume = id
					break
				}
			}
			r.Body.Close()
			want := 100
			if page == 2 {
				want = 5
			}
			if got != want || resume == "" {
				t.Fatalf("page=%d got=%d scan=%v", page, got, scan.Err())
			}
		}
		if len(seen) != 205 {
			t.Fatalf("received %d rows", len(seen))
		}
	}
	// Revocation on an idle stream must be observed even when cuts are unchanged.
	r := open(resume)
	defer r.Body.Close()
	var before, after int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM query_jobs WHERE tenant_id=$1`, f.tenantID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	objectsBefore := store.OperationCounts()
	time.Sleep(1100 * time.Millisecond)
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM query_jobs WHERE tenant_id=$1`, f.tenantID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after || objectsBefore != store.OperationCounts() {
		t.Fatal("idle Live created jobs or touched object storage")
	}
	if _, err := f.pool.Exec(ctx, `UPDATE memberships SET role='member' WHERE tenant_id=$1 AND user_id=$2`, f.tenantID, f.tenantID*100+1); err != nil {
		t.Fatal(err)
	}
	scan := bufio.NewScanner(r.Body)
	forbidden := false
	for scan.Scan() {
		if strings.Contains(scan.Text(), `"code":"forbidden"`) {
			forbidden = true
		}
		if strings.HasPrefix(scan.Text(), "event: rows") {
			t.Fatal("rows after revoked idle scope")
		}
	}
	if !forbidden {
		t.Fatalf("missing forbidden SSE: %v", scan.Err())
	}
	// Revoke during slow result export, after the worker already succeeded.
	if _, err := f.pool.Exec(ctx, `UPDATE memberships SET role='admin' WHERE tenant_id=$1 AND user_id=$2`, f.tenantID, f.tenantID*100+1); err != nil {
		t.Fatal(err)
	}
	guarded := *service
	guarded.Exporter = afterExport{runner: service.Exporter, after: func() error {
		_, err := f.pool.Exec(ctx, `UPDATE memberships SET role='member' WHERE tenant_id=$1 AND user_id=$2`, f.tenantID, f.tenantID*100+1)
		return err
	}}
	startToken, err := codec.SignLive(1, query.PrincipalHash(f.tenantID*100+1, token), scope, positions, time.Now().Add(time.Minute).UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	emitted := false
	err = guarded.Live(ctx, control.SessionPrincipal{UserID: f.tenantID*100 + 1}, token, query.PublicLiveRequest{TenantID: f.tenantID, ProjectIDs: []int64{f.projectID}, Kinds: []model.Kind{model.KindLog}, Filter: filter, Canonical: canonical}, startToken, func(query.LiveEvent) error { emitted = true; return nil })
	if !errors.Is(err, control.ErrForbidden) || emitted {
		t.Fatalf("post-export revoke err=%v emitted=%v", err, emitted)
	}
}

type afterExport struct {
	runner api.QueryExportRunner
	after  func() error
}

type reportedLive struct {
	api.PublicQueryService
	t *testing.T
}

func (s reportedLive) Live(ctx context.Context, p control.SessionPrincipal, token [32]byte, r query.PublicLiveRequest, resume string, emit func(query.LiveEvent) error) error {
	err := s.PublicQueryService.Live(ctx, p, token, r, resume, emit)
	if err != nil && !errors.Is(err, context.Canceled) {
		s.t.Logf("Live failure: %v", err)
	}
	return err
}

func (r afterExport) Export(ctx context.Context, request engine.QueryExportRequest) (engine.QueryExportSummary, error) {
	summary, err := r.runner.Export(ctx, request)
	if err == nil {
		err = r.after()
	}
	return summary, err
}
