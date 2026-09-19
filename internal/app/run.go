package app

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type Runtime struct {
	config       Config
	resources    Resources
	database     *control.RuntimeDatabase
	store        *storage.S3Store
	batcher      *ingest.Batcher
	publication  *control.PublicationOperations
	queryControl *control.QueryOperations
	queryWorker  *DurableQueryWorkflow
	converter    *DurableConversionWorkflow
	publisher    *DurablePublicationWorkflow
	workerOwner  string
	handler      http.Handler
	installation control.RuntimeInstallation
	markerKey    string
	markerBytes  int64
	markerSHA    string
	ready        atomic.Bool
	started      chan struct{}
	startOnce    sync.Once
	addrMu       sync.RWMutex
	addr         string
}

func NewRuntime(ctx context.Context, config Config) (*Runtime, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if !config.Roles[RoleAPI] && !config.Roles[RoleWorker] && !config.Roles[RoleScheduler] {
		return nil, errors.New("api, worker, or scheduler role is required")
	}
	resources, err := ResourcesForRoles(config.Roles)
	if err != nil {
		return nil, err
	}
	if err := ensurePrivateDirectory(config.ScratchDir); err != nil {
		return nil, err
	}
	database, err := control.OpenRuntimeDatabase(ctx, config.DatabaseURL, resources.PoolMaxConns)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	fail := func(cause error) (*Runtime, error) {
		database.Close()
		return nil, cause
	}
	if err := database.Ping(ctx); err != nil {
		return fail(fmt.Errorf("ping PostgreSQL: %w", err))
	}
	if err := database.VerifySchema(ctx); err != nil {
		return fail(fmt.Errorf("verify runtime schema: %w", err))
	}
	identity, err := StorageIdentity(config.S3)
	if err != nil {
		return fail(err)
	}
	authOperations, err := database.AuthOperations()
	if err != nil {
		return fail(err)
	}
	if config.BootstrapTokenFile != "" {
		bootstrap, err := readHexSecret(config.BootstrapTokenFile)
		if err != nil {
			return fail(fmt.Errorf("read bootstrap token: %w", err))
		}
		installationID, err := randomUUID()
		if err != nil {
			return fail(err)
		}
		if err := authOperations.EnsureSetupInstallation(ctx, installationID, identity, sha256.Sum256(bootstrap[:])); err != nil {
			return fail(fmt.Errorf("initialize setup authority: %w", err))
		}
	}
	installation, err := database.LoadInstallation(ctx)
	if err != nil {
		return fail(fmt.Errorf("load installation: %w", err))
	}
	if installation.GlobalScrubPolicySHA != control.EmptyGlobalScrubPolicySHA {
		return fail(errors.New("global scrub policy does not match this runtime"))
	}
	if identity != installation.StorageIdentity {
		return fail(errors.New("configured S3 storage identity does not match PostgreSQL authority"))
	}
	store, err := storage.NewS3Store(ctx, config.S3)
	if err != nil {
		return fail(err)
	}
	marker, markerKey, markerSHA, err := InstallationMarker(installation.InstallationID, identity)
	if err != nil {
		return fail(err)
	}
	if installation.SetupState == control.SetupReady {
		if err := store.VerifyObject(ctx, markerKey, int64(len(marker)), markerSHA); err != nil {
			return fail(fmt.Errorf("verify installation marker: %w", err))
		}
	} else if !config.Roles[RoleAPI] {
		return fail(errors.New("an uninitialized installation requires the API role"))
	}
	runtime := &Runtime{
		config: config, resources: resources, database: database, store: store, installation: installation,
		markerKey: markerKey, markerBytes: int64(len(marker)), markerSHA: markerSHA, started: make(chan struct{}),
	}
	mux := http.NewServeMux()
	if config.Roles[RoleAPI] {
		authHashKey, err := readHexSecret(config.AuthHashKeyFile)
		if err != nil {
			return fail(fmt.Errorf("read auth hash key: %w", err))
		}
		passwords, err := api.NewPasswordHasher(resources.Working)
		if err != nil {
			return fail(err)
		}
		management, err := api.NewManagementHandler(api.ManagementConfig{
			Auth: authOperations, Passwords: passwords, PublicOrigin: strings.TrimRight(config.PublicURL, "/"),
			SecureCookie: !config.InsecureCookie, LoginBucketKey: authHashKey, BuildMarker: InstallationMarker,
			StoreMarker: func(ctx context.Context, key string, body []byte, checksum string) error {
				return store.EnsureImmutableObject(ctx, key, body, checksum)
			},
			OnSetupComplete: func() {
				runtime.ready.Store(true)
			},
		})
		if err != nil {
			return fail(err)
		}
		management.Register(mux)
		operations, err := database.IngestOperations()
		if err != nil {
			return fail(err)
		}
		processID, err := ingest.NewAcceptanceID()
		if err != nil {
			return fail(err)
		}
		workflow, err := ingest.NewWorkflow(ingest.WorkflowConfig{
			Control: operations, Store: store, InstallationID: installation.InstallationID, StorageGeneration: installation.StorageGeneration,
			ProcessID: processID, TempDir: filepath.Join(config.ScratchDir, "ingest-"+processID), SpoolBudget: resources.Disk,
		})
		if err != nil {
			return fail(err)
		}
		runtime.batcher, err = ingest.NewBatcher(ingest.BatcherConfig{Processor: workflow})
		if err != nil {
			return fail(err)
		}
		ingestHandler, err := api.NewIngestHandler(api.Config{
			Sink: runtime.batcher, ResolveProject: operations.LoadProjectTenant,
			ResolveAuthorization: operations.LoadProjectAuthorization, ResolveOrigins: operations.LoadProjectOrigins,
			IngressBudget: resources.Ingress, WorkingBudget: resources.Working,
		})
		if err != nil {
			_ = runtime.batcher.Drain(context.Background())
			return fail(err)
		}
		mux.Handle("/api/", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if !runtime.ready.Load() {
				writeReadinessError(writer)
				return
			}
			ingestHandler.ServeHTTP(writer, request)
		}))
	}
	if config.Roles[RoleWorker] {
		operations, err := database.PublicationOperations()
		if err != nil {
			return fail(err)
		}
		owner, err := randomUUID()
		if err != nil {
			return fail(err)
		}
		runtime.publication, runtime.workerOwner = operations, owner
		runtime.converter = &DurableConversionWorkflow{Control: operations, Store: store, Runner: ProcessConversionRunner{}, InstallationID: installation.InstallationID, ScratchDir: filepath.Join(config.ScratchDir, "worker")}
		runtime.publisher = &DurablePublicationWorkflow{Control: operations, Store: store}
	}
	if config.Roles[RoleScheduler] || config.Roles[RoleWorker] {
		operations, err := database.QueryOperations()
		if err != nil {
			return fail(err)
		}
		runtime.queryControl = operations
	}
	if config.Roles[RoleWorker] {
		runtime.queryWorker = &DurableQueryWorkflow{
			Control: runtime.queryControl, Store: store, Runner: ProcessQueryRunner{},
			InstallationID: installation.InstallationID, ScratchDir: filepath.Join(config.ScratchDir, "query-worker"),
		}
	}
	mux.HandleFunc("/livez", runtime.livez)
	mux.HandleFunc("/readyz", runtime.readyz)
	runtime.handler = mux
	runtime.ready.Store(installation.SetupState == control.SetupReady)
	return runtime, nil
}

func Run(ctx context.Context, config Config) error {
	runtime, err := NewRuntime(ctx, config)
	if err != nil {
		return err
	}
	debug.SetMemoryLimit(runtime.resources.GoMemoryLimit)
	return runtime.Run(ctx)
}

func (runtime *Runtime) Handler() http.Handler    { return runtime.handler }
func (runtime *Runtime) Started() <-chan struct{} { return runtime.started }

func (runtime *Runtime) Addr() string {
	runtime.addrMu.RLock()
	defer runtime.addrMu.RUnlock()
	return runtime.addr
}

func (runtime *Runtime) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", runtime.config.HTTPAddr)
	if err != nil {
		if runtime.batcher != nil {
			_ = runtime.batcher.Drain(context.Background())
		}
		runtime.database.Close()
		return err
	}
	runtime.addrMu.Lock()
	runtime.addr = listener.Addr().String()
	runtime.addrMu.Unlock()
	runtime.startOnce.Do(func() { close(runtime.started) })
	server := &http.Server{
		Handler: runtime.handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 35 * time.Second, MaxHeaderBytes: 32 << 10,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	workerResult := make(chan error, 1)
	workerContext, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	if runtime.publication != nil {
		go func() { workerResult <- runtime.runWorker(workerContext) }()
	}
	schedulerResult := make(chan error, 1)
	if runtime.queryControl != nil {
		go func() { schedulerResult <- runtime.runRetentionScheduler(workerContext) }()
	}

	var cause error
	select {
	case <-ctx.Done():
	case cause = <-serveResult:
		if errors.Is(cause, http.ErrServerClosed) {
			cause = nil
		}
	}
	runtime.ready.Store(false)
	stopWorker()
	drainCtx, cancel := context.WithTimeout(context.Background(), runtime.config.DrainTimeout)
	defer cancel()
	drainResult := make(chan error, 1)
	go func() {
		if runtime.batcher == nil {
			drainResult <- nil
			return
		}
		drainResult <- runtime.batcher.Drain(drainCtx)
	}()
	shutdownErr := server.Shutdown(drainCtx)
	drainErr := <-drainResult
	workingErr := runtime.resources.Working.Drain(drainCtx)
	ingressErr := runtime.resources.Ingress.Drain(drainCtx)
	diskErr := runtime.resources.Disk.Drain(drainCtx)
	select {
	case serveErr := <-serveResult:
		if cause == nil && !errors.Is(serveErr, http.ErrServerClosed) {
			cause = serveErr
		}
	default:
	}
	if runtime.publication != nil {
		select {
		case workerErr := <-workerResult:
			cause = errors.Join(cause, workerErr)
		case <-drainCtx.Done():
			cause = errors.Join(cause, drainCtx.Err())
		}
	}
	if runtime.queryControl != nil {
		select {
		case schedulerErr := <-schedulerResult:
			cause = errors.Join(cause, schedulerErr)
		case <-drainCtx.Done():
			cause = errors.Join(cause, drainCtx.Err())
		}
	}
	runtime.database.Close()
	return errors.Join(cause, shutdownErr, drainErr, workingErr, ingressErr, diskErr)
}

func (runtime *Runtime) livez(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte("{\"status\":\"alive\"}\n"))
}

func (runtime *Runtime) readyz(writer http.ResponseWriter, request *http.Request) {
	if !runtime.ready.Load() {
		writeReadinessError(writer)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := runtime.database.Ping(ctx); err != nil {
		writeReadinessError(writer)
		return
	}
	if err := runtime.store.VerifyObject(ctx, runtime.markerKey, runtime.markerBytes, runtime.markerSHA); err != nil {
		writeReadinessError(writer)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte("{\"status\":\"ready\"}\n"))
}

func writeReadinessError(writer http.ResponseWriter) {
	requestID, err := randomUUID()
	if err != nil {
		requestID = "00000000-0000-4000-8000-000000000000"
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(writer, "{\"code\":\"dependency_unavailable\",\"message\":\"dependency unavailable\",\"retryable\":true,\"request_id\":%q}\n", requestID)
}

func ensurePrivateDirectory(path string) error {
	if path == "" {
		return errors.New("scratch directory is required")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("scratch directory must be private")
	}
	return nil
}
