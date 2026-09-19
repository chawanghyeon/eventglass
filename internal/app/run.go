package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
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
	if !config.Roles[RoleAPI] {
		return nil, errors.New("the current runtime requires the api role")
	}
	if config.Roles[RoleWorker] || config.Roles[RoleScheduler] {
		return nil, errors.New("worker and scheduler roles remain unavailable until their durable loops are implemented")
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
	installation, err := database.LoadInstallation(ctx)
	if err != nil {
		return fail(fmt.Errorf("load installation: %w", err))
	}
	if installation.GlobalScrubPolicySHA != control.EmptyGlobalScrubPolicySHA {
		return fail(errors.New("global scrub policy does not match this runtime"))
	}
	identity, err := StorageIdentity(config.S3)
	if err != nil {
		return fail(err)
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
	if err := store.VerifyObject(ctx, markerKey, int64(len(marker)), markerSHA); err != nil {
		return fail(fmt.Errorf("verify installation marker: %w", err))
	}
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
	batcher, err := ingest.NewBatcher(ingest.BatcherConfig{Processor: workflow})
	if err != nil {
		return fail(err)
	}
	ingestHandler, err := api.NewIngestHandler(api.Config{
		Sink: batcher, ResolveProject: operations.LoadProjectTenant,
		ResolveAuthorization: operations.LoadProjectAuthorization, ResolveOrigins: operations.LoadProjectOrigins,
		IngressBudget: resources.Ingress, WorkingBudget: resources.Working,
	})
	if err != nil {
		_ = batcher.Drain(context.Background())
		return fail(err)
	}
	runtime := &Runtime{
		config: config, resources: resources, database: database, store: store, batcher: batcher, installation: installation,
		markerKey: markerKey, markerBytes: int64(len(marker)), markerSHA: markerSHA, started: make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.Handle("/api/", ingestHandler)
	mux.HandleFunc("/livez", runtime.livez)
	mux.HandleFunc("/readyz", runtime.readyz)
	runtime.handler = mux
	runtime.ready.Store(true)
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
		_ = runtime.batcher.Drain(context.Background())
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

	var cause error
	select {
	case <-ctx.Done():
	case cause = <-serveResult:
		if errors.Is(cause, http.ErrServerClosed) {
			cause = nil
		}
	}
	runtime.ready.Store(false)
	drainCtx, cancel := context.WithTimeout(context.Background(), runtime.config.DrainTimeout)
	defer cancel()
	drainResult := make(chan error, 1)
	go func() { drainResult <- runtime.batcher.Drain(drainCtx) }()
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
	runtime.database.Close()
	return errors.Join(cause, shutdownErr, drainErr, workingErr, ingressErr, diskErr)
}

func (runtime *Runtime) livez(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte("live\n"))
}

func (runtime *Runtime) readyz(writer http.ResponseWriter, request *http.Request) {
	if !runtime.ready.Load() {
		http.Error(writer, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := runtime.database.Ping(ctx); err != nil {
		http.Error(writer, "dependency unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := runtime.store.VerifyObject(ctx, runtime.markerKey, runtime.markerBytes, runtime.markerSHA); err != nil {
		http.Error(writer, "dependency unavailable", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte("ready\n"))
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
