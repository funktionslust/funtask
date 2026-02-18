package funtask

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// TaskFunc is the function signature for task implementations. The
// function receives a Run context for cancellation and progress
// reporting, and Params for typed parameter access. It must respect
// ctx cancellation — when ctx is cancelled (via /stop or shutdown),
// the function should return promptly.
type TaskFunc func(ctx *Run, params Params) Result

// Option configures a Server. Pass options to New.
type Option func(*Server)

type taskSlot struct {
	mu          sync.Mutex
	running     bool
	jobID       string
	startedAt   time.Time
	cancel      context.CancelFunc
	run         *Run
	callbackURL string
	taskDone    bool

	// Last-job tracking for health endpoint.
	lastJobID       string
	lastSuccess     bool
	lastCompletedAt time.Time
	lastDuration    string
}

// Server hosts one or more named tasks that an orchestrator can trigger via HTTP.
// Configure it with New and start it with ListenAndServe.
type Server struct {
	name string

	// Task registry
	tasks map[string]TaskFunc
	slots map[string]*taskSlot

	// Security
	authToken         string
	callbackAllowlist []string

	// Directories
	deadLetterDir string

	// Timeouts
	maxDuration     time.Duration
	syncTimeout     time.Duration
	shutdownTimeout time.Duration

	// Callback config
	callbackRetries int
	callbackTimeout time.Duration

	// Health
	readiness func() error
	startedAt time.Time
	draining  atomic.Bool

	// Shutdown coordination
	wg     sync.WaitGroup
	stopCh chan struct{}

	// Server (set during ListenAndServe)
	logger    *slog.Logger
	server    *http.Server
	cache     *resultCache
	deliverer *deliverer
}

// New creates a server. At least one WithTask is required.
func New(name string, opts ...Option) *Server {
	f := &Server{
		name:            name,
		tasks:           make(map[string]TaskFunc),
		authToken:       os.Getenv("FUNTASK_AUTH_TOKEN"),
		deadLetterDir:   os.Getenv("FUNTASK_DEAD_LETTER_DIR"),
		syncTimeout:     2 * time.Minute,
		shutdownTimeout: 30 * time.Second,
		callbackRetries: 5,
		callbackTimeout: 30 * time.Second,
		stopCh:          make(chan struct{}),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// WithTask registers a named task.
func WithTask(name string, fn TaskFunc) Option {
	return func(f *Server) {
		f.tasks[name] = fn
	}
}

// WithAuthToken sets the bearer token for all endpoints.
func WithAuthToken(token string) Option {
	return func(f *Server) {
		f.authToken = token
	}
}

// WithDeadLetterDir sets the dead letter directory path. Required.
func WithDeadLetterDir(path string) Option {
	return func(f *Server) {
		f.deadLetterDir = path
	}
}

// WithMaxDuration sets the maximum job duration. Default: no limit.
func WithMaxDuration(d time.Duration) Option {
	return func(f *Server) {
		f.maxDuration = d
	}
}

// WithSyncTimeout sets the sync-mode timeout. Default: 2m.
func WithSyncTimeout(d time.Duration) Option {
	return func(f *Server) {
		f.syncTimeout = d
	}
}

// WithShutdownTimeout sets the graceful shutdown timeout. Default: 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(f *Server) {
		f.shutdownTimeout = d
	}
}

// WithCallbackRetries sets the number of callback delivery attempts.
// Default: 5.
func WithCallbackRetries(n int) Option {
	return func(f *Server) {
		f.callbackRetries = n
	}
}

// WithCallbackTimeout sets the per-attempt callback HTTP timeout.
// Default: 30s.
func WithCallbackTimeout(d time.Duration) Option {
	return func(f *Server) {
		f.callbackTimeout = d
	}
}

// WithCallbackAllowlist sets allowed callback URL origins.
func WithCallbackAllowlist(origins ...string) Option {
	return func(f *Server) {
		f.callbackAllowlist = origins
	}
}

// WithReadiness sets a custom readiness check for /readyz.
func WithReadiness(fn func() error) Option {
	return func(f *Server) {
		f.readiness = fn
	}
}

// ListenAndServe validates configuration, starts the HTTP server, and
// blocks until SIGTERM is received or the server fails. On SIGTERM the
// server enters draining state, cancels in-flight jobs, and shuts down
// the HTTP server gracefully.
func (f *Server) ListenAndServe(addr string) error {
	if err := f.validate(); err != nil {
		return err
	}
	f.logger = slog.With("server", f.name)
	f.slots = make(map[string]*taskSlot, len(f.tasks))
	for name := range f.tasks {
		f.slots[name] = &taskSlot{}
	}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.startedAt = time.Now()
	f.logger.Info("starting server", "addr", addr, "tasks", len(f.tasks))
	f.server = &http.Server{
		Addr:    addr,
		Handler: f.routes(),
	}

	errCh := make(chan error, 1)
	go func() {
		if err := f.server.ListenAndServe(); err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
	case <-f.stopCh:
	}

	return f.shutdown()
}

func (f *Server) shutdown() error {
	f.draining.Store(true)
	f.logger.Info("shutdown initiated")

	f.cancelAllJobs()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), f.shutdownTimeout)
	defer cancel()
	if err := f.server.Shutdown(shutdownCtx); err != nil {
		f.logger.Error("server shutdown error", "error", err)
	}

	// Wait for in-flight jobs to finish delivery.
	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		f.logger.Info("all jobs completed")
	case <-shutdownCtx.Done():
		f.logger.Warn("shutdown timeout reached, sending worker_shutdown for stuck jobs")
		f.handleStuckJobs()
	}

	f.logger.Info("shutdown complete")
	return nil
}

func (f *Server) handleStuckJobs() {
	for taskName, slot := range f.slots {
		slot.mu.Lock()
		if !slot.running || slot.taskDone {
			slot.mu.Unlock()
			continue
		}
		jobID := slot.jobID
		callbackURL := slot.callbackURL
		startedAt := slot.startedAt
		slot.mu.Unlock() // unlock before I/O

		log := f.logger.With("task", taskName, "jobId", jobID)

		duration := time.Since(startedAt).Round(time.Millisecond).String()
		result := Fail("worker_shutdown", "server is shutting down")
		resp := buildJobResponse(jobID, result, duration)
		data, _ := marshalResult(resp)

		if err := f.deliverer.writeDeadLetter(jobID, data); err != nil {
			log.Error("dead letter write failed for stuck job", "error", err)
			continue
		}

		if callbackURL != "" {
			if err := f.deliverer.deliverOnce(jobID, callbackURL, data); err != nil {
				log.Warn("shutdown callback failed for stuck job", "error", err)
			}
		}

		log.Warn("worker_shutdown sent for stuck job", "duration", duration)
	}
}

func (f *Server) cancelAllJobs() {
	for name, slot := range f.slots {
		slot.mu.Lock()
		if slot.running && slot.cancel != nil {
			cancelFn := slot.cancel
			jobID := slot.jobID
			slot.mu.Unlock() // unlock before calling cancelFn
			cancelFn()
			f.logger.Debug("cancelled job for shutdown", "task", name, "jobId", jobID)
		} else {
			slot.mu.Unlock() // unlock — no running job on this slot
		}
	}
}

func (f *Server) validate() error {
	var errs []string
	if len(f.tasks) == 0 {
		errs = append(errs, "no tasks registered")
	}
	if f.authToken == "" {
		errs = append(errs, "auth token required")
	}
	if f.deadLetterDir == "" {
		errs = append(errs, "dead letter directory required")
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("server %q: %s", f.name, strings.Join(errs, "; "))
}
