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
type Option interface {
	apply(*Server)
}

type optionFunc func(*Server)

func (f optionFunc) apply(s *Server) { f(s) }

// TaskDef configures a named task. Create with Task, pass to New.
type TaskDef struct {
	name        string
	fn          TaskFunc
	description string
	example     map[string]any
	keepResults int // per-task result history limit; 0 = use server default
}

// Task creates a named task definition. Pass the result to New.
func Task(name string, fn TaskFunc) *TaskDef {
	return &TaskDef{name: name, fn: fn}
}

// Description sets a human-readable description for the task.
func (td *TaskDef) Description(desc string) *TaskDef {
	td.description = desc
	return td
}

// KeepResults sets the number of recent results to retain for this task.
// Overrides the server-wide default set by WithResultHistory.
func (td *TaskDef) KeepResults(n int) *TaskDef {
	td.keepResults = n
	return td
}

// Example sets example parameters for the task. The dashboard uses this
// to pre-fill the Params textarea and show a Params button.
// The map is copied immediately; later mutations to the original are safe.
func (td *TaskDef) Example(params map[string]any) *TaskDef {
	cp := make(map[string]any, len(params))
	for k, v := range params {
		cp[k] = v
	}
	td.example = cp
	return td
}

func (td *TaskDef) apply(s *Server) {
	s.tasks[td.name] = td.fn
	if td.description != "" {
		s.taskDescriptions[td.name] = td.description
	} else {
		delete(s.taskDescriptions, td.name)
	}
	if td.example != nil {
		cp := make(map[string]any, len(td.example))
		for k, v := range td.example {
			cp[k] = v
		}
		s.taskExamples[td.name] = cp
	} else {
		delete(s.taskExamples, td.name)
	}
	if td.keepResults > 0 {
		if s.taskResultSizes == nil {
			s.taskResultSizes = make(map[string]int)
		}
		s.taskResultSizes[td.name] = td.keepResults
	}
}

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

type customRoute struct {
	pattern string
	handler http.Handler
}

// Server hosts one or more named tasks that an orchestrator can trigger via HTTP.
// Configure it with New and start it with ListenAndServe.
type Server struct {
	name string

	// Task registry
	tasks            map[string]TaskFunc
	taskDescriptions map[string]string
	taskExamples     map[string]map[string]any
	slots            map[string]*taskSlot

	// Custom HTTP handlers
	customHandlers   []customRoute
	dashboardEnabled bool

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

	// Result history
	resultHistorySize int            // server-wide default (0 = use defaultResultHistory)
	taskResultSizes   map[string]int // per-task overrides

	// Server (set during ListenAndServe)
	logger     *slog.Logger
	tokenBytes []byte // pre-computed for constant-time auth checks
	server     *http.Server
	events     *eventBroker
	history    *resultHistory
	deliverer  *deliverer
}

// New creates a server. At least one Task is required.
func New(name string, opts ...Option) *Server {
	f := &Server{
		name:             name,
		tasks:            make(map[string]TaskFunc),
		taskDescriptions: make(map[string]string),
		taskExamples:     make(map[string]map[string]any),
		authToken:        os.Getenv("FUNTASK_AUTH_TOKEN"),
		deadLetterDir:    os.Getenv("FUNTASK_DEAD_LETTER_DIR"),
		syncTimeout:      2 * time.Minute,
		shutdownTimeout:  30 * time.Second,
		callbackRetries:  5,
		callbackTimeout:  30 * time.Second,
		stopCh:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt.apply(f)
	}
	return f
}

// WithAuthToken sets the bearer token for all endpoints.
func WithAuthToken(token string) Option {
	return optionFunc(func(f *Server) {
		f.authToken = token
	})
}

// WithDeadLetterDir sets the dead letter directory path. Required.
func WithDeadLetterDir(path string) Option {
	return optionFunc(func(f *Server) {
		f.deadLetterDir = path
	})
}

// WithMaxDuration sets the maximum job duration. Default: no limit.
func WithMaxDuration(d time.Duration) Option {
	return optionFunc(func(f *Server) {
		f.maxDuration = d
	})
}

// WithSyncTimeout sets the sync-mode timeout. Default: 2m.
func WithSyncTimeout(d time.Duration) Option {
	return optionFunc(func(f *Server) {
		f.syncTimeout = d
	})
}

// WithShutdownTimeout sets the graceful shutdown timeout. Default: 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return optionFunc(func(f *Server) {
		f.shutdownTimeout = d
	})
}

// WithCallbackRetries sets the number of callback delivery attempts.
// Default: 5.
func WithCallbackRetries(n int) Option {
	return optionFunc(func(f *Server) {
		f.callbackRetries = n
	})
}

// WithCallbackTimeout sets the per-attempt callback HTTP timeout.
// Default: 30s.
func WithCallbackTimeout(d time.Duration) Option {
	return optionFunc(func(f *Server) {
		f.callbackTimeout = d
	})
}

// WithCallbackAllowlist sets allowed callback URL origins.
func WithCallbackAllowlist(origins ...string) Option {
	return optionFunc(func(f *Server) {
		f.callbackAllowlist = origins
	})
}

// WithReadiness sets a custom readiness check for /readyz.
func WithReadiness(fn func() error) Option {
	return optionFunc(func(f *Server) {
		f.readiness = fn
	})
}

// WithHandler registers a custom HTTP handler on the server's mux.
// The pattern follows net/http.ServeMux syntax (e.g. "GET /api/orders").
// Custom handlers are not protected by the server's bearer-token auth;
// apply your own middleware as needed. Patterns must not conflict with
// built-in routes (/run, /stop, /result, /health, /livez, /readyz).
func WithHandler(pattern string, handler http.Handler) Option {
	return optionFunc(func(f *Server) {
		f.customHandlers = append(f.customHandlers, customRoute{pattern, handler})
	})
}

// WithResultHistory sets the server-wide default for how many recent
// results to keep per task. Default: 10. Individual tasks can override
// this with TaskDef.KeepResults.
func WithResultHistory(n int) Option {
	return optionFunc(func(f *Server) {
		f.resultHistorySize = n
	})
}

// WithDashboard enables the built-in developer dashboard at /dashboard.
// The dashboard shows task status, progress, errors, and allows triggering
// tasks. It uses Server-Sent Events via /events for live updates and
// handles authentication client-side by prompting for the bearer token.
// The /dashboard path becomes reserved when enabled and cannot be used
// by WithHandler.
func WithDashboard() Option {
	return optionFunc(func(f *Server) {
		f.dashboardEnabled = true
	})
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
	f.tokenBytes = []byte(f.authToken)
	f.slots = make(map[string]*taskSlot, len(f.tasks))
	for name := range f.tasks {
		f.slots[name] = &taskSlot{}
	}
	f.events = &eventBroker{clients: make(map[chan struct{}]struct{})}
	f.history = newResultHistory(f.slots, f.resultHistorySize, f.taskResultSizes)
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
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
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
	for _, r := range f.customHandlers {
		if r.pattern == "" {
			errs = append(errs, "custom handler has empty pattern")
		} else if conflict := reservedRoute(routePath(r.pattern)); conflict != "" {
			errs = append(errs, fmt.Sprintf("custom handler %q conflicts with reserved route %s", r.pattern, conflict))
		} else if f.dashboardEnabled && routePath(r.pattern) == "/dashboard" {
			errs = append(errs, fmt.Sprintf("custom handler %q conflicts with built-in dashboard", r.pattern))
		}
		if r.handler == nil {
			errs = append(errs, fmt.Sprintf("custom handler %q has nil handler", r.pattern))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("server %q: %s", f.name, strings.Join(errs, "; "))
}

// reservedRoute returns the reserved path that conflicts with path,
// or an empty string if no conflict exists.
func reservedRoute(path string) string {
	for _, r := range []string{"/health", "/livez", "/readyz", "/events"} {
		if path == r {
			return r
		}
	}
	for _, r := range []string{"/run/", "/stop/", "/result/"} {
		if strings.HasPrefix(path, r) {
			return r
		}
	}
	return ""
}

// routePath extracts the path component from a ServeMux pattern,
// stripping any method prefix or host.
func routePath(pattern string) string {
	if _, after, ok := strings.Cut(pattern, " "); ok {
		return strings.TrimSpace(after)
	}
	if strings.HasPrefix(pattern, "/") {
		return pattern
	}
	if i := strings.Index(pattern, "/"); i >= 0 {
		return pattern[i:]
	}
	return pattern
}
