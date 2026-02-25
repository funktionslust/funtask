package funtask

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sync"
	"time"
)

const (
	maxResultSize        = 10 << 20 // 10 MB
	defaultResultHistory = 10
)

type errorResponse struct {
	Error string `json:"error"`
}

type runRequest struct {
	JobID       string         `json:"jobId"`
	CallbackURL string         `json:"callbackUrl"`
	Params      map[string]any `json:"params"`
}

type jobResponse struct {
	JobID    string         `json:"jobId"`
	Success  bool           `json:"success"`
	Message  string         `json:"message,omitempty"`
	Duration string         `json:"duration"`
	Data     map[string]any `json:"data,omitempty"`
	Error    *jobError      `json:"error,omitempty"`
}

type jobError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Step    string `json:"step,omitempty"`
}

type busyResponse struct {
	Error      string     `json:"error"`
	Task       string     `json:"task"`
	CurrentJob currentJob `json:"currentJob"`
}

type currentJob struct {
	JobID   string `json:"jobId"`
	Running string `json:"running"`
}

type stopResponse struct {
	Task    string `json:"task"`
	JobID   string `json:"jobId,omitempty"`
	Message string `json:"message"`
}

type runningJobStatus struct {
	JobID       string `json:"jobId"`
	Status      string `json:"status"`
	Running     string `json:"running"`
	CurrentStep string `json:"currentStep,omitempty"`
}

type resultEntry struct {
	jobID      string
	response   jobResponse
	finishedAt time.Time
}

type taskResults struct {
	entries []resultEntry // newest at end
	limit   int
}

type resultHistory struct {
	mu    sync.RWMutex
	tasks map[string]*taskResults
}

func newResultHistory(slots map[string]*taskSlot, serverDefault int, perTask map[string]int) *resultHistory {
	if serverDefault <= 0 {
		serverDefault = defaultResultHistory
	}
	tasks := make(map[string]*taskResults, len(slots))
	for name := range slots {
		limit := serverDefault
		if n, ok := perTask[name]; ok && n > 0 {
			limit = n
		}
		tasks[name] = &taskResults{limit: limit}
	}
	return &resultHistory{tasks: tasks}
}

// store saves a job response in the task's result history. If the
// serialized response exceeds the maximum result size, it is replaced
// with a result_too_large error. The (possibly replaced) response is
// returned for use by the caller.
func (h *resultHistory) store(task, jobID string, resp jobResponse) jobResponse {
	data, err := json.Marshal(resp)
	if err != nil {
		resp = jobResponse{
			JobID:    resp.JobID,
			Success:  false,
			Duration: resp.Duration,
			Error: &jobError{
				Code:    "result_serialization_failed",
				Message: "result could not be serialized",
			},
		}
	} else if len(data) > maxResultSize {
		resp = jobResponse{
			JobID:    resp.JobID,
			Success:  false,
			Duration: resp.Duration,
			Error: &jobError{
				Code:    "result_too_large",
				Message: fmt.Sprintf("result exceeds maximum size (%d MB)", maxResultSize>>20),
			},
		}
	}

	h.mu.Lock()
	tr, ok := h.tasks[task]
	if !ok {
		tr = &taskResults{limit: defaultResultHistory}
		h.tasks[task] = tr
	}
	tr.entries = append(tr.entries, resultEntry{jobID: jobID, response: resp, finishedAt: time.Now()})
	if len(tr.entries) > tr.limit {
		tr.entries = tr.entries[len(tr.entries)-tr.limit:]
	}
	h.mu.Unlock()
	return resp
}

// lookup returns a stored result for a specific task and jobID.
func (h *resultHistory) lookup(task, jobID string) (jobResponse, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	tr, ok := h.tasks[task]
	if !ok {
		return jobResponse{}, false
	}
	for i := len(tr.entries) - 1; i >= 0; i-- {
		if tr.entries[i].jobID == jobID {
			return tr.entries[i].response, true
		}
	}
	return jobResponse{}, false
}

// lookupByJobID searches across all tasks for a result matching the
// given jobID. First match wins (searches newest entries first).
func (h *resultHistory) lookupByJobID(jobID string) (jobResponse, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, tr := range h.tasks {
		for i := len(tr.entries) - 1; i >= 0; i-- {
			if tr.entries[i].jobID == jobID {
				return tr.entries[i].response, true
			}
		}
	}
	return jobResponse{}, false
}

// list returns a copy of stored results for a task, newest first.
func (h *resultHistory) list(task string) []resultEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	tr, ok := h.tasks[task]
	if !ok {
		return nil
	}
	n := len(tr.entries)
	out := make([]resultEntry, n)
	for i, e := range tr.entries {
		out[n-1-i] = e
	}
	return out
}

// healthResults returns stored results for a task as healthResult values,
// newest first. Returns nil for unknown tasks or tasks with no results.
func (h *resultHistory) healthResults(task string) []healthResult {
	h.mu.RLock()
	defer h.mu.RUnlock()
	tr, ok := h.tasks[task]
	if !ok || len(tr.entries) == 0 {
		return nil
	}
	n := len(tr.entries)
	results := make([]healthResult, n)
	for i, e := range tr.entries {
		hr := healthResult{
			JobID:    e.jobID,
			Success:  e.response.Success,
			Duration: e.response.Duration,
			Finished: e.finishedAt.Format(time.RFC3339),
		}
		if e.response.Success {
			hr.Message = e.response.Message
		} else {
			hr.Error = e.response.Error
			if e.response.Error != nil {
				hr.Message = e.response.Error.Message
			}
		}
		results[n-1-i] = hr
	}
	return results
}

// buildJobResponse creates a jobResponse from a task Result.
func buildJobResponse(jobID string, result Result, duration string) jobResponse {
	resp := jobResponse{
		JobID:    jobID,
		Success:  result.Success,
		Duration: duration,
	}
	if result.Success {
		resp.Message = result.Message
		resp.Data = result.Data
	} else {
		resp.Error = &jobError{
			Code:    result.ErrorCode,
			Message: result.Message,
			Step:    result.LastStep,
		}
	}
	return resp
}

// writeJSON sets the Content-Type to application/json, writes the HTTP
// status code, and JSON-encodes v into the response body.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// routes builds the HTTP handler with all registered routes.
func (f *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// Unauthenticated probes
	mux.HandleFunc("GET /livez", f.handleLivez)
	mux.HandleFunc("GET /readyz", f.handleReadyz)

	// Authenticated endpoints
	authed := func(handler http.HandlerFunc) http.Handler {
		return requireToken(f.tokenBytes, handler)
	}
	mux.Handle("POST /run/{task}", authed(f.handleRun))
	mux.Handle("POST /stop/{task}", authed(f.handleStop))
	mux.Handle("GET /result/{jobId}", authed(f.handleResult))
	mux.Handle("GET /health", authed(f.handleHealth))
	mux.HandleFunc("GET /events", f.handleEvents)

	// Dashboard HTML is served without bearer-token auth. The page handles
	// authentication client-side by prompting for the token and calling
	// /health (which requires auth) before showing any server data.
	if f.dashboardEnabled {
		mux.HandleFunc("GET /dashboard", serveDashboard)
	}

	for _, r := range f.customHandlers {
		mux.Handle(r.pattern, r.handler)
	}

	return mux
}

func (f *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if f.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{
			Error: "Server is shutting down",
		})
		return
	}

	taskName := r.PathValue("task")
	taskFunc, ok := f.tasks[taskName]
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: fmt.Sprintf("task %q not found", taskName),
		})
		return
	}

	var req runRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
			return
		}
	}

	if req.CallbackURL != "" {
		if len(f.callbackAllowlist) == 0 {
			f.logger.Debug("callback URL rejected: no allowlist configured", "task", taskName, "callbackUrl", req.CallbackURL)
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "callback URLs not allowed (no allowlist configured)"})
			return
		}
		if err := validateCallbackURL(req.CallbackURL, f.callbackAllowlist); err != nil {
			f.logger.Debug("callback URL rejected: not in allowlist", "task", taskName, "callbackUrl", req.CallbackURL)
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
			return
		}
	}

	// --- Shared path: jobID generation and duplicate detection ---

	jobID := req.JobID
	if jobID == "" {
		jobID = generateJobID()
	}

	log := f.logger.With("task", taskName, "jobId", jobID)

	// Check result cache for completed duplicate.
	if cached, ok := f.history.lookup(taskName, jobID); ok {
		log.Debug("duplicate detection: returning cached result")
		writeJSON(w, http.StatusOK, cached)
		return
	}

	slot := f.slots[taskName]
	slot.mu.Lock()
	if slot.running {
		// Duplicate detection: same jobId still running.
		if slot.jobID == jobID {
			status := runningJobStatus{
				JobID:       slot.jobID,
				Status:      "running",
				Running:     time.Since(slot.startedAt).Round(time.Second).String(),
				CurrentStep: slot.run.currentStep(),
			}
			slot.mu.Unlock() // unlock before I/O (writeJSON)
			log.Debug("duplicate detection: job still running")
			writeJSON(w, http.StatusOK, status)
			return
		}
		resp := busyResponse{
			Error: "Task is busy with a different job",
			Task:  taskName,
			CurrentJob: currentJob{
				JobID:   slot.jobID,
				Running: time.Since(slot.startedAt).Round(time.Second).String(),
			},
		}
		slot.mu.Unlock() // unlock before I/O (writeJSON)
		writeJSON(w, http.StatusConflict, resp)
		return
	}

	// --- Async path ---

	if req.CallbackURL != "" {
		var jobCtx context.Context
		var jobCancel context.CancelFunc
		if f.maxDuration > 0 {
			jobCtx, jobCancel = context.WithTimeout(context.Background(), f.maxDuration)
		} else {
			jobCtx, jobCancel = context.WithCancel(context.Background())
		}

		run := &Run{Context: jobCtx, onChange: f.events.notify}
		startedAt := time.Now()

		slot.running = true
		slot.jobID = jobID
		slot.startedAt = startedAt
		slot.cancel = jobCancel
		slot.run = run
		slot.callbackURL = req.CallbackURL
		f.wg.Add(1)
		slot.mu.Unlock()
		f.events.notify()

		log.Info("async job started")

		callbackURL := req.CallbackURL
		params := Params{m: req.Params}
		if params.m == nil {
			params.m = make(map[string]any)
		}

		go func() {
			defer f.wg.Done()
			defer jobCancel()

			result := safeRunTask(taskFunc, run, params, log)

			// Check for max duration exceeded.
			if f.maxDuration > 0 && jobCtx.Err() == context.DeadlineExceeded {
				result = Fail("max_duration_exceeded", "job exceeded maximum duration (%s)", f.maxDuration)
				result.LastStep = run.currentStep()
				log.Warn("max duration exceeded", "maxDuration", f.maxDuration)
			}

			// Mark task function as done before delivery begins.
			slot.mu.Lock()
			slot.taskDone = true
			slot.mu.Unlock()

			duration := time.Since(startedAt).Round(time.Millisecond).String()
			resp := buildJobResponse(jobID, result, duration)
			data, resp := marshalResult(resp)

			// Enforce result size limit before delivery.
			if len(data) > maxResultSize {
				resp = jobResponse{
					JobID:    resp.JobID,
					Success:  false,
					Duration: resp.Duration,
					Error: &jobError{
						Code:    "result_too_large",
						Message: fmt.Sprintf("result exceeds maximum size (%d MB)", maxResultSize>>20),
					},
				}
				data, resp = marshalResult(resp)
			}

			// Write dead letter BEFORE callback attempt — ensures recoverability if delivery fails.
			if err := f.deliverer.writeDeadLetter(jobID, data); err != nil {
				log.Error("dead letter write failed", "error", err)
				// Do NOT attempt callback — result safety net is missing.
			} else if f.draining.Load() {
				// Single attempt during shutdown — full retry schedule exceeds shutdown timeout.
				if err := f.deliverer.deliverOnce(jobID, callbackURL, data); err != nil {
					log.Warn("shutdown callback delivery failed", "error", err)
				}
			} else {
				if err := f.deliverer.deliver(jobID, callbackURL, data); err != nil {
					log.Warn("async callback delivery failed", "error", err)
				}
			}

			f.history.store(taskName, jobID, resp)

			slot.release(jobID, resp.Success, duration)
			f.events.notify()

			attrs := []any{"success", result.Success, "duration", duration}
			if result.cause != nil {
				attrs = append(attrs, "error", result.cause)
			}
			log.Info("async job completed", attrs...)
		}()

		writeJSON(w, http.StatusAccepted, struct {
			JobID string `json:"jobId"`
		}{JobID: jobID})
		return
	}

	// --- Sync path ---

	var jobCtx context.Context
	var jobCancel context.CancelFunc
	if f.maxDuration > 0 {
		jobCtx, jobCancel = context.WithTimeout(r.Context(), f.maxDuration)
	} else {
		jobCtx, jobCancel = context.WithCancel(r.Context())
	}
	defer jobCancel() // safety net — explicit calls below are the primary cancel points

	run := &Run{Context: jobCtx, onChange: f.events.notify}
	startedAt := time.Now()

	slot.running = true
	slot.jobID = jobID
	slot.startedAt = startedAt
	slot.cancel = jobCancel
	slot.run = run
	f.wg.Add(1)
	syncOwnsWG := true
	defer func() {
		if syncOwnsWG {
			f.wg.Done()
		}
	}()
	slot.mu.Unlock() // unlock after slot acquisition — must not hold during task execution
	f.events.notify()

	log.Info("job started")

	params := Params{m: req.Params}
	if params.m == nil {
		params.m = make(map[string]any)
	}

	// Run task in a goroutine for panic recovery and timeout select.
	done := make(chan Result, 1)
	go func() {
		done <- safeRunTask(taskFunc, run, params, log)
	}()

	syncTimer := time.NewTimer(f.syncTimeout)
	defer syncTimer.Stop()

	var result Result
	select {
	case result = <-done:
		// Task completed before sync timeout.
	case <-syncTimer.C:
		jobCancel()
		log.Warn("sync timeout exceeded", "timeout", f.syncTimeout)

		// Transfer WaitGroup ownership to the background goroutine.
		syncOwnsWG = false

		// Background goroutine waits for task to finish, caches result, then releases slot.
		go func() {
			defer f.wg.Done()
			timedOutResult := <-done

			// Mark task function as done before slot release.
			slot.mu.Lock()
			slot.taskDone = true
			slot.mu.Unlock()

			d := time.Since(startedAt).Round(time.Millisecond).String()
			resp := buildJobResponse(jobID, timedOutResult, d)
			resp = f.history.store(taskName, jobID, resp)

			slot.release(jobID, resp.Success, d)
			f.events.notify()
			log.Info("job released after sync timeout")
		}()

		writeJSON(w, http.StatusGatewayTimeout, errorResponse{
			Error: fmt.Sprintf("Sync mode timeout exceeded (%s). Use callbackUrl for long-running tasks.", f.syncTimeout),
		})
		return
	}

	// Check for max duration exceeded (only on normal completion path).
	if f.maxDuration > 0 && jobCtx.Err() == context.DeadlineExceeded {
		result = Fail("max_duration_exceeded", "job exceeded maximum duration (%s)", f.maxDuration)
		result.LastStep = run.currentStep()
		log.Warn("max duration exceeded", "maxDuration", f.maxDuration)
	}

	jobCancel()

	duration := time.Since(startedAt).Round(time.Millisecond).String()
	resp := buildJobResponse(jobID, result, duration)
	resp = f.history.store(taskName, jobID, resp)

	slot.release(jobID, resp.Success, duration)
	f.events.notify()

	writeJSON(w, http.StatusOK, resp)
	attrs := []any{"success", result.Success, "duration", duration}
	if result.cause != nil {
		attrs = append(attrs, "error", result.cause)
	}
	log.Info("job completed", attrs...)
}

// safeRunTask executes fn with panic recovery. If the task panics, the
// panic value is logged and converted to a Fail result with code "panic".
// The last step is always propagated to the result for diagnostic context.
func safeRunTask(fn TaskFunc, run *Run, params Params, log *slog.Logger) (result Result) {
	defer func() {
		if v := recover(); v != nil {
			stack := debug.Stack()
			log.Error("panic recovered in task", "panic", v, "stack", string(stack))
			result = Fail("panic", "%v", v)
			result.LastStep = run.currentStep()
		}
	}()
	result = fn(run, params)
	if result.LastStep == "" && run.currentStep() != "" {
		result.LastStep = run.currentStep()
	}
	return result
}

// release records last-job stats and resets the slot for the next job.
// The caller must NOT hold slot.mu.
func (s *taskSlot) release(jobID string, success bool, duration string) {
	s.mu.Lock()
	s.lastJobID = jobID
	s.lastSuccess = success
	s.lastCompletedAt = time.Now()
	s.lastDuration = duration
	s.running = false
	s.jobID = ""
	s.startedAt = time.Time{}
	s.cancel = nil
	s.run = nil
	s.callbackURL = ""
	s.taskDone = false
	s.mu.Unlock()
}

func generateJobID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (f *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	taskName := r.PathValue("task")
	if _, ok := f.tasks[taskName]; !ok {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: fmt.Sprintf("task %q not found", taskName),
		})
		return
	}

	slot := f.slots[taskName]
	slot.mu.Lock()
	if !slot.running {
		slot.mu.Unlock() // unlock before I/O (writeJSON)
		writeJSON(w, http.StatusOK, stopResponse{
			Task:    taskName,
			Message: "No running job",
		})
		return
	}
	jobID := slot.jobID
	cancelFn := slot.cancel
	slot.mu.Unlock() // unlock before calling cancelFn and writeJSON

	cancelFn()

	f.logger.With("task", taskName, "jobId", jobID).Debug("cancellation requested")

	writeJSON(w, http.StatusOK, stopResponse{
		Task:    taskName,
		JobID:   jobID,
		Message: "Cancellation requested",
	})
}

func (f *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobId")
	if cached, ok := f.history.lookupByJobID(jobID); ok {
		writeJSON(w, http.StatusOK, cached)
		return
	}
	writeJSON(w, http.StatusNotFound, errorResponse{Error: "result not found"})
}
