package funtask

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sync"
	"time"
)

const (
	resultCacheTTL = 5 * time.Minute
	maxResultSize  = 10 << 20 // 10 MB
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

type resultCacheEntry struct {
	taskName  string
	jobID     string
	response  jobResponse
	expiresAt time.Time
}

type resultCache struct {
	mu      sync.RWMutex
	entries map[string]*resultCacheEntry
}

func cacheKey(task, jobID string) string {
	return task + "\x00" + jobID
}

// store caches a job response. If the serialized response exceeds the
// maximum result size, it is replaced with a result_too_large error.
// Expired entries are evicted on every store call. The (possibly
// replaced) response is returned for use by the caller.
func (c *resultCache) store(task, jobID string, resp jobResponse) jobResponse {
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

	now := time.Now()
	c.mu.Lock()
	for k, e := range c.entries {
		if now.After(e.expiresAt) {
			delete(c.entries, k)
		}
	}
	c.entries[cacheKey(task, jobID)] = &resultCacheEntry{
		taskName:  task,
		jobID:     jobID,
		response:  resp,
		expiresAt: now.Add(resultCacheTTL),
	}
	c.mu.Unlock()
	return resp
}

// lookup returns a cached result for a specific task and jobID.
func (c *resultCache) lookup(task, jobID string) (jobResponse, bool) {
	key := cacheKey(task, jobID)
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return jobResponse{}, false
	}
	if time.Now().After(e.expiresAt) {
		// Re-check under write lock to avoid deleting a concurrently-replaced fresh entry.
		c.mu.Lock()
		if cur, ok := c.entries[key]; ok && time.Now().After(cur.expiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return jobResponse{}, false
	}
	return e.response, true
}

// lookupByJobID searches across all tasks for a cached result
// matching the given jobID. First non-expired match wins.
// Expired entries are skipped but not deleted — store() handles eviction.
func (c *resultCache) lookupByJobID(jobID string) (jobResponse, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	for _, e := range c.entries {
		if now.After(e.expiresAt) {
			continue
		}
		if e.jobID == jobID {
			return e.response, true
		}
	}
	return jobResponse{}, false
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
		return requireToken(f.authToken, handler)
	}
	mux.Handle("POST /run/{task}", authed(f.handleRun))
	mux.Handle("POST /stop/{task}", authed(f.handleStop))
	mux.Handle("GET /result/{jobId}", authed(f.handleResult))
	mux.Handle("GET /health", authed(f.handleHealth))

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
	if cached, ok := f.cache.lookup(taskName, jobID); ok {
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

		run := &Run{Context: jobCtx}
		startedAt := time.Now()

		slot.running = true
		slot.jobID = jobID
		slot.startedAt = startedAt
		slot.cancel = jobCancel
		slot.run = run
		slot.callbackURL = req.CallbackURL
		f.wg.Add(1)
		slot.mu.Unlock()

		log.Info("async job started")

		callbackURL := req.CallbackURL
		params := Params{m: req.Params}
		if params.m == nil {
			params.m = make(map[string]any)
		}

		go func() {
			defer f.wg.Done()
			defer jobCancel()

			// Run task with panic recovery.
			var result Result
			func() {
				defer func() {
					if v := recover(); v != nil {
						stack := debug.Stack()
						log.Error("panic recovered in task", "panic", v, "stack", string(stack))
						result = Fail("panic", "%v", v)
						result.LastStep = run.currentStep()
					}
				}()
				result = taskFunc(run, params)
				if result.LastStep == "" && run.currentStep() != "" {
					result.LastStep = run.currentStep()
				}
			}()

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

			f.cache.store(taskName, jobID, resp)

			slot.release(jobID, resp.Success, duration)

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

	run := &Run{Context: jobCtx}
	startedAt := time.Now()

	slot.running = true
	slot.jobID = jobID
	slot.startedAt = startedAt
	slot.cancel = jobCancel
	slot.run = run
	f.wg.Add(1)
	slot.mu.Unlock() // unlock after slot acquisition — must not hold during task execution

	log.Info("job started")

	params := Params{m: req.Params}
	if params.m == nil {
		params.m = make(map[string]any)
	}

	// Run task in a goroutine for panic recovery and timeout select.
	done := make(chan Result, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				stack := debug.Stack()
				log.Error("panic recovered in task", "panic", v, "stack", string(stack))
				res := Fail("panic", "%v", v)
				res.LastStep = run.currentStep()
				done <- res
			}
		}()
		result := taskFunc(run, params)
		if result.LastStep == "" && run.currentStep() != "" {
			result.LastStep = run.currentStep()
		}
		done <- result
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

		// Background goroutine waits for task to finish, caches result, then releases slot.
		go func() {
			defer f.wg.Done()
			timedOutResult := <-done

			// Mark task function as done before slot release.
			slot.mu.Lock()
			slot.taskDone = true
			slot.mu.Unlock()

			d := time.Since(startedAt).Round(time.Millisecond).String()
			r := buildJobResponse(jobID, timedOutResult, d)
			r = f.cache.store(taskName, jobID, r)

			slot.release(jobID, r.Success, d)
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
	resp = f.cache.store(taskName, jobID, resp)

	slot.release(jobID, resp.Success, duration)

	writeJSON(w, http.StatusOK, resp)
	attrs := []any{"success", result.Success, "duration", duration}
	if result.cause != nil {
		attrs = append(attrs, "error", result.cause)
	}
	log.Info("job completed", attrs...)
	f.wg.Done()
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
	if cached, ok := f.cache.lookupByJobID(jobID); ok {
		writeJSON(w, http.StatusOK, cached)
		return
	}
	writeJSON(w, http.StatusNotFound, errorResponse{Error: "result not found"})
}
