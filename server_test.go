package funtask

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")
	f := New("test-server",
		WithTask("echo", dummyTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = make(map[string]*taskSlot, len(f.tasks))
	for name := range f.tasks {
		f.slots[name] = &taskSlot{}
	}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.startedAt = time.Now()
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}
	return f
}

func startTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	f := testServer(t)
	return httptest.NewServer(f.routes())
}

func testServerWithTasks(t *testing.T, opts ...Option) *httptest.Server {
	t.Helper()
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")
	defaults := []Option{
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
	}
	f := New("test-server", append(defaults, opts...)...)
	f.logger = slog.With("server", f.name)
	f.slots = make(map[string]*taskSlot, len(f.tasks))
	for name := range f.tasks {
		f.slots[name] = &taskSlot{}
	}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}
	return httptest.NewServer(f.routes())
}

func TestWriteJSON_StatusAndBody(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusOK, map[string]string{"hello": "world"})
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["hello"] != "world" {
		t.Errorf("body = %v, want {\"hello\":\"world\"}", body)
	}
}

func TestWriteJSON_ErrorEnvelope(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
	var body errorResponse
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Error != "not found" {
		t.Errorf("error = %q, want %q", body.Error, "not found")
	}
}

func TestRoutes_LivezNoAuth(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/livez")
	if err != nil {
		t.Fatalf("GET /livez: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("body = %v, want {\"status\":\"ok\"}", body)
	}
}

func TestRoutes_ReadyzNoAuth(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("body = %v, want {\"status\":\"ok\"}", body)
	}
}

func TestRoutes_ReadyzFailing(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")
	f := New("test-server",
		WithTask("echo", dummyTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithReadiness(func() error {
			return fmt.Errorf("database unavailable")
		}),
	)
	f.logger = slog.With("server", f.name)
	f.slots = make(map[string]*taskSlot, len(f.tasks))
	for name := range f.tasks {
		f.slots[name] = &taskSlot{}
	}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "database unavailable" {
		t.Errorf("error = %q, want %q", body.Error, "database unavailable")
	}
}

func TestRoutes_AuthRequired(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/run/echo", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "unauthorized" {
		t.Errorf("error = %q, want %q", body.Error, "unauthorized")
	}
}

func TestRoutes_RunUnknownTask(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/run/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /run/nonexistent: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error == "" {
		t.Error("expected non-empty error message for unknown task")
	}
}

func TestRoutes_RunKnownTask(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/run/echo", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !body.Success {
		t.Errorf("success = %v, want true", body.Success)
	}
	if body.JobID == "" {
		t.Error("jobId is empty, want non-empty UUID")
	}
}

func TestRoutes_StopRequiresAuth(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/stop/echo", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /stop/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestRoutes_ResultRequiresAuth(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/result/some-id")
	if err != nil {
		t.Fatalf("GET /result/some-id: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestRoutes_HealthRequiresAuth(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestStop_RunningJob(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		select {
		case <-ctx.Done():
			close(cancelled)
			return Fail("cancelled", "stopped")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"slow": slowTask}
	f.slots = map[string]*taskSlot{"slow": {}}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start task in background.
	done := make(chan struct{})
	go func() {
		_, _ = postRun(srv, "slow", `{"jobId":"stop-test-1"}`)
		close(done)
	}()
	<-started

	// Send stop request.
	resp, err := postStop(srv, "slow")
	if err != nil {
		t.Fatalf("POST /stop/slow: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body stopResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Task != "slow" {
		t.Errorf("task = %q, want %q", body.Task, "slow")
	}
	if body.JobID != "stop-test-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "stop-test-1")
	}
	if body.Message != "Cancellation requested" {
		t.Errorf("message = %q, want %q", body.Message, "Cancellation requested")
	}

	// Verify task received cancellation.
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not receive cancellation")
	}

	<-done
}

func TestStop_NoRunningJob(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := postStop(srv, "echo")
	if err != nil {
		t.Fatalf("POST /stop/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body stopResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Task != "echo" {
		t.Errorf("task = %q, want %q", body.Task, "echo")
	}
	if body.JobID != "" {
		t.Errorf("jobId = %q, want empty (no running job)", body.JobID)
	}
	if body.Message != "No running job" {
		t.Errorf("message = %q, want %q", body.Message, "No running job")
	}
}

func TestStop_UnknownTask(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := postStop(srv, "unknown")
	if err != nil {
		t.Fatalf("POST /stop/unknown: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body errorResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Error != `task "unknown" not found` {
		t.Errorf("error = %q, want %q", body.Error, `task "unknown" not found`)
	}
}

func TestStop_CancellationDetected(t *testing.T) {
	started := make(chan struct{})
	resultCh := make(chan Result, 1)
	blockingTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("waiting")
		close(started)
		<-ctx.Done()
		res := Fail("cancelled", "job was cancelled")
		res.LastStep = "waiting"
		resultCh <- res
		return res
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"block": blockingTask}
	f.slots = map[string]*taskSlot{"block": {}}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start blocking task.
	done := make(chan struct{})
	go func() {
		_, _ = postRun(srv, "block", `{"jobId":"cancel-detect-1"}`)
		close(done)
	}()
	<-started

	// Cancel it.
	resp, err := postStop(srv, "block")
	if err != nil {
		t.Fatalf("POST /stop/block: %v", err)
	}
	_ = resp.Body.Close()

	// Verify task detected cancellation and produced a proper result.
	select {
	case res := <-resultCh:
		if res.Success {
			t.Error("result.Success = true, want false")
		}
		if res.ErrorCode != "cancelled" {
			t.Errorf("errorCode = %q, want %q", res.ErrorCode, "cancelled")
		}
		if res.LastStep != "waiting" {
			t.Errorf("lastStep = %q, want %q", res.LastStep, "waiting")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task did not return after cancellation")
	}

	<-done
}

func TestRoutes_ResultNotFound(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/result/some-id", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /result/some-id: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "result not found" {
		t.Errorf("error = %q, want %q", body.Error, "result not found")
	}
}

func TestRoutes_HealthReturns200(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/health", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["name"] != "test-server" {
		t.Errorf("name = %v, want %q", body["name"], "test-server")
	}
	if body["status"] != "ok" {
		t.Errorf("status = %v, want %q", body["status"], "ok")
	}
}

func TestRoutes_MethodNotAllowed(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	// GET /run/echo should fail — only POST is registered
	resp, err := http.Get(srv.URL + "/run/echo")
	if err != nil {
		t.Fatalf("GET /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

// --- Synchronous Task Execution Tests ---

func postRun(srv *httptest.Server, task, body string) (*http.Response, error) {
	req, _ := http.NewRequest("POST", srv.URL+"/run/"+task, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-secret")
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func postStop(srv *httptest.Server, task string) (*http.Response, error) {
	req, _ := http.NewRequest("POST", srv.URL+"/stop/"+task, nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	return http.DefaultClient.Do(req)
}

func TestRunHandler_SyncSuccess(t *testing.T) {
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		msg, _ := params.String("msg")
		return OK("echo: %s", msg).WithData("msg", msg)
	})
	srv := testServerWithTasks(t, WithTask("echo", echoTask))
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"params":{"msg":"hi"}}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !body.Success {
		t.Errorf("success = %v, want true", body.Success)
	}
	if body.Message != "echo: hi" {
		t.Errorf("message = %q, want %q", body.Message, "echo: hi")
	}
	if body.JobID == "" {
		t.Error("jobId is empty, want non-empty")
	}
	if body.Duration == "" {
		t.Error("duration is empty, want non-empty")
	}
	if body.Data["msg"] != "hi" {
		t.Errorf("data.msg = %v, want %q", body.Data["msg"], "hi")
	}
	if body.Error != nil {
		t.Errorf("error = %v, want nil", body.Error)
	}
}

func TestRunHandler_EmptyBody(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/run/echo", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !body.Success {
		t.Errorf("success = %v, want true", body.Success)
	}
	if body.JobID == "" {
		t.Error("jobId is empty, want non-empty")
	}
	if body.Duration == "" {
		t.Error("duration is empty, want non-empty")
	}
}

func TestRunHandler_InvalidJSON(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{bad json`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "invalid request body" {
		t.Errorf("error = %q, want %q", body.Error, "invalid request body")
	}
}

func TestRunHandler_BusyTask_Returns409(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-release
		return OK("done")
	})
	srv := testServerWithTasks(t, WithTask("slow", slowTask))
	defer srv.Close()

	// Start first request in background
	var firstResp *http.Response
	var firstErr error
	done := make(chan struct{})
	go func() {
		firstResp, firstErr = postRun(srv, "slow", `{"jobId":"job-1"}`)
		close(done)
	}()

	<-started // wait for task to be running

	// Second request should get 409
	resp, err := postRun(srv, "slow", `{"jobId":"job-2"}`)
	if err != nil {
		t.Fatalf("POST /run/slow (second): %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body busyResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "Task is busy with a different job" {
		t.Errorf("error = %q, want %q", body.Error, "Task is busy with a different job")
	}
	if body.Task != "slow" {
		t.Errorf("task = %q, want %q", body.Task, "slow")
	}
	if body.CurrentJob.JobID != "job-1" {
		t.Errorf("currentJob.jobId = %q, want %q", body.CurrentJob.JobID, "job-1")
	}
	if body.CurrentJob.Running == "" {
		t.Error("currentJob.running is empty, want non-empty duration")
	}

	close(release)
	<-done
	if firstErr != nil {
		t.Fatalf("POST /run/slow (first): %v", firstErr)
	}
	_ = firstResp.Body.Close()
}

func TestRunHandler_ConcurrentDifferentTasks(t *testing.T) {
	taskA := TaskFunc(func(ctx *Run, params Params) Result {
		time.Sleep(10 * time.Millisecond)
		return OK("a done")
	})
	taskB := TaskFunc(func(ctx *Run, params Params) Result {
		time.Sleep(10 * time.Millisecond)
		return OK("b done")
	})
	srv := testServerWithTasks(t, WithTask("task-a", taskA), WithTask("task-b", taskB))
	defer srv.Close()

	var wg sync.WaitGroup
	results := make([]int, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		resp, err := postRun(srv, "task-a", "")
		if err != nil {
			t.Errorf("POST /run/task-a: %v", err)
			return
		}
		results[0] = resp.StatusCode
		_ = resp.Body.Close()
	}()
	go func() {
		defer wg.Done()
		resp, err := postRun(srv, "task-b", "")
		if err != nil {
			t.Errorf("POST /run/task-b: %v", err)
			return
		}
		results[1] = resp.StatusCode
		_ = resp.Body.Close()
	}()

	wg.Wait()
	for i, code := range results {
		if code != http.StatusOK {
			t.Errorf("task %d: status = %d, want 200", i, code)
		}
	}
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestRunHandler_GeneratesJobID(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := postRun(srv, "echo", "")
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !uuidPattern.MatchString(body.JobID) {
		t.Errorf("jobId = %q, want UUID v4 format", body.JobID)
	}
}

func TestRunHandler_ClientJobID(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"jobId":"my-custom-id"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.JobID != "my-custom-id" {
		t.Errorf("jobId = %q, want %q", body.JobID, "my-custom-id")
	}
}

func TestRunHandler_FailResult(t *testing.T) {
	failTask := TaskFunc(func(ctx *Run, params Params) Result {
		return Fail("db_error", "connection refused")
	})
	srv := testServerWithTasks(t, WithTask("fail", failTask))
	defer srv.Close()

	resp, err := postRun(srv, "fail", "")
	if err != nil {
		t.Fatalf("POST /run/fail: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Success {
		t.Error("success = true, want false")
	}
	if body.Message != "" {
		t.Errorf("message = %q, want empty (failure uses error object)", body.Message)
	}
	if body.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if body.Error.Code != "db_error" {
		t.Errorf("error.code = %q, want %q", body.Error.Code, "db_error")
	}
	if body.Error.Message != "connection refused" {
		t.Errorf("error.message = %q, want %q", body.Error.Message, "connection refused")
	}
}

func TestRunHandler_WithData(t *testing.T) {
	dataTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("processed").WithData("count", 42).WithData("status", "ok")
	})
	srv := testServerWithTasks(t, WithTask("data", dataTask))
	defer srv.Close()

	resp, err := postRun(srv, "data", "")
	if err != nil {
		t.Fatalf("POST /run/data: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !body.Success {
		t.Error("success = false, want true")
	}
	if body.Data["count"] != float64(42) {
		t.Errorf("data.count = %v, want 42", body.Data["count"])
	}
	if body.Data["status"] != "ok" {
		t.Errorf("data.status = %v, want %q", body.Data["status"], "ok")
	}
}

func TestRunHandler_CallbackURL_NoAllowlist_Rejected(t *testing.T) {
	srv := startTestServer(t) // no allowlist configured
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://example.com/cb"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	want := "callback URLs not allowed (no allowlist configured)"
	if body.Error != want {
		t.Errorf("error = %q, want %q", body.Error, want)
	}
}

func TestRunHandler_CallbackURL_WithAllowlist_InvalidURL_Rejected(t *testing.T) {
	srv := testServerWithTasks(t,
		WithTask("echo", dummyTask),
		WithCallbackAllowlist("https://hooks.example.com"),
	)
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://evil.com/steal"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "callback URL not allowed" {
		t.Errorf("error = %q, want %q", body.Error, "callback URL not allowed")
	}
}

func testAsyncServer(t *testing.T, opts ...Option) *Server {
	t.Helper()
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")
	defaults := []Option{
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithCallbackAllowlist("https://hooks.example.com"),
	}
	f := New("test-server", append(defaults, opts...)...)
	f.logger = slog.With("server", f.name)
	f.slots = make(map[string]*taskSlot, len(f.tasks))
	for name := range f.tasks {
		f.slots[name] = &taskSlot{}
	}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}
	return f
}

func TestRunHandler_CallbackURL_WithAllowlist_ValidURL_Accepted(t *testing.T) {
	delivered := make(chan struct{})
	f := testAsyncServer(t, WithTask("echo", dummyTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		defer close(delivered)
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/webhook/123"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body struct {
		JobID string `json:"jobId"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.JobID == "" {
		t.Error("jobId is empty, want non-empty")
	}

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}
}

func TestRunHandler_NoCallbackURL_NoAllowlist_SyncWorks(t *testing.T) {
	srv := startTestServer(t) // no allowlist, no callbackUrl
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestRunHandler_LastStepInError(t *testing.T) {
	stepTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("connecting to database")
		ctx.Step("querying orders")
		return Fail("query_error", "timeout waiting for response")
	})
	srv := testServerWithTasks(t, WithTask("step", stepTask))
	defer srv.Close()

	resp, err := postRun(srv, "step", "")
	if err != nil {
		t.Fatalf("POST /run/step: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if body.Error.Step != "querying orders" {
		t.Errorf("error.step = %q, want %q", body.Error.Step, "querying orders")
	}
}

// --- Timeout Enforcement & Panic Recovery Tests ---

func TestRunHandler_PanicRecovery(t *testing.T) {
	panicTask := TaskFunc(func(ctx *Run, params Params) Result {
		panic("something went wrong")
	})
	srv := testServerWithTasks(t, WithTask("panic", panicTask))
	defer srv.Close()

	resp, err := postRun(srv, "panic", "")
	if err != nil {
		t.Fatalf("POST /run/panic: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Success {
		t.Error("success = true, want false")
	}
	if body.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if body.Error.Code != "panic" {
		t.Errorf("error.code = %q, want %q", body.Error.Code, "panic")
	}
	if !strings.Contains(body.Error.Message, "something went wrong") {
		t.Errorf("error.message = %q, want to contain %q", body.Error.Message, "something went wrong")
	}
}

func TestRunHandler_PanicRecovery_LastStep(t *testing.T) {
	panicTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("doing X")
		panic("oops")
	})
	srv := testServerWithTasks(t, WithTask("panic", panicTask))
	defer srv.Close()

	resp, err := postRun(srv, "panic", "")
	if err != nil {
		t.Fatalf("POST /run/panic: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if body.Error.Step != "doing X" {
		t.Errorf("error.step = %q, want %q", body.Error.Step, "doing X")
	}
}

func TestRunHandler_PanicRecovery_NoStackInResponse(t *testing.T) {
	panicTask := TaskFunc(func(ctx *Run, params Params) Result {
		panic("boom")
	})
	srv := testServerWithTasks(t, WithTask("panic", panicTask))
	defer srv.Close()

	resp, err := postRun(srv, "panic", "")
	if err != nil {
		t.Fatalf("POST /run/panic: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var raw map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&raw)

	// Marshal back to string to check for stack trace indicators
	b, _ := json.Marshal(raw)
	s := string(b)
	if strings.Contains(s, "goroutine") {
		t.Error("response contains 'goroutine' — stack trace leaked into response")
	}
	if strings.Contains(s, ".go:") {
		t.Error("response contains '.go:' — stack trace leaked into response")
	}
}

func TestRunHandler_PanicRecovery_SlotReleased(t *testing.T) {
	panicTask := TaskFunc(func(ctx *Run, params Params) Result {
		panic("crash")
	})
	srv := testServerWithTasks(t, WithTask("panic", panicTask))
	defer srv.Close()

	// First request panics
	resp, err := postRun(srv, "panic", "")
	if err != nil {
		t.Fatalf("POST /run/panic (first): %v", err)
	}
	_ = resp.Body.Close()

	// Second request should succeed (slot released)
	resp, err = postRun(srv, "panic", "")
	if err != nil {
		t.Fatalf("POST /run/panic (second): %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (slot should be released after panic)", resp.StatusCode)
	}
}

func TestRunHandler_PanicRecovery_NonStringValue(t *testing.T) {
	panicTask := TaskFunc(func(ctx *Run, params Params) Result {
		panic(42)
	})
	srv := testServerWithTasks(t, WithTask("panic", panicTask))
	defer srv.Close()

	resp, err := postRun(srv, "panic", "")
	if err != nil {
		t.Fatalf("POST /run/panic: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if body.Error.Code != "panic" {
		t.Errorf("error.code = %q, want %q", body.Error.Code, "panic")
	}
	if !strings.Contains(body.Error.Message, "42") {
		t.Errorf("error.message = %q, want to contain %q", body.Error.Message, "42")
	}
}

func TestRunHandler_SyncTimeout_Returns504(t *testing.T) {
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	srv := testServerWithTasks(t,
		WithTask("slow", slowTask),
		WithSyncTimeout(50*time.Millisecond),
	)
	defer srv.Close()

	resp, err := postRun(srv, "slow", "")
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	want := fmt.Sprintf("Sync mode timeout exceeded (%s). Use callbackUrl for long-running tasks.", 50*time.Millisecond)
	if body.Error != want {
		t.Errorf("error = %q, want %q", body.Error, want)
	}
}

func TestRunHandler_SyncTimeout_CancelsContext(t *testing.T) {
	ctxCancelled := make(chan bool, 1)
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			ctxCancelled <- true
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			ctxCancelled <- false
			return OK("done")
		}
	})
	srv := testServerWithTasks(t,
		WithTask("slow", slowTask),
		WithSyncTimeout(50*time.Millisecond),
	)
	defer srv.Close()

	resp, err := postRun(srv, "slow", "")
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case cancelled := <-ctxCancelled:
		if !cancelled {
			t.Error("context was not cancelled after sync timeout")
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for task to observe context cancellation")
	}
}

func TestRunHandler_SyncTimeout_SlotReleased(t *testing.T) {
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	srv := testServerWithTasks(t,
		WithTask("slow", slowTask),
		WithSyncTimeout(50*time.Millisecond),
	)
	defer srv.Close()

	// First request times out with 504
	resp, err := postRun(srv, "slow", "")
	if err != nil {
		t.Fatalf("POST /run/slow (first): %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", resp.StatusCode)
	}

	// Poll until slot is released (cleanup goroutine must finish)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = postRun(srv, "slow", "")
		if err != nil {
			t.Fatalf("POST /run/slow (second): %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			// Got 200 or 504 — slot was released (not 409 busy)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("slot still busy after 2s — cleanup goroutine did not release it")
}

func TestRunHandler_MaxDuration(t *testing.T) {
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	srv := testServerWithTasks(t,
		WithTask("slow", slowTask),
		WithMaxDuration(50*time.Millisecond),
		WithSyncTimeout(5*time.Second),
	)
	defer srv.Close()

	resp, err := postRun(srv, "slow", "")
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Success {
		t.Error("success = true, want false")
	}
	if body.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if body.Error.Code != "max_duration_exceeded" {
		t.Errorf("error.code = %q, want %q", body.Error.Code, "max_duration_exceeded")
	}
	wantMsg := fmt.Sprintf("job exceeded maximum duration (%s)", 50*time.Millisecond)
	if body.Error.Message != wantMsg {
		t.Errorf("error.message = %q, want %q", body.Error.Message, wantMsg)
	}
}

func TestRunHandler_MaxDuration_CancelsContext(t *testing.T) {
	ctxCancelled := make(chan bool, 1)
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			ctxCancelled <- true
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			ctxCancelled <- false
			return OK("done")
		}
	})
	srv := testServerWithTasks(t,
		WithTask("slow", slowTask),
		WithMaxDuration(50*time.Millisecond),
		WithSyncTimeout(5*time.Second),
	)
	defer srv.Close()

	resp, err := postRun(srv, "slow", "")
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case cancelled := <-ctxCancelled:
		if !cancelled {
			t.Error("context was not cancelled at maxDuration boundary")
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for task to observe context cancellation")
	}
}

func TestRunHandler_NormalCompletion_Unchanged(t *testing.T) {
	fastTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("fast result")
	})
	srv := testServerWithTasks(t, WithTask("fast", fastTask))
	defer srv.Close()

	resp, err := postRun(srv, "fast", "")
	if err != nil {
		t.Fatalf("POST /run/fast: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !body.Success {
		t.Error("success = false, want true")
	}
	if body.Message != "fast result" {
		t.Errorf("message = %q, want %q", body.Message, "fast result")
	}
	if body.JobID == "" {
		t.Error("jobId is empty, want non-empty")
	}
	if body.Duration == "" {
		t.Error("duration is empty, want non-empty")
	}
}

func TestRunHandler_ContentType(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	tests := []struct {
		name string
		body string
		want int
	}{
		{"success", "", http.StatusOK},
		{"bad json", "{bad", http.StatusBadRequest},
		{"async no allowlist", `{"callbackUrl":"https://x.com"}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := postRun(srv, "echo", tt.body)
			if err != nil {
				t.Fatalf("POST /run/echo: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("content-type = %q, want %q", ct, "application/json")
			}
		})
	}
}

// --- Duplicate Detection & Result Cache Tests ---

func getResult(srv *httptest.Server, jobID string) (*http.Response, error) {
	req, _ := http.NewRequest("GET", srv.URL+"/result/"+jobID, nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	return http.DefaultClient.Do(req)
}

func TestRunHandler_DuplicateRunning(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-release
		return OK("done")
	})
	srv := testServerWithTasks(t, WithTask("slow", slowTask))
	defer srv.Close()

	var firstResp *http.Response
	var firstErr error
	done := make(chan struct{})
	go func() {
		firstResp, firstErr = postRun(srv, "slow", `{"jobId":"dup-run-1"}`)
		close(done)
	}()

	<-started

	// Duplicate request with same jobId
	resp, err := postRun(srv, "slow", `{"jobId":"dup-run-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow (duplicate): %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body runningJobStatus
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.JobID != "dup-run-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "dup-run-1")
	}
	if body.Status != "running" {
		t.Errorf("status = %q, want %q", body.Status, "running")
	}
	if body.Running == "" {
		t.Error("running is empty, want non-empty duration string")
	}

	close(release)
	<-done
	if firstErr != nil {
		t.Fatalf("POST /run/slow (first): %v", firstErr)
	}
	_ = firstResp.Body.Close()
}

func TestRunHandler_DuplicateRunning_CurrentStep(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("doing X")
		close(started)
		<-release
		return OK("done")
	})
	srv := testServerWithTasks(t, WithTask("slow", slowTask))
	defer srv.Close()

	var firstResp *http.Response
	var firstErr error
	done := make(chan struct{})
	go func() {
		firstResp, firstErr = postRun(srv, "slow", `{"jobId":"dup-step-1"}`)
		close(done)
	}()

	<-started

	resp, err := postRun(srv, "slow", `{"jobId":"dup-step-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow (duplicate): %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body runningJobStatus
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.CurrentStep != "doing X" {
		t.Errorf("currentStep = %q, want %q", body.CurrentStep, "doing X")
	}

	close(release)
	<-done
	if firstErr != nil {
		t.Fatalf("POST /run/slow (first): %v", firstErr)
	}
	_ = firstResp.Body.Close()
}

func TestRunHandler_DuplicateCompleted(t *testing.T) {
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("echo result").WithData("key", "val")
	})
	srv := testServerWithTasks(t, WithTask("echo", echoTask))
	defer srv.Close()

	// First request
	resp1, err := postRun(srv, "echo", `{"jobId":"dup-done-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo (first): %v", err)
	}
	defer func() { _ = resp1.Body.Close() }()
	var body1 jobResponse
	_ = json.NewDecoder(resp1.Body).Decode(&body1)
	if !body1.Success {
		t.Fatalf("first request: success = false, want true")
	}

	// Duplicate request with same jobId
	resp2, err := postRun(srv, "echo", `{"jobId":"dup-done-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo (duplicate): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp2.StatusCode)
	}
	var body2 jobResponse
	_ = json.NewDecoder(resp2.Body).Decode(&body2)
	if body2.JobID != "dup-done-1" {
		t.Errorf("jobId = %q, want %q", body2.JobID, "dup-done-1")
	}
	if !body2.Success {
		t.Error("success = false, want true")
	}
	if body2.Message != "echo result" {
		t.Errorf("message = %q, want %q", body2.Message, "echo result")
	}
	if body2.Data["key"] != "val" {
		t.Errorf("data.key = %v, want %q", body2.Data["key"], "val")
	}
	if body2.Duration == "" {
		t.Error("duration is empty on cached result, want non-empty")
	}
}

func TestGetResult_Found(t *testing.T) {
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("cached result").WithData("found", true)
	})
	srv := testServerWithTasks(t, WithTask("echo", echoTask))
	defer srv.Close()

	// Run the task first
	resp1, err := postRun(srv, "echo", `{"jobId":"get-result-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	_ = resp1.Body.Close()

	// GET /result/{jobId}
	resp, err := getResult(srv, "get-result-1")
	if err != nil {
		t.Fatalf("GET /result/get-result-1: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.JobID != "get-result-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "get-result-1")
	}
	if !body.Success {
		t.Error("success = false, want true")
	}
	if body.Message != "cached result" {
		t.Errorf("message = %q, want %q", body.Message, "cached result")
	}
	if body.Data["found"] != true {
		t.Errorf("data.found = %v, want true", body.Data["found"])
	}
}

func TestGetResult_NotFound(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := getResult(srv, "unknown-job-id")
	if err != nil {
		t.Fatalf("GET /result/unknown-job-id: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "result not found" {
		t.Errorf("error = %q, want %q", body.Error, "result not found")
	}
}

func TestResultCache_Expiry(t *testing.T) {
	c := &resultCache{entries: make(map[string]*resultCacheEntry)}
	resp := jobResponse{JobID: "exp-1", Success: true, Duration: "1ms"}

	// Manually insert an already-expired entry.
	c.mu.Lock()
	c.entries[cacheKey("task", "exp-1")] = &resultCacheEntry{
		taskName:  "task",
		jobID:     "exp-1",
		response:  resp,
		expiresAt: time.Now().Add(-1 * time.Second),
	}
	c.mu.Unlock()

	// lookup should return false for expired entry.
	if _, ok := c.lookup("task", "exp-1"); ok {
		t.Error("lookup returned true for expired entry, want false")
	}

	// lookupByJobID should also return false.
	if _, ok := c.lookupByJobID("exp-1"); ok {
		t.Error("lookupByJobID returned true for expired entry, want false")
	}
}

func TestResultCache_LazyEviction(t *testing.T) {
	c := &resultCache{entries: make(map[string]*resultCacheEntry)}

	// Insert two expired entries.
	c.mu.Lock()
	c.entries[cacheKey("task", "old-1")] = &resultCacheEntry{
		taskName:  "task",
		jobID:     "old-1",
		response:  jobResponse{JobID: "old-1"},
		expiresAt: time.Now().Add(-1 * time.Second),
	}
	c.entries[cacheKey("task", "old-2")] = &resultCacheEntry{
		taskName:  "task",
		jobID:     "old-2",
		response:  jobResponse{JobID: "old-2"},
		expiresAt: time.Now().Add(-1 * time.Second),
	}
	c.mu.Unlock()

	// store() should evict the expired entries.
	c.store("task", "new-1", jobResponse{JobID: "new-1", Success: true, Duration: "1ms"})

	c.mu.RLock()
	count := len(c.entries)
	_, hasOld1 := c.entries[cacheKey("task", "old-1")]
	_, hasOld2 := c.entries[cacheKey("task", "old-2")]
	_, hasNew := c.entries[cacheKey("task", "new-1")]
	c.mu.RUnlock()

	if hasOld1 {
		t.Error("expired entry old-1 still in cache after store")
	}
	if hasOld2 {
		t.Error("expired entry old-2 still in cache after store")
	}
	if !hasNew {
		t.Error("new entry not found in cache after store")
	}
	if count != 1 {
		t.Errorf("cache size = %d, want 1", count)
	}
}

func TestRunHandler_ResultTooLarge(t *testing.T) {
	bigTask := TaskFunc(func(ctx *Run, params Params) Result {
		big := make([]byte, 11<<20) // 11 MB
		return OK("big").WithData("payload", string(big))
	})
	srv := testServerWithTasks(t, WithTask("big", bigTask))
	defer srv.Close()

	resp, err := postRun(srv, "big", `{"jobId":"big-1"}`)
	if err != nil {
		t.Fatalf("POST /run/big: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Success {
		t.Error("success = true, want false (result too large)")
	}
	if body.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if body.Error.Code != "result_too_large" {
		t.Errorf("error.code = %q, want %q", body.Error.Code, "result_too_large")
	}
	if body.Error.Message != "result exceeds maximum size (10 MB)" {
		t.Errorf("error.message = %q, want %q", body.Error.Message, "result exceeds maximum size (10 MB)")
	}
	if body.JobID != "big-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "big-1")
	}
}

func TestRunHandler_DuplicatePerTask(t *testing.T) {
	taskA := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("from A").WithData("task", "a")
	})
	taskB := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("from B").WithData("task", "b")
	})
	srv := testServerWithTasks(t, WithTask("task-a", taskA), WithTask("task-b", taskB))
	defer srv.Close()

	// Run same jobId on task-a and task-b — both should execute independently.
	respA, err := postRun(srv, "task-a", `{"jobId":"shared-id"}`)
	if err != nil {
		t.Fatalf("POST /run/task-a: %v", err)
	}
	defer func() { _ = respA.Body.Close() }()
	var bodyA jobResponse
	_ = json.NewDecoder(respA.Body).Decode(&bodyA)
	if !bodyA.Success {
		t.Fatal("task-a: success = false, want true")
	}
	if bodyA.Data["task"] != "a" {
		t.Errorf("task-a: data.task = %v, want %q", bodyA.Data["task"], "a")
	}

	respB, err := postRun(srv, "task-b", `{"jobId":"shared-id"}`)
	if err != nil {
		t.Fatalf("POST /run/task-b: %v", err)
	}
	defer func() { _ = respB.Body.Close() }()
	var bodyB jobResponse
	_ = json.NewDecoder(respB.Body).Decode(&bodyB)
	if !bodyB.Success {
		t.Fatal("task-b: success = false, want true")
	}
	if bodyB.Data["task"] != "b" {
		t.Errorf("task-b: data.task = %v, want %q", bodyB.Data["task"], "b")
	}
}

func TestGetResult_CrossTask(t *testing.T) {
	taskA := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("from task-a")
	})
	srv := testServerWithTasks(t, WithTask("task-a", taskA))
	defer srv.Close()

	// Complete task-a with a specific jobId.
	resp1, err := postRun(srv, "task-a", `{"jobId":"cross-task-1"}`)
	if err != nil {
		t.Fatalf("POST /run/task-a: %v", err)
	}
	_ = resp1.Body.Close()

	// GET /result/{jobId} should find it across all tasks.
	resp, err := getResult(srv, "cross-task-1")
	if err != nil {
		t.Fatalf("GET /result/cross-task-1: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.JobID != "cross-task-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "cross-task-1")
	}
	if !body.Success {
		t.Error("success = false, want true")
	}
	if body.Message != "from task-a" {
		t.Errorf("message = %q, want %q", body.Message, "from task-a")
	}
}

func TestRunHandler_SyncTimeout_CachesResult(t *testing.T) {
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	srv := testServerWithTasks(t,
		WithTask("slow", slowTask),
		WithSyncTimeout(50*time.Millisecond),
	)
	defer srv.Close()

	// First request times out with 504
	resp, err := postRun(srv, "slow", `{"jobId":"timeout-cache-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", resp.StatusCode)
	}

	// Poll GET /result until the cleanup goroutine caches the result.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = getResult(srv, "timeout-cache-1")
		if err != nil {
			t.Fatalf("GET /result/timeout-cache-1: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return // cached result found
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("cached result not found via GET /result after sync timeout cleanup")
}

func TestRunHandler_NormalCompletion_StillWorks(t *testing.T) {
	fastTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("regression check")
	})
	srv := testServerWithTasks(t, WithTask("fast", fastTask))
	defer srv.Close()

	resp, err := postRun(srv, "fast", "")
	if err != nil {
		t.Fatalf("POST /run/fast: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if !body.Success {
		t.Error("success = false, want true")
	}
	if body.Message != "regression check" {
		t.Errorf("message = %q, want %q", body.Message, "regression check")
	}
	if body.JobID == "" {
		t.Error("jobId is empty, want non-empty")
	}
	if body.Duration == "" {
		t.Error("duration is empty, want non-empty")
	}
}

// --- Async Execution Path Tests ---

func TestRunHandler_Async_Returns202WithJobId(t *testing.T) {
	done := make(chan struct{})
	f := testAsyncServer(t, WithTask("echo", dummyTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		defer close(done)
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"async-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body struct {
		JobID string `json:"jobId"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.JobID != "async-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "async-1")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}
}

func TestRunHandler_Async_DeliveryChain(t *testing.T) {
	var dlWritten []byte
	var dlRemoved bool
	var cbURL string
	var cbData []byte

	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("async result").WithData("key", "val")
	})
	f := testAsyncServer(t, WithTask("echo", echoTask))
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		dlWritten = data
		return nil
	}
	f.deliverer.removeFunc = func(name string) error {
		dlRemoved = true
		return nil
	}
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		// Verify dead letter was written BEFORE callback attempt.
		if dlWritten == nil {
			t.Error("callback attempted before dead letter was written")
		}
		cbURL = url
		cbData = data
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"chain-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	// Wait for async goroutine to fully complete (slot released).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := f.slots["echo"]
		s.mu.Lock()
		running := s.running
		s.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify dead letter was written before callback.
	if dlWritten == nil {
		t.Fatal("dead letter was not written")
	}
	// Verify callback was POSTed to the correct URL.
	if cbURL != "https://hooks.example.com/cb" {
		t.Errorf("callback URL = %q, want %q", cbURL, "https://hooks.example.com/cb")
	}
	// Verify callback data matches dead letter data.
	if string(cbData) != string(dlWritten) {
		t.Error("callback data does not match dead letter data")
	}
	// Verify dead letter was removed on success.
	if !dlRemoved {
		t.Error("dead letter was not removed after successful delivery")
	}
	// Verify the callback payload contains expected fields.
	var payload jobResponse
	if err := json.Unmarshal(cbData, &payload); err != nil {
		t.Fatalf("unmarshal callback data: %v", err)
	}
	if payload.JobID != "chain-1" {
		t.Errorf("payload.jobId = %q, want %q", payload.JobID, "chain-1")
	}
	if !payload.Success {
		t.Error("payload.success = false, want true")
	}
	if payload.Message != "async result" {
		t.Errorf("payload.message = %q, want %q", payload.Message, "async result")
	}
}

func TestRunHandler_Async_PanicRecovery(t *testing.T) {
	done := make(chan []byte, 1)
	panicTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("setup")
		panic("async boom")
	})
	f := testAsyncServer(t, WithTask("panic", panicTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		done <- data
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "panic", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"panic-async-1"}`)
	if err != nil {
		t.Fatalf("POST /run/panic: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	var cbData []byte
	select {
	case cbData = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}

	var payload jobResponse
	if err := json.Unmarshal(cbData, &payload); err != nil {
		t.Fatalf("unmarshal callback data: %v", err)
	}
	if payload.Success {
		t.Error("payload.success = true, want false")
	}
	if payload.Error == nil {
		t.Fatal("payload.error is nil, want non-nil")
	}
	if payload.Error.Code != "panic" {
		t.Errorf("payload.error.code = %q, want %q", payload.Error.Code, "panic")
	}
	if !strings.Contains(payload.Error.Message, "async boom") {
		t.Errorf("payload.error.message = %q, want to contain %q", payload.Error.Message, "async boom")
	}
	if payload.Error.Step != "setup" {
		t.Errorf("payload.error.step = %q, want %q", payload.Error.Step, "setup")
	}
	// Verify no stack trace in response.
	raw := string(cbData)
	if strings.Contains(raw, "goroutine") {
		t.Error("callback payload contains 'goroutine' — stack trace leaked")
	}
}

func TestRunHandler_Async_MaxDurationExceeded(t *testing.T) {
	done := make(chan []byte, 1)
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	f := testAsyncServer(t,
		WithTask("slow", slowTask),
		WithMaxDuration(50*time.Millisecond),
	)
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		done <- data
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "slow", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"maxdur-async-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	var cbData []byte
	select {
	case cbData = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}

	var payload jobResponse
	if err := json.Unmarshal(cbData, &payload); err != nil {
		t.Fatalf("unmarshal callback data: %v", err)
	}
	if payload.Success {
		t.Error("payload.success = true, want false")
	}
	if payload.Error == nil {
		t.Fatal("payload.error is nil, want non-nil")
	}
	if payload.Error.Code != "max_duration_exceeded" {
		t.Errorf("payload.error.code = %q, want %q", payload.Error.Code, "max_duration_exceeded")
	}
	wantMsg := fmt.Sprintf("job exceeded maximum duration (%s)", 50*time.Millisecond)
	if payload.Error.Message != wantMsg {
		t.Errorf("payload.error.message = %q, want %q", payload.Error.Message, wantMsg)
	}
}

func TestRunHandler_Async_Busy409(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-release
		return OK("done")
	})
	f := testAsyncServer(t, WithTask("slow", slowTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start first async job.
	resp1, err := postRun(srv, "slow", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"busy-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow (first): %v", err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp1.StatusCode)
	}

	<-started

	// Second async request with DIFFERENT jobId — should get 409.
	resp2, err := postRun(srv, "slow", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"busy-2"}`)
	if err != nil {
		t.Fatalf("POST /run/slow (second): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp2.StatusCode)
	}
	var body busyResponse
	_ = json.NewDecoder(resp2.Body).Decode(&body)
	if body.Error != "Task is busy with a different job" {
		t.Errorf("error = %q, want %q", body.Error, "Task is busy with a different job")
	}
	if body.CurrentJob.JobID != "busy-1" {
		t.Errorf("currentJob.jobId = %q, want %q", body.CurrentJob.JobID, "busy-1")
	}

	close(release)
	// Wait for cleanup.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := f.slots["slow"]
		s.mu.Lock()
		running := s.running
		s.mu.Unlock()
		if !running {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("async goroutine did not complete within 2s")
}

func TestRunHandler_Async_DuplicateRunning(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-release
		return OK("done")
	})
	f := testAsyncServer(t, WithTask("slow", slowTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start async job.
	resp1, err := postRun(srv, "slow", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"dup-async-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow (first): %v", err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp1.StatusCode)
	}

	<-started

	// Duplicate request with same jobId — should return 200 running status (not 202).
	resp2, err := postRun(srv, "slow", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"dup-async-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow (duplicate): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp2.StatusCode)
	}
	var body runningJobStatus
	_ = json.NewDecoder(resp2.Body).Decode(&body)
	if body.JobID != "dup-async-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "dup-async-1")
	}
	if body.Status != "running" {
		t.Errorf("status = %q, want %q", body.Status, "running")
	}

	close(release)
	// Wait for async goroutine to complete.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := f.slots["slow"]
		s.mu.Lock()
		running := s.running
		s.mu.Unlock()
		if !running {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("async goroutine did not complete within 2s")
}

func TestRunHandler_Async_DuplicateCached(t *testing.T) {
	done := make(chan struct{})
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("cached async").WithData("k", "v")
	})
	f := testAsyncServer(t, WithTask("echo", echoTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		defer close(done)
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start async job.
	resp1, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"dup-cached-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo (first): %v", err)
	}
	_ = resp1.Body.Close()

	// Wait for delivery to complete.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}

	// Wait for slot release and cache store.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := f.slots["echo"]
		s.mu.Lock()
		running := s.running
		s.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Duplicate request — should return 200 with cached result.
	resp2, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"dup-cached-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo (duplicate): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp2.StatusCode)
	}
	var body jobResponse
	_ = json.NewDecoder(resp2.Body).Decode(&body)
	if body.JobID != "dup-cached-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "dup-cached-1")
	}
	if !body.Success {
		t.Error("success = false, want true")
	}
	if body.Data["k"] != "v" {
		t.Errorf("data.k = %v, want %q", body.Data["k"], "v")
	}
}

func TestRunHandler_Async_SlotReleasedAfterDelivery(t *testing.T) {
	done := make(chan struct{})
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("done")
	})
	f := testAsyncServer(t, WithTask("echo", echoTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		defer close(done)
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"slot-release-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}

	// Wait for slot release.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := f.slots["echo"]
		s.mu.Lock()
		running := s.running
		s.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A new sync job should succeed on the same task.
	resp2, err := postRun(srv, "echo", `{}`)
	if err != nil {
		t.Fatalf("POST /run/echo (second): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (slot should be released)", resp2.StatusCode)
	}
}

func TestRunHandler_Async_ResultCachedAfterDelivery(t *testing.T) {
	done := make(chan struct{})
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("async cached result")
	})
	f := testAsyncServer(t, WithTask("echo", echoTask))
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		defer close(done)
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"result-cache-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}

	// Poll GET /result until cached.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = getResult(srv, "result-cache-1")
		if err != nil {
			t.Fatalf("GET /result/result-cache-1: %v", err)
		}
		if resp.StatusCode == http.StatusOK {
			var body jobResponse
			_ = json.NewDecoder(resp.Body).Decode(&body)
			_ = resp.Body.Close()
			if body.JobID != "result-cache-1" {
				t.Errorf("jobId = %q, want %q", body.JobID, "result-cache-1")
			}
			if !body.Success {
				t.Error("success = false, want true")
			}
			if body.Message != "async cached result" {
				t.Errorf("message = %q, want %q", body.Message, "async cached result")
			}
			return
		}
		_ = resp.Body.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("cached result not found via GET /result after async delivery")
}

// --- SIGTERM Handling & Draining State Tests ---

func TestHandleRun_DrainingRejects503(t *testing.T) {
	f := testServer(t)
	f.draining.Store(true)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"jobId":"drain-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body errorResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Error != "Server is shutting down" {
		t.Errorf("error = %q, want %q", body.Error, "Server is shutting down")
	}
}

func TestDraining_StopStillWorks(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		select {
		case <-ctx.Done():
			close(cancelled)
			return Fail("cancelled", "stopped")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"slow": slowTask}
	f.slots = map[string]*taskSlot{"slow": {}}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start task in background.
	done := make(chan struct{})
	go func() {
		_, _ = postRun(srv, "slow", `{"jobId":"drain-stop-1"}`)
		close(done)
	}()
	<-started

	// Set draining AFTER job started.
	f.draining.Store(true)

	// /stop should still work during draining.
	resp, err := postStop(srv, "slow")
	if err != nil {
		t.Fatalf("POST /stop/slow: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body stopResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Task != "slow" {
		t.Errorf("task = %q, want %q", body.Task, "slow")
	}
	if body.JobID != "drain-stop-1" {
		t.Errorf("jobId = %q, want %q", body.JobID, "drain-stop-1")
	}
	if body.Message != "Cancellation requested" {
		t.Errorf("message = %q, want %q", body.Message, "Cancellation requested")
	}

	// Verify task received cancellation.
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not receive cancellation")
	}

	<-done

	// /run should be rejected during draining.
	resp2, err := postRun(srv, "slow", `{"jobId":"drain-stop-2"}`)
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("draining /run status = %d, want 503", resp2.StatusCode)
	}
}

func TestRunHandler_Async_DeadLetterWriteFailure(t *testing.T) {
	callbackAttempted := false

	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("should not deliver")
	})
	f := testAsyncServer(t, WithTask("echo", echoTask))
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		return fmt.Errorf("disk full")
	}
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		callbackAttempted = true
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"dl-fail-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	// Wait for async goroutine to complete (slot released).
	// Polling starts AFTER postRun returns 202, so slot is guaranteed to be acquired.
	slot := f.slots["echo"]
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		slot.mu.Lock()
		running := slot.running
		slot.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if callbackAttempted {
		t.Error("callback was attempted despite dead letter write failure")
	}

	// Slot should be released.
	slot.mu.Lock()
	running := slot.running
	slot.mu.Unlock()
	if running {
		t.Error("slot is still running, want released after dead letter write failure")
	}
}
