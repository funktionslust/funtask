package funtask

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func getHealth(srv *httptest.Server) (*http.Response, error) {
	req, _ := http.NewRequest("GET", srv.URL+"/health", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	return http.DefaultClient.Do(req)
}

func TestHealthHandler_NodeInfo(t *testing.T) {
	f := testServer(t)
	f.startedAt = time.Now().Add(-1 * time.Hour)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}

	var body healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Name != "test-server" {
		t.Errorf("node = %q, want %q", body.Name, "test-server")
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
	if body.Uptime == "" {
		t.Error("uptime is empty, want non-empty")
	}
	if body.Tasks == nil {
		t.Fatal("tasks is nil, want non-nil map")
	}
}

func TestHealthHandler_IdleTaskNoJobs(t *testing.T) {
	f := testServer(t)
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task, ok := body.Tasks["echo"]
	if !ok {
		t.Fatal("tasks[\"echo\"] not found")
	}
	if task.Status != "idle" {
		t.Errorf("echo status = %q, want %q", task.Status, "idle")
	}
	if task.LastJob != nil {
		t.Errorf("echo lastJob = %+v, want nil (never ran)", task.LastJob)
	}
	if task.CurrentJob != nil {
		t.Errorf("echo currentJob = %+v, want nil", task.CurrentJob)
	}
}

func TestHealthHandler_RunningTask(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("Processing batch 3")
		ctx.Progress(3, 10, "Batch 3 of 10")
		close(started)
		<-release
		return OK("done")
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"slow": slowTask}
	f.slots = map[string]*taskSlot{"slow": {}}
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start task in background.
	done := make(chan struct{})
	go func() {
		_, _ = postRun(srv, "slow", `{"jobId":"health-run-1"}`)
		close(done)
	}()

	<-started

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task, ok := body.Tasks["slow"]
	if !ok {
		t.Fatal("tasks[\"slow\"] not found")
	}
	if task.Status != "running" {
		t.Errorf("status = %q, want %q", task.Status, "running")
	}
	if task.CurrentJob == nil {
		t.Fatal("currentJob is nil")
	}
	if task.CurrentJob.JobID != "health-run-1" {
		t.Errorf("jobId = %q, want %q", task.CurrentJob.JobID, "health-run-1")
	}
	if task.CurrentJob.Running == "" {
		t.Error("running is empty, want non-empty duration")
	}
	if task.CurrentJob.CurrentStep != "Batch 3 of 10" {
		t.Errorf("currentStep = %q, want %q", task.CurrentJob.CurrentStep, "Batch 3 of 10")
	}
	if task.CurrentJob.LastStepAt == "" {
		t.Error("lastStepAt is empty, want non-empty")
	}
	if task.CurrentJob.Progress == nil {
		t.Fatal("progress is nil, want non-nil")
	}
	if task.CurrentJob.Progress.Current != 3 {
		t.Errorf("progress.current = %d, want 3", task.CurrentJob.Progress.Current)
	}
	if task.CurrentJob.Progress.Total != 10 {
		t.Errorf("progress.total = %d, want 10", task.CurrentJob.Progress.Total)
	}
	if task.CurrentJob.Progress.Percent != 30 {
		t.Errorf("progress.percent = %v, want 30", task.CurrentJob.Progress.Percent)
	}

	close(release)
	<-done
}

func TestHealthHandler_RunningTaskStepOnly(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("Connecting to API")
		close(started)
		<-release
		return OK("done")
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"slow": slowTask}
	f.slots = map[string]*taskSlot{"slow": {}}
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	done := make(chan struct{})
	go func() {
		_, _ = postRun(srv, "slow", `{"jobId":"step-only-1"}`)
		close(done)
	}()

	<-started

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["slow"]
	if task.CurrentJob == nil {
		t.Fatal("currentJob is nil")
	}
	if task.CurrentJob.CurrentStep != "Connecting to API" {
		t.Errorf("currentStep = %q, want %q", task.CurrentJob.CurrentStep, "Connecting to API")
	}
	if task.CurrentJob.Progress != nil {
		t.Errorf("progress = %+v, want nil (Step only, no Progress called)", task.CurrentJob.Progress)
	}

	close(release)
	<-done
}

func TestHealthHandler_IdleWithLastJob(t *testing.T) {
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("echo done")
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"echo": echoTask}
	f.slots = map[string]*taskSlot{"echo": {}}
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Run task to completion.
	resp1, err := postRun(srv, "echo", `{"jobId":"last-job-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	_ = resp1.Body.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task, ok := body.Tasks["echo"]
	if !ok {
		t.Fatal("tasks[\"echo\"] not found")
	}
	if task.Status != "idle" {
		t.Errorf("status = %q, want %q", task.Status, "idle")
	}
	if task.CurrentJob != nil {
		t.Errorf("currentJob = %+v, want nil", task.CurrentJob)
	}
	if task.LastJob == nil {
		t.Fatal("lastJob is nil, want non-nil")
	}
	if task.LastJob.JobID != "last-job-1" {
		t.Errorf("lastJob.jobId = %q, want %q", task.LastJob.JobID, "last-job-1")
	}
	if !task.LastJob.Success {
		t.Error("lastJob.success = false, want true")
	}
	if task.LastJob.Finished == "" {
		t.Error("lastJob.finished is empty, want RFC3339 timestamp")
	}
	if task.LastJob.Duration == "" {
		t.Error("lastJob.duration is empty, want non-empty")
	}
}

func TestHealthHandler_MultipleTasks(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		ctx.Step("working")
		close(started)
		<-release
		return OK("done")
	})
	fastTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("fast done")
	})

	f := testServer(t)
	f.tasks = map[string]TaskFunc{"slow": slowTask, "fast": fastTask}
	f.slots = map[string]*taskSlot{"slow": {}, "fast": {}}
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Complete the fast task first.
	resp1, err := postRun(srv, "fast", `{"jobId":"fast-1"}`)
	if err != nil {
		t.Fatalf("POST /run/fast: %v", err)
	}
	_ = resp1.Body.Close()

	// Start slow task.
	done := make(chan struct{})
	go func() {
		_, _ = postRun(srv, "slow", `{"jobId":"slow-1"}`)
		close(done)
	}()

	<-started

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	// Slow should be running.
	slow, ok := body.Tasks["slow"]
	if !ok {
		t.Fatal("tasks[\"slow\"] not found")
	}
	if slow.Status != "running" {
		t.Errorf("slow status = %q, want %q", slow.Status, "running")
	}
	if slow.CurrentJob == nil {
		t.Fatal("slow currentJob is nil")
	}
	if slow.CurrentJob.JobID != "slow-1" {
		t.Errorf("slow jobId = %q, want %q", slow.CurrentJob.JobID, "slow-1")
	}

	// Fast should be idle with lastJob.
	fast, ok := body.Tasks["fast"]
	if !ok {
		t.Fatal("tasks[\"fast\"] not found")
	}
	if fast.Status != "idle" {
		t.Errorf("fast status = %q, want %q", fast.Status, "idle")
	}
	if fast.LastJob == nil {
		t.Fatal("fast lastJob is nil")
	}
	if fast.LastJob.JobID != "fast-1" {
		t.Errorf("fast lastJob.jobId = %q, want %q", fast.LastJob.JobID, "fast-1")
	}

	close(release)
	<-done
}

func TestHealthHandler_IdleWithLastJob_Failed(t *testing.T) {
	failTask := TaskFunc(func(ctx *Run, params Params) Result {
		return Fail("db_error", "connection refused")
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"fail": failTask}
	f.slots = map[string]*taskSlot{"fail": {}}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp1, err := postRun(srv, "fail", `{"jobId":"fail-job-1"}`)
	if err != nil {
		t.Fatalf("POST /run/fail: %v", err)
	}
	_ = resp1.Body.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["fail"]
	if task.Status != "idle" {
		t.Errorf("status = %q, want %q", task.Status, "idle")
	}
	if task.LastJob == nil {
		t.Fatal("lastJob is nil, want non-nil")
	}
	if task.LastJob.JobID != "fail-job-1" {
		t.Errorf("lastJob.jobId = %q, want %q", task.LastJob.JobID, "fail-job-1")
	}
	if task.LastJob.Success {
		t.Error("lastJob.success = true, want false")
	}
}

func TestHealthHandler_IdleWithLastJob_Async(t *testing.T) {
	done := make(chan struct{})
	echoTask := TaskFunc(func(ctx *Run, params Params) Result {
		return OK("async done")
	})
	f := testServerWith(t,
		Task("echo", echoTask),
		WithCallbackAllowlist("https://hooks.example.com"),
	)
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		defer close(done)
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp1, err := postRun(srv, "echo", `{"callbackUrl":"https://hooks.example.com/cb","jobId":"async-health-1"}`)
	if err != nil {
		t.Fatalf("POST /run/echo: %v", err)
	}
	_ = resp1.Body.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async delivery")
	}

	// Wait for slot release.
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

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["echo"]
	if task.Status != "idle" {
		t.Errorf("status = %q, want %q", task.Status, "idle")
	}
	if task.LastJob == nil {
		t.Fatal("lastJob is nil, want non-nil")
	}
	if task.LastJob.JobID != "async-health-1" {
		t.Errorf("lastJob.jobId = %q, want %q", task.LastJob.JobID, "async-health-1")
	}
	if !task.LastJob.Success {
		t.Error("lastJob.success = false, want true")
	}
}

func TestHealthHandler_IdleWithLastJob_SyncTimeout(t *testing.T) {
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		select {
		case <-ctx.Done():
			return Fail("cancelled", "context cancelled")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})
	f := testServer(t)
	f.tasks = map[string]TaskFunc{"slow": slowTask}
	f.slots = map[string]*taskSlot{"slow": {}}
	f.syncTimeout = 50 * time.Millisecond
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Trigger sync timeout.
	resp1, err := postRun(srv, "slow", `{"jobId":"timeout-health-1"}`)
	if err != nil {
		t.Fatalf("POST /run/slow: %v", err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp1.StatusCode)
	}

	// Wait for cleanup goroutine to release slot.
	slot := f.slots["slow"]
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

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["slow"]
	if task.Status != "idle" {
		t.Errorf("status = %q, want %q", task.Status, "idle")
	}
	if task.LastJob == nil {
		t.Fatal("lastJob is nil, want non-nil")
	}
	if task.LastJob.JobID != "timeout-health-1" {
		t.Errorf("lastJob.jobId = %q, want %q", task.LastJob.JobID, "timeout-health-1")
	}
	if task.LastJob.Duration == "" {
		t.Error("lastJob.duration is empty, want non-empty")
	}
}

func TestHealthHandler_Draining(t *testing.T) {
	f := testServer(t)
	f.startedAt = time.Now()
	f.draining.Store(true)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Status != "draining" {
		t.Errorf("status = %q, want %q", body.Status, "draining")
	}
}

func testServerWithReadiness(t *testing.T, fn func() error) *Server {
	t.Helper()
	return testServerWith(t, Task("echo", dummyTask), WithReadiness(fn))
}

func TestReadyz_DrainingReturns503(t *testing.T) {
	f := testServer(t)
	f.draining.Store(true)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "server is shutting down" {
		t.Errorf("error = %q, want %q", body.Error, "server is shutting down")
	}
}

func TestReadyz_DrainingWithCustomCheck(t *testing.T) {
	f := testServerWithReadiness(t, func() error { return nil }) // healthy check
	f.draining.Store(true)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (draining takes precedence over healthy custom check)", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "server is shutting down" {
		t.Errorf("error = %q, want %q", body.Error, "server is shutting down")
	}
}

func TestReadyz_NotDraining_CustomCheckPasses(t *testing.T) {
	f := testServerWithReadiness(t, func() error { return nil })
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want %q", ct, "application/json")
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("status = %q, want %q", body["status"], "ok")
	}
}

func TestHealthHandler_TaskDescription(t *testing.T) {
	f := testServerWith(t, Task("echo", dummyTask).Description("Echoes back params"))
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["echo"]
	if task.Description != "Echoes back params" {
		t.Errorf("description = %q, want %q", task.Description, "Echoes back params")
	}
}

func TestHealthHandler_TaskExample(t *testing.T) {
	f := testServerWith(t, Task("echo", dummyTask).Example(map[string]any{"count": 10}))
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["echo"]
	if task.Example == nil {
		t.Fatal("example is nil")
	}
	// JSON numbers decode as float64.
	if task.Example["count"] != float64(10) {
		t.Errorf("example[count] = %v, want 10", task.Example["count"])
	}
}

func TestHealthHandler_RunningTaskWithMetadata(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slowTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-release
		return OK("done")
	})
	f := testServerWith(t,
		Task("slow", slowTask).
			Description("A slow task").
			Example(map[string]any{"delay": 5}),
	)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	done := make(chan struct{})
	go func() {
		_, _ = postRun(srv, "slow", `{"jobId":"meta-run-1"}`)
		close(done)
	}()
	<-started

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["slow"]
	if task.Status != "running" {
		t.Errorf("status = %q, want %q", task.Status, "running")
	}
	if task.Description != "A slow task" {
		t.Errorf("description = %q, want %q", task.Description, "A slow task")
	}
	if task.Example == nil {
		t.Fatal("example is nil, want non-nil for running task with metadata")
	}
	if task.Example["delay"] != float64(5) {
		t.Errorf("example[delay] = %v, want 5", task.Example["delay"])
	}

	close(release)
	<-done
}

func TestHealthHandler_ResultsInResponse(t *testing.T) {
	successTask := TaskFunc(func(_ *Run, _ Params) Result {
		return OK("it worked")
	})
	failTask := TaskFunc(func(_ *Run, _ Params) Result {
		return Fail("bad_input", "missing field")
	})

	f := testServer(t)
	f.tasks = map[string]TaskFunc{"task": successTask}
	f.slots = map[string]*taskSlot{"task": {}}
	f.history = newResultHistory(f.slots, f.resultHistorySize, f.taskResultSizes)
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Run success.
	r1, _ := postRun(srv, "task", `{"jobId":"j1"}`)
	closeBody(r1)

	// Swap to fail task and run again.
	f.tasks["task"] = failTask
	r2, _ := postRun(srv, "task", `{"jobId":"j2"}`)
	closeBody(r2)

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	task := body.Tasks["task"]
	if len(task.Results) != 2 {
		t.Fatalf("results length = %d, want 2", len(task.Results))
	}

	// Newest first.
	newest := task.Results[0]
	if newest.JobID != "j2" {
		t.Errorf("results[0].jobId = %q, want %q", newest.JobID, "j2")
	}
	if newest.Success {
		t.Error("results[0].success = true, want false")
	}
	if newest.Error == nil {
		t.Fatal("results[0].error is nil, want non-nil")
	}
	if newest.Error.Code != "bad_input" {
		t.Errorf("results[0].error.code = %q, want %q", newest.Error.Code, "bad_input")
	}
	if newest.Message != "missing field" {
		t.Errorf("results[0].message = %q, want %q", newest.Message, "missing field")
	}

	oldest := task.Results[1]
	if oldest.JobID != "j1" {
		t.Errorf("results[1].jobId = %q, want %q", oldest.JobID, "j1")
	}
	if !oldest.Success {
		t.Error("results[1].success = false, want true")
	}
	if oldest.Message != "it worked" {
		t.Errorf("results[1].message = %q, want %q", oldest.Message, "it worked")
	}
	if oldest.Error != nil {
		t.Errorf("results[1].error = %+v, want nil", oldest.Error)
	}
	if oldest.Finished == "" {
		t.Error("results[1].finished is empty, want RFC3339 timestamp")
	}
}

func TestHealthHandler_ResultsOmittedWhenEmpty(t *testing.T) {
	f := testServer(t)
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var raw map[string]json.RawMessage
	_ = json.NewDecoder(resp.Body).Decode(&raw)

	var tasks map[string]json.RawMessage
	_ = json.Unmarshal(raw["tasks"], &tasks)

	var taskMap map[string]json.RawMessage
	_ = json.Unmarshal(tasks["echo"], &taskMap)

	if _, ok := taskMap["results"]; ok {
		t.Error("results field should be omitted when no results exist")
	}
}

func TestHealthHandler_ResultsWhileRunning(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	task := TaskFunc(func(ctx *Run, _ Params) Result {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return OK("first done")
		}
		close(started)
		<-release
		return OK("second done")
	})

	f := testServer(t)
	f.tasks = map[string]TaskFunc{"task": task}
	f.slots = map[string]*taskSlot{"task": {}}
	f.history = newResultHistory(f.slots, f.resultHistorySize, f.taskResultSizes)
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Complete one run first (instant).
	r1, _ := postRun(srv, "task", `{"jobId":"j1"}`)
	closeBody(r1)

	// Start a second run that blocks.
	go func() {
		r2, _ := postRun(srv, "task", `{"jobId":"j2"}`)
		closeBody(r2)
	}()
	<-started

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	var body healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	tk := body.Tasks["task"]
	if tk.Status != "running" {
		t.Errorf("status = %q, want %q", tk.Status, "running")
	}
	if tk.CurrentJob == nil {
		t.Error("currentJob is nil, want non-nil")
	}
	if len(tk.Results) != 1 {
		t.Errorf("results length = %d, want 1 (from prior completed run)", len(tk.Results))
	}

	close(release)
}

func TestHealthHandler_TaskNoMetadata_OmitsFields(t *testing.T) {
	f := testServer(t)
	f.startedAt = time.Now()
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := getHealth(srv)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer closeBody(resp)

	// Decode as raw JSON to verify omitempty.
	var raw map[string]json.RawMessage
	_ = json.NewDecoder(resp.Body).Decode(&raw)

	var tasks map[string]json.RawMessage
	_ = json.Unmarshal(raw["tasks"], &tasks)

	var taskMap map[string]json.RawMessage
	_ = json.Unmarshal(tasks["echo"], &taskMap)

	if _, ok := taskMap["description"]; ok {
		t.Error("description field should be omitted when not set")
	}
	if _, ok := taskMap["example"]; ok {
		t.Error("example field should be omitted when not set")
	}
}
