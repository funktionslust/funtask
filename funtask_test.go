package funtask

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

var dummyTask TaskFunc = func(ctx *Run, params Params) Result {
	return OK("done")
}

func validServer() *Server {
	return New("test-server",
		WithTask("task1", dummyTask),
		WithAuthToken("secret"),
		WithDeadLetterDir("/tmp/dead-letters"),
	)
}

func TestNew(t *testing.T) {
	f := New("my-server")
	if f == nil {
		t.Fatal("New returned nil")
	}
}

func TestNew_WithName(t *testing.T) {
	f := New("order-server")
	if f.name != "order-server" {
		t.Errorf("name = %q, want %q", f.name, "order-server")
	}
}

func TestNew_EmptyName(t *testing.T) {
	f := New("")
	if f.name != "" {
		t.Errorf("name = %q, want empty string", f.name)
	}
}

func TestNew_WithTask(t *testing.T) {
	f := New("n", WithTask("sync", dummyTask))
	if _, ok := f.tasks["sync"]; !ok {
		t.Error("task 'sync' not registered")
	}
}

func TestNew_MultipleTasks(t *testing.T) {
	f := New("n",
		WithTask("a", dummyTask),
		WithTask("b", dummyTask),
		WithTask("c", dummyTask),
	)
	if len(f.tasks) != 3 {
		t.Errorf("tasks count = %d, want 3", len(f.tasks))
	}
	for _, name := range []string{"a", "b", "c"} {
		if _, ok := f.tasks[name]; !ok {
			t.Errorf("task %q not registered", name)
		}
	}
}

func TestNew_EnvDefaults(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "env-token")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "/env/dead-letters")

	f := New("n")
	if f.authToken != "env-token" {
		t.Errorf("authToken = %q, want %q", f.authToken, "env-token")
	}
	if f.deadLetterDir != "/env/dead-letters" {
		t.Errorf("deadLetterDir = %q, want %q", f.deadLetterDir, "/env/dead-letters")
	}
}

func TestNew_OptionOverridesEnv(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "env-token")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "/env/dead-letters")

	f := New("n",
		WithAuthToken("opt-token"),
		WithDeadLetterDir("/opt/dead-letters"),
	)
	if f.authToken != "opt-token" {
		t.Errorf("authToken = %q, want %q", f.authToken, "opt-token")
	}
	if f.deadLetterDir != "/opt/dead-letters" {
		t.Errorf("deadLetterDir = %q, want %q", f.deadLetterDir, "/opt/dead-letters")
	}
}

func TestNew_Defaults(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("n")
	if f.syncTimeout != 2*time.Minute {
		t.Errorf("syncTimeout = %v, want 2m", f.syncTimeout)
	}
	if f.shutdownTimeout != 30*time.Second {
		t.Errorf("shutdownTimeout = %v, want 30s", f.shutdownTimeout)
	}
	if f.callbackRetries != 5 {
		t.Errorf("callbackRetries = %d, want 5", f.callbackRetries)
	}
	if f.callbackTimeout != 30*time.Second {
		t.Errorf("callbackTimeout = %v, want 30s", f.callbackTimeout)
	}
	if f.maxDuration != 0 {
		t.Errorf("maxDuration = %v, want 0", f.maxDuration)
	}
	if f.tasks == nil {
		t.Error("tasks map is nil, want initialized")
	}
	if f.readiness != nil {
		t.Error("readiness should be nil by default")
	}
	if f.callbackAllowlist != nil {
		t.Errorf("callbackAllowlist = %v, want nil by default", f.callbackAllowlist)
	}
	if f.logger != nil {
		t.Error("logger should be nil by default (set during ListenAndServe)")
	}
	if f.server != nil {
		t.Error("server should be nil by default (set during ListenAndServe)")
	}
	if f.slots != nil {
		t.Error("slots should be nil by default (set during ListenAndServe)")
	}
}

func TestListenAndServe_NoTasks(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("n")
	err := f.ListenAndServe(":0")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no tasks registered") {
		t.Errorf("error = %q, want it to contain %q", err, "no tasks registered")
	}
}

func TestListenAndServe_NoAuthToken(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("n",
		WithTask("t", dummyTask),
		WithDeadLetterDir("/tmp/dl"),
	)
	err := f.ListenAndServe(":0")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "auth token required") {
		t.Errorf("error = %q, want it to contain %q", err, "auth token required")
	}
	if strings.Contains(err.Error(), "no tasks registered") {
		t.Error("error should not contain 'no tasks registered' when task is provided")
	}
	if strings.Contains(err.Error(), "dead letter directory required") {
		t.Error("error should not contain 'dead letter directory required' when dir is provided")
	}
}

func TestListenAndServe_NoDeadLetterDir(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("n",
		WithTask("t", dummyTask),
		WithAuthToken("secret"),
	)
	err := f.ListenAndServe(":0")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "dead letter directory required") {
		t.Errorf("error = %q, want it to contain %q", err, "dead letter directory required")
	}
	if strings.Contains(err.Error(), "no tasks registered") {
		t.Error("error should not contain 'no tasks registered' when task is provided")
	}
	if strings.Contains(err.Error(), "auth token required") {
		t.Error("error should not contain 'auth token required' when token is provided")
	}
}

func TestListenAndServe_MultipleErrors(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("bad-server")
	err := f.ListenAndServe(":0")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	for _, want := range []string{"no tasks registered", "auth token required", "dead letter directory required"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error = %q, want it to contain %q", msg, want)
		}
	}
	if !strings.Contains(msg, `"bad-server"`) {
		t.Errorf("error = %q, want it to contain node name %q", msg, "bad-server")
	}
}

func TestListenAndServe_ValidConfig(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")
	f := validServer()
	// validate() is called first by ListenAndServe; test that directly
	// since ListenAndServe now blocks (starts HTTP server).
	if err := f.validate(); err != nil {
		t.Errorf("validate = %v, want nil", err)
	}
}

func TestWithTask(t *testing.T) {
	f := New("n", WithTask("process", dummyTask))
	fn, ok := f.tasks["process"]
	if !ok {
		t.Fatal("task 'process' not found")
	}
	if fn == nil {
		t.Error("task function is nil")
	}
}

func TestWithTask_OverwritesExisting(t *testing.T) {
	first := TaskFunc(func(ctx *Run, params Params) Result { return OK("first") })
	second := TaskFunc(func(ctx *Run, params Params) Result { return OK("second") })
	f := New("n", WithTask("sync", first), WithTask("sync", second))
	if len(f.tasks) != 1 {
		t.Errorf("tasks count = %d, want 1 (overwrite)", len(f.tasks))
	}
	result := f.tasks["sync"](nil, Params{})
	if result.Message != "second" {
		t.Errorf("task result = %q, want %q (second should overwrite first)", result.Message, "second")
	}
}

func TestWithAuthToken(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	f := New("n", WithAuthToken("my-token"))
	if f.authToken != "my-token" {
		t.Errorf("authToken = %q, want %q", f.authToken, "my-token")
	}
}

func TestWithDeadLetterDir(t *testing.T) {
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")
	f := New("n", WithDeadLetterDir("/data/dl"))
	if f.deadLetterDir != "/data/dl" {
		t.Errorf("deadLetterDir = %q, want %q", f.deadLetterDir, "/data/dl")
	}
}

func TestWithMaxDuration(t *testing.T) {
	f := New("n", WithMaxDuration(5*time.Minute))
	if f.maxDuration != 5*time.Minute {
		t.Errorf("maxDuration = %v, want 5m", f.maxDuration)
	}
}

func TestWithSyncTimeout(t *testing.T) {
	f := New("n", WithSyncTimeout(10*time.Second))
	if f.syncTimeout != 10*time.Second {
		t.Errorf("syncTimeout = %v, want 10s", f.syncTimeout)
	}
}

func TestWithShutdownTimeout(t *testing.T) {
	f := New("n", WithShutdownTimeout(1*time.Minute))
	if f.shutdownTimeout != 1*time.Minute {
		t.Errorf("shutdownTimeout = %v, want 1m", f.shutdownTimeout)
	}
}

func TestWithCallbackRetries(t *testing.T) {
	f := New("n", WithCallbackRetries(3))
	if f.callbackRetries != 3 {
		t.Errorf("callbackRetries = %d, want 3", f.callbackRetries)
	}
}

func TestWithCallbackTimeout(t *testing.T) {
	f := New("n", WithCallbackTimeout(15*time.Second))
	if f.callbackTimeout != 15*time.Second {
		t.Errorf("callbackTimeout = %v, want 15s", f.callbackTimeout)
	}
}

func TestWithCallbackAllowlist(t *testing.T) {
	f := New("n", WithCallbackAllowlist("https://a.com/", "https://b.com/"))
	if len(f.callbackAllowlist) != 2 {
		t.Fatalf("callbackAllowlist length = %d, want 2", len(f.callbackAllowlist))
	}
	if f.callbackAllowlist[0] != "https://a.com/" {
		t.Errorf("callbackAllowlist[0] = %q, want %q", f.callbackAllowlist[0], "https://a.com/")
	}
	if f.callbackAllowlist[1] != "https://b.com/" {
		t.Errorf("callbackAllowlist[1] = %q, want %q", f.callbackAllowlist[1], "https://b.com/")
	}
}

func TestWithReadiness(t *testing.T) {
	called := false
	fn := func() error {
		called = true
		return nil
	}
	f := New("n", WithReadiness(fn))
	if f.readiness == nil {
		t.Fatal("readiness function is nil")
	}
	_ = f.readiness()
	if !called {
		t.Error("readiness function was not called")
	}
}

func TestOptionFunctions_NoValidation(t *testing.T) {
	// All With* functions should accept any value without panicking
	// including zero values, negative values, empty strings, nil
	f := New("n",
		WithTask("", dummyTask),
		WithAuthToken(""),
		WithDeadLetterDir(""),
		WithMaxDuration(0),
		WithMaxDuration(-1*time.Second),
		WithSyncTimeout(0),
		WithShutdownTimeout(0),
		WithCallbackRetries(0),
		WithCallbackRetries(-1),
		WithCallbackTimeout(0),
		WithCallbackAllowlist(),
		WithReadiness(nil),
	)
	if f == nil {
		t.Fatal("New returned nil with edge-case options")
	}
	if len(f.callbackAllowlist) != 0 {
		t.Errorf("callbackAllowlist = %v, want empty after WithCallbackAllowlist()", f.callbackAllowlist)
	}
}

// --- SIGTERM Handling & Draining State Tests ---

func TestCancelAllJobs(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

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

	f := New("test-server",
		WithTask("slow", slowTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"slow": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start task in background.
	done := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/slow", strings.NewReader(`{"jobId":"cancel-all-1"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(done)
	}()
	<-started

	// Call cancelAllJobs directly.
	f.cancelAllJobs()

	// Verify task received cancellation.
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not receive cancellation from cancelAllJobs")
	}

	<-done
}

func TestCancelAllJobs_MultipleTasks(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	startedA := make(chan struct{})
	cancelledA := make(chan struct{})
	taskA := TaskFunc(func(ctx *Run, params Params) Result {
		close(startedA)
		select {
		case <-ctx.Done():
			close(cancelledA)
			return Fail("cancelled", "stopped")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})

	startedB := make(chan struct{})
	cancelledB := make(chan struct{})
	taskB := TaskFunc(func(ctx *Run, params Params) Result {
		close(startedB)
		select {
		case <-ctx.Done():
			close(cancelledB)
			return Fail("cancelled", "stopped")
		case <-time.After(10 * time.Second):
			return OK("done")
		}
	})

	f := New("test-server",
		WithTask("a", taskA),
		WithTask("b", taskB),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"a": {}, "b": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start both tasks in background.
	doneA := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/a", strings.NewReader(`{"jobId":"multi-a"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(doneA)
	}()
	doneB := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/b", strings.NewReader(`{"jobId":"multi-b"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(doneB)
	}()
	<-startedA
	<-startedB

	// Cancel all jobs.
	f.cancelAllJobs()

	// Verify both tasks received cancellation.
	select {
	case <-cancelledA:
	case <-time.After(2 * time.Second):
		t.Fatal("task A did not receive cancellation from cancelAllJobs")
	}
	select {
	case <-cancelledB:
	case <-time.After(2 * time.Second):
		t.Fatal("task B did not receive cancellation from cancelAllJobs")
	}

	<-doneA
	<-doneB
}

func TestShutdown_SetsDrainingAndCancels(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

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

	f := New("test-server",
		WithTask("slow", slowTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithShutdownTimeout(5*time.Second),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"slow": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}
	// Set f.server to a dummy — Shutdown on a never-started server is a safe no-op.
	f.server = &http.Server{}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start task in background.
	done := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/slow", strings.NewReader(`{"jobId":"shutdown-1"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(done)
	}()
	<-started

	// Call shutdown() directly.
	if err := f.shutdown(); err != nil {
		t.Fatalf("shutdown() = %v, want nil", err)
	}

	// Verify draining is set.
	if !f.draining.Load() {
		t.Error("draining = false after shutdown, want true")
	}

	// Verify task received cancellation.
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not receive cancellation from shutdown")
	}

	<-done
}

func TestListenAndServe_GracefulShutdown(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("shutdown-server",
		WithTask("echo", dummyTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithShutdownTimeout(5*time.Second),
	)

	// Find a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- f.ListenAndServe(addr)
	}()

	// Wait for server to be ready by polling.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Trigger shutdown via stopCh.
	close(f.stopCh)

	// Verify ListenAndServe returns nil.
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ListenAndServe = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServe did not return after stopCh closed")
	}

	// Verify draining is set.
	if !f.draining.Load() {
		t.Error("draining = false after graceful shutdown, want true")
	}
}

func TestListenAndServe_GracefulShutdown_CancelsJobs(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

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

	f := New("shutdown-server",
		WithTask("slow", slowTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithShutdownTimeout(5*time.Second),
	)

	// Find a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- f.ListenAndServe(addr)
	}()

	// Wait for server to be ready.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Start a job.
	jobDone := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("POST", "http://"+addr+"/run/slow", strings.NewReader(`{"jobId":"shutdown-job-1"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(jobDone)
	}()
	<-started

	// Trigger shutdown.
	close(f.stopCh)

	// Verify the in-flight job received cancellation.
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight job did not receive cancellation during shutdown")
	}

	// Verify ListenAndServe returns.
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ListenAndServe = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServe did not return after shutdown")
	}

	if !f.draining.Load() {
		t.Error("draining = false after shutdown, want true")
	}
}

// --- Shutdown Completion & Timeout Tests ---

func TestShutdown_WaitsForJobs(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	started := make(chan struct{})
	taskFunc := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		// Complete after a short delay (simulates fast task responding to cancellation).
		select {
		case <-ctx.Done():
			return Fail("cancelled", "stopped")
		case <-time.After(100 * time.Millisecond):
			return OK("done")
		}
	})

	f := New("test-server",
		WithTask("fast", taskFunc),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithShutdownTimeout(5*time.Second),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"fast": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}
	f.server = &http.Server{}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start task.
	done := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/fast", strings.NewReader(`{"jobId":"wait-1"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(done)
	}()
	<-started

	// Shutdown — should wait for job to complete, not timeout.
	shutdownStart := time.Now()
	if err := f.shutdown(); err != nil {
		t.Fatalf("shutdown() = %v, want nil", err)
	}
	shutdownDuration := time.Since(shutdownStart)

	// Shutdown should complete well within the 5s timeout.
	if shutdownDuration > 3*time.Second {
		t.Errorf("shutdown took %v, expected < 3s (job should complete quickly)", shutdownDuration)
	}

	<-done
}

func TestShutdown_WorkerShutdownForStuckJob(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	started := make(chan struct{})
	// Task that ignores cancellation — simulates a stuck task.
	stuckTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-time.After(30 * time.Second)
		return OK("should not reach here")
	})

	f := New("test-server",
		WithTask("stuck", stuckTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithShutdownTimeout(200*time.Millisecond),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"stuck": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}

	// Capture dead letter writes.
	var writtenJobID string
	var writtenData []byte
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		writtenJobID = name
		writtenData = data
		return nil
	}
	f.server = &http.Server{}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start stuck task.
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/stuck", strings.NewReader(`{"jobId":"stuck-1"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()
	<-started

	// Shutdown with short timeout — should trigger worker_shutdown.
	if err := f.shutdown(); err != nil {
		t.Fatalf("shutdown() = %v, want nil", err)
	}

	// Verify dead letter was written with worker_shutdown.
	if writtenJobID == "" {
		t.Fatal("no dead letter file written for stuck job")
	}
	if !strings.Contains(writtenJobID, "stuck-1") {
		t.Errorf("dead letter path = %q, want it to contain %q", writtenJobID, "stuck-1")
	}

	var resp jobResponse
	if err := json.Unmarshal(writtenData, &resp); err != nil {
		t.Fatalf("dead letter JSON parse: %v", err)
	}
	if resp.Success {
		t.Error("worker_shutdown result should not be success")
	}
	if resp.Error == nil {
		t.Fatal("worker_shutdown result should have error")
	}
	if resp.Error.Code != "worker_shutdown" {
		t.Errorf("error code = %q, want %q", resp.Error.Code, "worker_shutdown")
	}
	if resp.Error.Message != "server is shutting down" {
		t.Errorf("error message = %q, want %q", resp.Error.Message, "server is shutting down")
	}
}

func TestShutdown_WorkerShutdownWithCallback(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	started := make(chan struct{})
	stuckTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-time.After(30 * time.Second)
		return OK("should not reach here")
	})

	f := New("test-server",
		WithTask("stuck", stuckTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithShutdownTimeout(200*time.Millisecond),
		WithCallbackAllowlist("https://hooks.example.com"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"stuck": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.sleepFunc = func(time.Duration) {}

	// Capture callback POST.
	var callbackData []byte
	var callbackCalls int
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		callbackCalls++
		callbackData = data
		return 200, nil
	}
	f.server = &http.Server{}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start stuck async task with callback.
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/stuck",
			strings.NewReader(`{"jobId":"stuck-cb-1","callbackUrl":"https://hooks.example.com/webhook/123"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()
	<-started

	// Shutdown — should trigger worker_shutdown with callback.
	if err := f.shutdown(); err != nil {
		t.Fatalf("shutdown() = %v, want nil", err)
	}

	// Verify callback was called with worker_shutdown.
	if callbackCalls == 0 {
		t.Fatal("deliverOnce was not called for stuck async job")
	}
	if callbackData == nil {
		t.Fatal("callback data is nil")
	}

	var resp jobResponse
	if err := json.Unmarshal(callbackData, &resp); err != nil {
		t.Fatalf("callback JSON parse: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != "worker_shutdown" {
		t.Errorf("callback error code = %v, want %q", resp.Error, "worker_shutdown")
	}
}

func TestShutdown_TaskDoneSkipsWorkerShutdown(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("test-server",
		WithTask("task1", dummyTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"task1": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	var writeCalled bool
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		writeCalled = true
		return nil
	}
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}

	// Simulate a slot where task function returned (taskDone=true) but goroutine is in delivery.
	slot := f.slots["task1"]
	slot.running = true
	slot.jobID = "done-1"
	slot.taskDone = true
	slot.startedAt = time.Now()

	// Call handleStuckJobs directly.
	f.handleStuckJobs()

	// Verify NO dead letter was written — taskDone=true means task is not stuck.
	if writeCalled {
		t.Error("dead letter was written for taskDone=true slot, want skip")
	}
}

func TestHandleStuckJobs_MultipleSlots(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("test-server",
		WithTask("stuck", dummyTask),
		WithTask("idle", dummyTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{
		"stuck": {},
		"idle":  {},
	}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}

	// Track writes.
	var writtenPaths []string
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		writtenPaths = append(writtenPaths, name)
		return nil
	}

	// Set up: stuck task is running, idle task is not.
	stuckSlot := f.slots["stuck"]
	stuckSlot.running = true
	stuckSlot.jobID = "stuck-multi-1"
	stuckSlot.startedAt = time.Now()
	stuckSlot.taskDone = false

	// idle slot stays default (running=false).

	f.handleStuckJobs()

	// Verify: only the stuck slot got a dead letter.
	if len(writtenPaths) != 1 {
		t.Fatalf("dead letter writes = %d, want 1", len(writtenPaths))
	}
	if !strings.Contains(writtenPaths[0], "stuck-multi-1") {
		t.Errorf("dead letter path = %q, want it to contain %q", writtenPaths[0], "stuck-multi-1")
	}
}

func TestAsyncDelivery_DrainingUsesSingleAttempt(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	started := make(chan struct{})
	release := make(chan struct{})
	blockingTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-release
		return OK("done")
	})

	f := New("test-server",
		WithTask("async", blockingTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithCallbackAllowlist("https://hooks.example.com"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"async": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error { return nil }
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.sleepFunc = func(time.Duration) {}

	// Count POST calls.
	var postCalls int
	deliveryDone := make(chan struct{})
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		postCalls++
		close(deliveryDone)
		return 200, nil
	}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Start async job.
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/async",
			strings.NewReader(`{"jobId":"drain-1","callbackUrl":"https://hooks.example.com/webhook/1"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()
	<-started

	// Set draining BEFORE releasing the task.
	f.draining.Store(true)

	// Release the task — it will complete and use deliverOnce (single attempt).
	close(release)

	// Wait for delivery to complete.
	select {
	case <-deliveryDone:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery did not complete within timeout")
	}

	// Verify exactly 1 POST call (deliverOnce, not deliver with retries).
	if postCalls != 1 {
		t.Errorf("POST calls = %d, want 1 (single attempt via deliverOnce)", postCalls)
	}
}

func TestShutdown_SyncTimeoutStuckJob(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	started := make(chan struct{})
	// Task that ignores cancellation — simulates stuck task behind sync timeout.
	stuckTask := TaskFunc(func(ctx *Run, params Params) Result {
		close(started)
		<-time.After(30 * time.Second)
		return OK("should not reach here")
	})

	f := New("test-server",
		WithTask("stuck", stuckTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
		WithSyncTimeout(100*time.Millisecond),
		WithShutdownTimeout(300*time.Millisecond),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"stuck": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.postFunc = func(url string, data []byte) (int, error) { return 200, nil }
	f.deliverer.sleepFunc = func(time.Duration) {}

	// Capture dead letter writes.
	var writtenJobID string
	var writtenData []byte
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		writtenJobID = name
		writtenData = data
		return nil
	}
	f.server = &http.Server{}
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	// Send sync request (no callbackUrl) — will hit sync timeout.
	syncDone := make(chan struct{})
	go func() {
		req, _ := http.NewRequest("POST", srv.URL+"/run/stuck",
			strings.NewReader(`{"jobId":"sync-stuck-1"}`))
		req.Header.Set("Authorization", "Bearer test-secret")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(syncDone)
	}()
	<-started

	// Wait for sync timeout to fire so the background goroutine takes over.
	<-syncDone

	// Now the sync-timeout background goroutine is blocking on <-done.
	// Shutdown with short timeout should trigger worker_shutdown.
	if err := f.shutdown(); err != nil {
		t.Fatalf("shutdown() = %v, want nil", err)
	}

	// Verify dead letter was written with worker_shutdown.
	if writtenJobID == "" {
		t.Fatal("no dead letter written for sync-timeout stuck job")
	}
	if !strings.Contains(writtenJobID, "sync-stuck-1") {
		t.Errorf("dead letter path = %q, want it to contain %q", writtenJobID, "sync-stuck-1")
	}

	var resp jobResponse
	if err := json.Unmarshal(writtenData, &resp); err != nil {
		t.Fatalf("dead letter JSON parse: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != "worker_shutdown" {
		t.Errorf("error code = %v, want %q", resp.Error, "worker_shutdown")
	}
}

func TestHandleStuckJobs_DeadLetterWriteFailure(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")

	f := New("test-server",
		WithTask("stuck", dummyTask),
		WithAuthToken("test-secret"),
		WithDeadLetterDir("/tmp/test-dl"),
	)
	f.logger = slog.With("server", f.name)
	f.slots = map[string]*taskSlot{"stuck": {}}
	f.cache = &resultCache{entries: make(map[string]*resultCacheEntry)}
	f.deliverer = newDeliverer(f.deadLetterDir, f.logger, f.callbackRetries, f.callbackTimeout)

	// Dead letter write fails.
	f.deliverer.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		return fmt.Errorf("disk full")
	}
	var postCalled bool
	f.deliverer.postFunc = func(url string, data []byte) (int, error) {
		postCalled = true
		return 200, nil
	}
	f.deliverer.removeFunc = func(name string) error { return nil }
	f.deliverer.sleepFunc = func(time.Duration) {}

	// Set up stuck slot with callback.
	slot := f.slots["stuck"]
	slot.running = true
	slot.jobID = "write-fail-1"
	slot.startedAt = time.Now()
	slot.taskDone = false
	slot.callbackURL = "https://hooks.example.com/webhook/1"

	f.handleStuckJobs()

	// Verify: callback was NOT called because dead letter write failed.
	if postCalled {
		t.Error("postFunc should not be called when dead letter write fails")
	}
}
