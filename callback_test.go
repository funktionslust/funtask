package funtask

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDeadLetterPath_FormatsCorrectly(t *testing.T) {
	d := newDeliverer("/data/dead-letters", slog.Default(), 5, 30*time.Second)
	got, err := d.deadLetterPath("abc-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "/data/dead-letters/abc-123.json"
	if got != want {
		t.Errorf("deadLetterPath = %q, want %q", got, want)
	}
}

func TestDeadLetterPath_RejectsPathTraversal(t *testing.T) {
	d := newDeliverer("/data/dead-letters", slog.Default(), 5, 30*time.Second)
	_, err := d.deadLetterPath("../../etc/passwd")
	if err == nil {
		t.Fatal("expected error for path traversal, got nil")
	}
}

func TestWriteDeadLetter_PathTraversal(t *testing.T) {
	called := false
	d := newDeliverer("/data/dead-letters", slog.Default(), 5, 30*time.Second)
	d.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		called = true
		return nil
	}
	err := d.writeDeadLetter("../../etc/passwd", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for path traversal, got nil")
	}
	if called {
		t.Error("writeFunc should not be called for path traversal jobID")
	}
}

func TestWriteDeadLetter_Success(t *testing.T) {
	var capturedPath string
	var capturedData []byte
	var capturedPerm os.FileMode
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		capturedPath = name
		capturedData = data
		capturedPerm = perm
		return nil
	}

	payload := []byte(`{"jobId":"job-1","success":true}`)
	if err := d.writeDeadLetter("job-1", payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedPath != "/tmp/dl/job-1.json" {
		t.Errorf("path = %q, want %q", capturedPath, "/tmp/dl/job-1.json")
	}
	if string(capturedData) != string(payload) {
		t.Errorf("data = %q, want %q", capturedData, payload)
	}
	if capturedPerm != 0o600 {
		t.Errorf("perm = %o, want 600", capturedPerm)
	}
}

func TestWriteDeadLetter_Error(t *testing.T) {
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.writeFunc = func(name string, data []byte, perm os.FileMode) error {
		return fmt.Errorf("disk full")
	}

	err := d.writeDeadLetter("job-1", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "disk full" {
		t.Errorf("error = %q, want %q", err.Error(), "disk full")
	}
}

func TestRemoveDeadLetter_Success(t *testing.T) {
	var capturedPath string
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.removeFunc = func(name string) error {
		capturedPath = name
		return nil
	}

	if err := d.removeDeadLetter("job-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedPath != "/tmp/dl/job-1.json" {
		t.Errorf("path = %q, want %q", capturedPath, "/tmp/dl/job-1.json")
	}
}

func TestRemoveDeadLetter_Error(t *testing.T) {
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.removeFunc = func(name string) error {
		return fmt.Errorf("permission denied")
	}

	err := d.removeDeadLetter("job-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "permission denied" {
		t.Errorf("error = %q, want %q", err.Error(), "permission denied")
	}
}

func TestMarshalResult_Success(t *testing.T) {
	resp := jobResponse{
		JobID:    "job-1",
		Success:  true,
		Message:  "done",
		Duration: "10ms",
		Data:     map[string]any{"key": "val"},
	}
	data, got := marshalResult(resp)
	if got.JobID != "job-1" {
		t.Errorf("jobId = %q, want %q", got.JobID, "job-1")
	}
	if !got.Success {
		t.Error("success = false, want true")
	}

	var parsed jobResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.JobID != "job-1" {
		t.Errorf("parsed jobId = %q, want %q", parsed.JobID, "job-1")
	}
	if parsed.Data["key"] != "val" {
		t.Errorf("parsed data.key = %v, want %q", parsed.Data["key"], "val")
	}
}

func TestMarshalResult_UnmarshalableValue(t *testing.T) {
	resp := jobResponse{
		JobID:    "job-2",
		Success:  true,
		Duration: "5ms",
		Data:     map[string]any{"ch": make(chan int)},
	}
	data, got := marshalResult(resp)
	if got.Success {
		t.Error("success = true, want false (serialization failed)")
	}
	if got.Error == nil {
		t.Fatal("error is nil, want non-nil")
	}
	if got.Error.Code != "result_serialization_failed" {
		t.Errorf("error.code = %q, want %q", got.Error.Code, "result_serialization_failed")
	}
	if got.Error.Message != "result could not be serialized" {
		t.Errorf("error.message = %q, want %q", got.Error.Message, "result could not be serialized")
	}

	var parsed jobResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("fallback data should be valid JSON: %v", err)
	}
	if parsed.Error == nil || parsed.Error.Code != "result_serialization_failed" {
		t.Errorf("parsed fallback error.code = %v, want %q", parsed.Error, "result_serialization_failed")
	}
}

func TestMarshalResult_Fallback_PreservesFields(t *testing.T) {
	resp := jobResponse{
		JobID:    "preserve-me",
		Success:  true,
		Duration: "42ms",
		Data:     map[string]any{"bad": make(chan int)},
	}
	_, got := marshalResult(resp)
	if got.JobID != "preserve-me" {
		t.Errorf("jobId = %q, want %q", got.JobID, "preserve-me")
	}
	if got.Duration != "42ms" {
		t.Errorf("duration = %q, want %q", got.Duration, "42ms")
	}
	if got.Data != nil {
		t.Errorf("data = %v, want nil (original data excluded from fallback)", got.Data)
	}
}

func TestNewDeliverer_Defaults(t *testing.T) {
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	if d.writeFunc == nil {
		t.Error("writeFunc is nil, want non-nil default")
	}
	if d.removeFunc == nil {
		t.Error("removeFunc is nil, want non-nil default")
	}
	if d.postFunc == nil {
		t.Error("postFunc is nil, want non-nil default")
	}
	if d.sleepFunc == nil {
		t.Error("sleepFunc is nil, want non-nil default")
	}
	if d.deadLetterDir != "/tmp/dl" {
		t.Errorf("deadLetterDir = %q, want %q", d.deadLetterDir, "/tmp/dl")
	}
	if d.retries != 5 {
		t.Errorf("retries = %d, want 5", d.retries)
	}
}

func TestDeliver_Success_FirstAttempt(t *testing.T) {
	var removedPath string
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		return 200, nil
	}
	d.sleepFunc = func(time.Duration) {}
	d.removeFunc = func(name string) error {
		removedPath = name
		return nil
	}

	err := d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if removedPath != "/tmp/dl/job-1.json" {
		t.Errorf("removed path = %q, want %q", removedPath, "/tmp/dl/job-1.json")
	}
}

func TestDeliver_Success_AfterRetry(t *testing.T) {
	var attempts int
	var removeCalled bool
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		attempts++
		if attempts < 3 {
			return 0, fmt.Errorf("connection refused")
		}
		return 200, nil
	}
	d.sleepFunc = func(time.Duration) {}
	d.removeFunc = func(name string) error {
		removeCalled = true
		return nil
	}

	err := d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if !removeCalled {
		t.Error("removeFunc not called after successful delivery")
	}
}

func TestDeliver_AllAttemptsExhausted(t *testing.T) {
	var attempts int
	var removeCalled bool
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		attempts++
		return 0, fmt.Errorf("connection refused")
	}
	d.sleepFunc = func(time.Duration) {}
	d.removeFunc = func(name string) error {
		removeCalled = true
		return nil
	}

	err := d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if attempts != 5 {
		t.Errorf("attempts = %d, want 5", attempts)
	}
	if removeCalled {
		t.Error("removeFunc should not be called when all attempts fail")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error should wrap last attempt error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "job-1") {
		t.Errorf("error should contain jobID, got: %v", err)
	}
}

func TestDeliver_Non2xxTreatedAsFailure(t *testing.T) {
	var attempts int
	d := newDeliverer("/tmp/dl", slog.Default(), 3, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		attempts++
		return 500, nil
	}
	d.sleepFunc = func(time.Duration) {}
	d.removeFunc = func(name string) error { return nil }

	err := d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for non-2xx, got nil")
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestDeliver_RemoveFailure_NonFatal(t *testing.T) {
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		return 200, nil
	}
	d.sleepFunc = func(time.Duration) {}
	d.removeFunc = func(name string) error {
		return fmt.Errorf("permission denied")
	}

	err := d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err != nil {
		t.Fatalf("deliver should succeed even if remove fails: %v", err)
	}
}

func TestDeliver_RetrySchedule(t *testing.T) {
	var sleeps []time.Duration
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		return 0, fmt.Errorf("fail")
	}
	d.sleepFunc = func(dur time.Duration) {
		sleeps = append(sleeps, dur)
	}
	d.removeFunc = func(name string) error { return nil }

	_ = d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))

	// 5 attempts = 4 sleeps (no sleep before first attempt)
	if len(sleeps) != 4 {
		t.Fatalf("sleep count = %d, want 4", len(sleeps))
	}
	want := []time.Duration{1 * time.Second, 5 * time.Second, 15 * time.Second, 60 * time.Second}
	for i, w := range want {
		if sleeps[i] != w {
			t.Errorf("sleep[%d] = %v, want %v", i, sleeps[i], w)
		}
	}
}

func TestDeliver_NoSleepOnFirstAttempt(t *testing.T) {
	var sleepCalled bool
	d := newDeliverer("/tmp/dl", slog.Default(), 1, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		return 200, nil
	}
	d.sleepFunc = func(time.Duration) {
		sleepCalled = true
	}
	d.removeFunc = func(name string) error { return nil }

	err := d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sleepCalled {
		t.Error("sleepFunc should not be called when first attempt succeeds")
	}
}

func TestDeliver_RetryScheduleClamping(t *testing.T) {
	var sleeps []time.Duration
	d := newDeliverer("/tmp/dl", slog.Default(), 7, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		return 0, fmt.Errorf("fail")
	}
	d.sleepFunc = func(dur time.Duration) {
		sleeps = append(sleeps, dur)
	}
	d.removeFunc = func(name string) error { return nil }

	_ = d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))

	// 7 attempts = 6 sleeps; entries beyond retrySchedule clamp to last (120s)
	if len(sleeps) != 6 {
		t.Fatalf("sleep count = %d, want 6", len(sleeps))
	}
	want := []time.Duration{
		1 * time.Second, 5 * time.Second, 15 * time.Second,
		60 * time.Second, 120 * time.Second, 120 * time.Second,
	}
	for i, w := range want {
		if sleeps[i] != w {
			t.Errorf("sleep[%d] = %v, want %v", i, sleeps[i], w)
		}
	}
}

func TestDeliver_DefaultPostFunc_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := newDeliverer("/tmp/dl", slog.Default(), 1, 100*time.Millisecond)
	d.removeFunc = func(name string) error { return nil }

	// Use default postFunc — do NOT override
	err := d.deliver("job-1", srv.URL, []byte(`{}`))
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestDeliverOnce_Success(t *testing.T) {
	var postCalls int
	var removedPath string
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		postCalls++
		return 200, nil
	}
	d.removeFunc = func(name string) error {
		removedPath = name
		return nil
	}

	err := d.deliverOnce("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if postCalls != 1 {
		t.Errorf("postFunc calls = %d, want 1 (single attempt)", postCalls)
	}
	if removedPath != "/tmp/dl/job-1.json" {
		t.Errorf("removed path = %q, want %q", removedPath, "/tmp/dl/job-1.json")
	}
}

func TestDeliverOnce_Failure_NoRetry(t *testing.T) {
	var postCalls int
	var removeCalled bool
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		postCalls++
		return 0, fmt.Errorf("connection refused")
	}
	d.removeFunc = func(name string) error {
		removeCalled = true
		return nil
	}

	err := d.deliverOnce("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if postCalls != 1 {
		t.Errorf("postFunc calls = %d, want 1 (no retries)", postCalls)
	}
	if removeCalled {
		t.Error("removeFunc should not be called when delivery fails")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error should wrap cause, got: %v", err)
	}
	if !strings.Contains(err.Error(), "job-1") {
		t.Errorf("error should contain jobID, got: %v", err)
	}
}

func TestDeliverOnce_Non2xx_NoRetry(t *testing.T) {
	var postCalls int
	var removeCalled bool
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		postCalls++
		return 503, nil
	}
	d.removeFunc = func(name string) error {
		removeCalled = true
		return nil
	}

	err := d.deliverOnce("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for non-2xx, got nil")
	}
	if postCalls != 1 {
		t.Errorf("postFunc calls = %d, want 1 (no retries)", postCalls)
	}
	if removeCalled {
		t.Error("removeFunc should not be called for non-2xx")
	}
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Errorf("error should contain HTTP status, got: %v", err)
	}
}

func TestDeliverOnce_RemoveFailure_NonFatal(t *testing.T) {
	d := newDeliverer("/tmp/dl", slog.Default(), 5, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		return 200, nil
	}
	d.removeFunc = func(name string) error {
		return fmt.Errorf("permission denied")
	}

	err := d.deliverOnce("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err != nil {
		t.Fatalf("deliverOnce should succeed even if remove fails: %v", err)
	}
}

func TestDeliver_Non2xxWrapsStatusInError(t *testing.T) {
	d := newDeliverer("/tmp/dl", slog.Default(), 1, 30*time.Second)
	d.postFunc = func(url string, data []byte) (int, error) {
		return 503, nil
	}
	d.sleepFunc = func(time.Duration) {}
	d.removeFunc = func(name string) error { return nil }

	err := d.deliver("job-1", "https://hooks.example.com/callback", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Errorf("error should wrap HTTP status, got: %v", err)
	}
}
