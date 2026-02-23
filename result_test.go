package funtask

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestOK(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		args    []any
		wantMsg string
	}{
		{
			name:    "simple message",
			format:  "done",
			wantMsg: "done",
		},
		{
			name:    "formatted message",
			format:  "processed %d items",
			args:    []any{42},
			wantMsg: "processed 42 items",
		},
		{
			name:    "multiple format args",
			format:  "%s: %d of %d",
			args:    []any{"sync", 10, 50},
			wantMsg: "sync: 10 of 50",
		},
		{
			name:    "empty format string",
			format:  "",
			wantMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := OK(tt.format, tt.args...)
			if !r.Success {
				t.Error("expected Success to be true")
			}
			if r.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", r.Message, tt.wantMsg)
			}
			if r.ErrorCode != "" {
				t.Errorf("ErrorCode = %q, want empty", r.ErrorCode)
			}
		})
	}
}

func TestFail(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		format   string
		args     []any
		wantMsg  string
		wantCode string
	}{
		{
			name:     "simple failure",
			code:     "invalid_params",
			format:   "missing %s",
			args:     []any{"name"},
			wantMsg:  "missing name",
			wantCode: "invalid_params",
		},
		{
			name:     "no format args",
			code:     "not_found",
			format:   "resource not found",
			wantMsg:  "resource not found",
			wantCode: "not_found",
		},
		{
			name:     "empty code and message",
			code:     "",
			format:   "",
			wantMsg:  "",
			wantCode: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Fail(tt.code, tt.format, tt.args...)
			if r.Success {
				t.Error("expected Success to be false")
			}
			if r.ErrorCode != tt.wantCode {
				t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, tt.wantCode)
			}
			if r.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", r.Message, tt.wantMsg)
			}
		})
	}
}

// businessError implements ErrorCoder and UserMessager for testing.
type businessError struct {
	code    string
	message string
	detail  string
}

func (e businessError) Error() string       { return e.detail }
func (e businessError) ErrorCode() string   { return e.code }
func (e businessError) UserMessage() string { return e.message }

func TestFailFromError_PlainError(t *testing.T) {
	err := errors.New("connection refused")
	r := FailFromError(err, "internal_error")

	if r.Success {
		t.Error("expected Success to be false")
	}
	if r.ErrorCode != "internal_error" {
		t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, "internal_error")
	}
	if r.Message != "connection refused" {
		t.Errorf("Message = %q, want %q", r.Message, "connection refused")
	}
	if r.cause != err {
		t.Error("cause not stored")
	}
}

func TestFailFromError_ErrorCoder(t *testing.T) {
	err := businessError{code: "order_failed", message: "order could not be placed", detail: "db: constraint violation"}
	r := FailFromError(err, "internal_error")

	if r.ErrorCode != "order_failed" {
		t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, "order_failed")
	}
	if r.Message != "order could not be placed" {
		t.Errorf("Message = %q, want %q", r.Message, "order could not be placed")
	}
	if r.cause == nil {
		t.Error("cause not stored")
	}
}

func TestFailFromError_ErrorCoderOnly(t *testing.T) {
	err := &onlyCodedError{code: "rate_limited", msg: "too many requests"}
	r := FailFromError(err, "fallback")

	if r.ErrorCode != "rate_limited" {
		t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, "rate_limited")
	}
	// No UserMessager → falls back to err.Error()
	if r.Message != "too many requests" {
		t.Errorf("Message = %q, want %q", r.Message, "too many requests")
	}
}

// onlyCodedError implements ErrorCoder but not UserMessager.
type onlyCodedError struct {
	code string
	msg  string
}

func (e *onlyCodedError) Error() string     { return e.msg }
func (e *onlyCodedError) ErrorCode() string { return e.code }

func TestFailFromError_WrappedError(t *testing.T) {
	inner := businessError{code: "api_error", message: "API unreachable", detail: "connection timeout"}
	wrapped := fmt.Errorf("sync failed: %w", inner)
	r := FailFromError(wrapped, "internal_error")

	// errors.As should unwrap to find ErrorCoder
	if r.ErrorCode != "api_error" {
		t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, "api_error")
	}
	if r.Message != "API unreachable" {
		t.Errorf("Message = %q, want %q", r.Message, "API unreachable")
	}
}

func TestWithCause(t *testing.T) {
	err := errors.New("db connection lost")
	r := Fail("db_error", "database unavailable").WithCause(err)

	if r.cause != err {
		t.Error("cause not stored")
	}
	if r.ErrorCode != "db_error" {
		t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, "db_error")
	}
}

func TestWithCause_Nil(t *testing.T) {
	r := Fail("err", "failed").WithCause(nil)
	if r.cause != nil {
		t.Error("cause should be nil")
	}
}

func TestWithCause_DoesNotMutateOriginal(t *testing.T) {
	base := Fail("err", "failed")
	_ = base.WithCause(errors.New("something"))
	if base.cause != nil {
		t.Error("WithCause mutated the original result")
	}
}

func TestFailFromError_NilError(t *testing.T) {
	r := FailFromError(nil, "unknown")
	if r.Success {
		t.Error("expected Success to be false")
	}
	if r.ErrorCode != "unknown" {
		t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, "unknown")
	}
	if r.Message != "" {
		t.Errorf("Message = %q, want empty", r.Message)
	}
	if r.cause != nil {
		t.Error("cause should be nil")
	}
}

func TestWithCause_NotSerializedToJSON(t *testing.T) {
	r := Fail("db_error", "connection lost").WithCause(errors.New("secret internal detail"))
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	s := string(data)
	if strings.Contains(s, "secret internal detail") {
		t.Errorf("cause leaked into JSON: %s", s)
	}
	if strings.Contains(s, "cause") {
		t.Errorf("cause field present in JSON: %s", s)
	}
}

func TestWithCause_ChainsWithData(t *testing.T) {
	err := errors.New("timeout")
	r := Fail("timeout", "request timed out").
		WithCause(err).
		WithData("retries", 3)

	if r.cause != err {
		t.Error("cause lost after WithData chain")
	}
	if r.Data["retries"] != 3 {
		t.Errorf("Data[retries] = %v, want 3", r.Data["retries"])
	}
}

func TestWithData(t *testing.T) {
	t.Run("single key-value", func(t *testing.T) {
		r := OK("done").WithData("count", 42)
		if r.Data == nil {
			t.Fatal("Data is nil")
		}
		if r.Data["count"] != 42 {
			t.Errorf("Data[count] = %v, want 42", r.Data["count"])
		}
	})

	t.Run("chained key-values", func(t *testing.T) {
		r := OK("done").
			WithData("count", 42).
			WithData("source", "api")
		if r.Data == nil {
			t.Fatal("Data is nil")
		}
		if r.Data["count"] != 42 {
			t.Errorf("Data[count] = %v, want 42", r.Data["count"])
		}
		if r.Data["source"] != "api" {
			t.Errorf("Data[source] = %v, want api", r.Data["source"])
		}
	})

	t.Run("on failure result", func(t *testing.T) {
		r := Fail("err", "failed").WithData("detail", "extra")
		if r.Data == nil {
			t.Fatal("Data is nil")
		}
		if r.Data["detail"] != "extra" {
			t.Errorf("Data[detail] = %v, want extra", r.Data["detail"])
		}
	})

	t.Run("does not mutate original", func(t *testing.T) {
		base := OK("done").WithData("a", 1)
		_ = base.WithData("b", 2)
		if _, exists := base.Data["b"]; exists {
			t.Error("WithData mutated the original result's Data map")
		}
		if base.Data["a"] != 1 {
			t.Errorf("original Data[a] = %v, want 1", base.Data["a"])
		}
	})

	t.Run("key overwrite", func(t *testing.T) {
		r := OK("done").
			WithData("key", "first").
			WithData("key", "second")
		if r.Data["key"] != "second" {
			t.Errorf("Data[key] = %v, want second", r.Data["key"])
		}
	})
}

func TestWithData_LazyInit(t *testing.T) {
	r := OK("done")
	if r.Data != nil {
		t.Error("Data should be nil before WithData is called")
	}
	r = r.WithData("key", "val")
	if r.Data == nil {
		t.Error("Data should be initialized after WithData")
	}
}

func TestPartialSuccess(t *testing.T) {
	r := OK("batch complete").
		WithData("succeeded", 47).
		WithData("failed", 1)

	if !r.Success {
		t.Error("expected Success to be true for partial success")
	}
	if r.Data["succeeded"] != 47 {
		t.Errorf("Data[succeeded] = %v, want 47", r.Data["succeeded"])
	}
	if r.Data["failed"] != 1 {
		t.Errorf("Data[failed] = %v, want 1", r.Data["failed"])
	}
}

func TestLastStep(t *testing.T) {
	t.Run("settable on error result", func(t *testing.T) {
		r := Fail("timeout", "timed out")
		r.LastStep = "Fetching products"
		if r.LastStep != "Fetching products" {
			t.Errorf("LastStep = %q, want %q", r.LastStep, "Fetching products")
		}
	})

	t.Run("settable on success result", func(t *testing.T) {
		r := OK("done")
		r.LastStep = "Final step"
		if r.LastStep != "Final step" {
			t.Errorf("LastStep = %q, want %q", r.LastStep, "Final step")
		}
	})

	t.Run("default is empty", func(t *testing.T) {
		r := OK("done")
		if r.LastStep != "" {
			t.Errorf("LastStep = %q, want empty", r.LastStep)
		}
	})
}
