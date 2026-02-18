package funtask

import (
	"errors"
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

func TestFailWrap(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		err      error
		msg      string
		wantMsg  string
		wantCode string
	}{
		{
			name:     "wraps error with code and message",
			code:     "api_error",
			err:      errors.New("connection refused"),
			msg:      "API failed",
			wantMsg:  "API failed",
			wantCode: "api_error",
		},
		{
			name:     "raw error not in result",
			code:     "db_error",
			err:      errors.New("pq: unique constraint violated"),
			msg:      "Could not save record",
			wantMsg:  "Could not save record",
			wantCode: "db_error",
		},
		{
			name:     "nil error does not panic",
			code:     "unknown",
			err:      nil,
			msg:      "Something failed",
			wantMsg:  "Something failed",
			wantCode: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := FailWrap(tt.code, tt.err, tt.msg)
			if r.Success {
				t.Error("expected Success to be false")
			}
			if r.ErrorCode != tt.wantCode {
				t.Errorf("ErrorCode = %q, want %q", r.ErrorCode, tt.wantCode)
			}
			if r.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", r.Message, tt.wantMsg)
			}
			// Verify raw error is NOT stored anywhere in the Result
			if r.Data != nil {
				for k, v := range r.Data {
					if str, ok := v.(string); ok && tt.err != nil && str == tt.err.Error() {
						t.Errorf("raw error found in Data[%q]", k)
					}
				}
			}
		})
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
