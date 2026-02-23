// Package funtask provides a Go library for building HTTP task workers.
//
// A server hosts one or more named tasks that an orchestrator can trigger via HTTP.
// Task authors write functions that receive execution context and parameters,
// and return structured results.
package funtask

import (
	"errors"
	"fmt"
	"maps"
)

// ErrorCoder is implemented by errors that carry a machine-readable code.
// FailFromError uses this to extract the error code automatically.
type ErrorCoder interface {
	ErrorCode() string
}

// UserMessager is implemented by errors that carry a user-facing message
// separate from the technical error string. FailFromError uses this to
// extract a safe message for the result.
type UserMessager interface {
	UserMessage() string
}

// Result represents the outcome of a task execution. It carries a success
// flag, a human-readable message, an optional machine-readable error code,
// optional key-value data, and the last reported step for diagnostic context.
type Result struct {
	Success   bool
	Message   string
	ErrorCode string
	Data      map[string]any
	LastStep  string
	cause     error // unexported — logged server-side, never serialized
}

// OK creates a success result with a formatted message.
func OK(format string, args ...any) Result {
	return Result{
		Success: true,
		Message: fmt.Sprintf(format, args...),
	}
}

// Fail creates a failure result with a machine-readable error code and a
// formatted human-readable message.
func Fail(code string, format string, args ...any) Result {
	return Result{
		Success:   false,
		ErrorCode: code,
		Message:   fmt.Sprintf(format, args...),
	}
}

// FailFromError creates a failure result from a Go error. If the error
// implements ErrorCoder, its ErrorCode() is used; otherwise fallbackCode
// applies. If the error implements UserMessager, its UserMessage() becomes
// the result message; otherwise err.Error() is used. The original error
// is stored as the cause for server-side logging but never serialized.
func FailFromError(err error, fallbackCode string) Result {
	if err == nil {
		return Result{
			Success:   false,
			ErrorCode: fallbackCode,
		}
	}

	code := fallbackCode
	var ec ErrorCoder
	if errors.As(err, &ec) {
		code = ec.ErrorCode()
	}

	msg := err.Error()
	var um UserMessager
	if errors.As(err, &um) {
		msg = um.UserMessage()
	}

	return Result{
		Success:   false,
		ErrorCode: code,
		Message:   msg,
		cause:     err,
	}
}

// WithCause returns a copy of the result with the given error attached as
// the underlying cause. The cause is logged server-side but never included
// in callback payloads or JSON serialization.
func (r Result) WithCause(err error) Result {
	r.cause = err
	return r
}

// WithData returns a copy of the result with the given key-value pair added
// to its data map. Multiple calls can be chained. The original result is
// never modified.
//
// Note: after JSON round-trip (e.g. callback payloads), numeric values
// become float64 per encoding/json conventions.
func (r Result) WithData(key string, value any) Result {
	newData := make(map[string]any, len(r.Data)+1)
	maps.Copy(newData, r.Data)
	newData[key] = value
	r.Data = newData
	return r
}
