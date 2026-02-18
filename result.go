// Package funtask provides a Go library for building HTTP task workers.
//
// A server hosts one or more named tasks that an orchestrator can trigger via HTTP.
// Task authors write functions that receive execution context and parameters,
// and return structured results.
package funtask

import (
	"fmt"
	"maps"
)

// Result represents the outcome of a task execution. It carries a success
// flag, a human-readable message, an optional machine-readable error code,
// optional key-value data, and the last reported step for diagnostic context.
type Result struct {
	Success   bool
	Message   string
	ErrorCode string
	Data      map[string]any
	LastStep  string
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

// FailWrap creates a failure result from a Go error. The error is not
// included in the result, preventing internal details from leaking to
// callers. Log the error in your task function if needed.
func FailWrap(code string, err error, msg string) Result {
	return Result{
		Success:   false,
		ErrorCode: code,
		Message:   msg,
	}
}

// WithData returns a copy of the result with the given key-value pair added
// to its data map. Multiple calls can be chained. The original result is
// never modified.
func (r Result) WithData(key string, value any) Result {
	newData := make(map[string]any, len(r.Data)+1)
	maps.Copy(newData, r.Data)
	newData[key] = value
	r.Data = newData
	return r
}
