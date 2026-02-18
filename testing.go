package funtask

import (
	"context"
	"testing"
	"time"
)

// TestRun returns a *Run backed by the test's context.
// Cancelled when the test's deadline expires (if set via -timeout).
// Steps are recorded and logged via t.Log for debugging. Call
// TestRun(...).Steps() to retrieve the recorded steps for assertions.
func TestRun(t testing.TB) *Run {
	t.Helper()
	ctx := context.Background()
	if d, ok := t.(interface{ Deadline() (time.Time, bool) }); ok {
		if deadline, hasDeadline := d.Deadline(); hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			t.Cleanup(cancel)
		}
	}
	return &Run{Context: ctx, logf: t.Logf}
}

// TestRunWithContext returns a *Run backed by a custom context.
// Use this to test cancellation handling.
func TestRunWithContext(t testing.TB, ctx context.Context) *Run {
	t.Helper()
	return &Run{Context: ctx, logf: t.Logf}
}

// TestParams creates a Params value from a map literal.
// Fails the test immediately if the map contains types that Params
// cannot represent (e.g., channels, functions).
func TestParams(t testing.TB, m map[string]any) Params {
	t.Helper()
	for k, v := range m {
		switch v.(type) {
		case string, float64, bool, nil:
			// valid JSON-representable types
		default:
			t.Fatalf("TestParams: key %q has unsupported type %T", k, v)
		}
	}
	return Params{m: m}
}

// TestParamsRaw creates a Params value without validation.
// Use this to test parameter validation paths with bad inputs
// (e.g., wrong types, missing keys).
func TestParamsRaw(m map[string]any) Params {
	return Params{m: m}
}

// Steps returns all step descriptions recorded by Step() and
// Progress() calls, in order. Use this in tests to assert that a task
// function reported expected progress.
func (r *Run) Steps() []string {
	return r.steps
}
