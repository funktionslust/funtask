package funtask

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// fatalRecorder is a mock testing.TB that records Fatalf calls
// instead of aborting the test goroutine.
type fatalRecorder struct {
	testing.TB
	fatalCalled bool
	fatalMsg    string
}

func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.fatalCalled = true
	f.fatalMsg = fmt.Sprintf(format, args...)
}

func (f *fatalRecorder) Helper() {}

func TestTestRun(t *testing.T) {
	run := TestRun(t)
	if run == nil {
		t.Fatal("TestRun returned nil")
	}
	if run.Context == nil {
		t.Fatal("Context is nil")
	}
	if run.Steps() != nil {
		t.Errorf("Steps() = %v, want nil", run.Steps())
	}
}

func TestTestRun_StepsRecorded(t *testing.T) {
	run := TestRun(t)
	run.Step("Fetching data")
	run.Progress(1, 10, "Processing %d", 1)
	run.Step("Done")

	steps := run.Steps()
	want := []string{"Fetching data", "Processing 1", "Done"}
	if len(steps) != len(want) {
		t.Fatalf("Steps() length = %d, want %d", len(steps), len(want))
	}
	for i, s := range steps {
		if s != want[i] {
			t.Errorf("Steps()[%d] = %q, want %q", i, s, want[i])
		}
	}
}

func TestTestRun_ContextNotCancelled(t *testing.T) {
	run := TestRun(t)
	select {
	case <-run.Done():
		t.Error("TestRun context should not be cancelled")
	default:
		// expected
	}
	if run.Err() != nil {
		t.Errorf("Err() = %v, want nil", run.Err())
	}
}

func TestTestRunWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	run := TestRunWithContext(t, ctx)

	if run == nil {
		t.Fatal("TestRunWithContext returned nil")
	}

	// Context should not be cancelled yet
	select {
	case <-run.Done():
		t.Error("context should not be cancelled yet")
	default:
		// expected
	}

	// Cancel and verify propagation
	cancel()
	select {
	case <-run.Done():
		// expected
	default:
		t.Error("Done() should be closed after cancel")
	}
	if run.Err() != context.Canceled {
		t.Errorf("Err() = %v, want context.Canceled", run.Err())
	}
}

func TestTestRunWithContext_PreCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	run := TestRunWithContext(t, ctx)
	select {
	case <-run.Done():
		// expected
	default:
		t.Error("Done() should be closed for pre-cancelled context")
	}
	if run.Err() != context.Canceled {
		t.Errorf("Err() = %v, want context.Canceled", run.Err())
	}
}

func TestTestParams(t *testing.T) {
	p := TestParams(t, map[string]any{
		"name":    "test",
		"count":   float64(42),
		"verbose": true,
	})

	s, err := p.String("name")
	if err != nil {
		t.Fatalf("String error: %v", err)
	}
	if s != "test" {
		t.Errorf("String = %q, want %q", s, "test")
	}

	i, err := p.Int("count")
	if err != nil {
		t.Fatalf("Int error: %v", err)
	}
	if i != 42 {
		t.Errorf("Int = %d, want 42", i)
	}

	b, err := p.Bool("verbose")
	if err != nil {
		t.Fatalf("Bool error: %v", err)
	}
	if !b {
		t.Error("Bool = false, want true")
	}
}

func TestTestParams_ValidTypes(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"string", "hello"},
		{"float64", float64(3.14)},
		{"float64 zero", float64(0)},
		{"bool", true},
		{"nil", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := TestParams(t, map[string]any{"key": tt.value})
			if p.Raw() == nil {
				t.Error("Raw() should not be nil")
			}
		})
	}
}

func TestTestParams_InvalidTypes(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"channel", make(chan int)},
		{"function", func() {}},
		{"bare int", 42},
		{"slice", []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &fatalRecorder{TB: t}
			TestParams(rec, map[string]any{"key": tt.value})
			if !rec.fatalCalled {
				t.Errorf("expected Fatalf for type %T, but it was not called", tt.value)
			}
			wantKey := `"key"`
			if rec.fatalMsg == "" || !strings.Contains(rec.fatalMsg, wantKey) {
				t.Errorf("fatalMsg = %q, want it to mention key name %s", rec.fatalMsg, wantKey)
			}
		})
	}
}

func TestTestParams_EmptyMap(t *testing.T) {
	p := TestParams(t, map[string]any{})
	if p.Raw() == nil {
		t.Error("Raw() should not be nil for empty map")
	}
	if len(p.Raw()) != 0 {
		t.Errorf("Raw() length = %d, want 0", len(p.Raw()))
	}
}

func TestTestParamsRaw(t *testing.T) {
	p := TestParamsRaw(map[string]any{
		"limit": 42, // bare int, not float64
	})
	// Raw int should be stored as-is
	raw := p.Raw()
	if raw["limit"] != 42 {
		t.Errorf("Raw()[limit] = %v, want 42", raw["limit"])
	}
}

func TestTestParamsRaw_InvalidTypes(t *testing.T) {
	ch := make(chan int)
	fn := func() {}

	p := TestParamsRaw(map[string]any{
		"channel":  ch,
		"function": fn,
		"bare_int": 99,
	})

	// Should store everything without panicking
	raw := p.Raw()
	if raw["channel"] != ch {
		t.Error("channel not stored")
	}
	if raw["function"] == nil {
		t.Error("function not stored")
	}
	if raw["bare_int"] != 99 {
		t.Errorf("bare_int = %v, want 99", raw["bare_int"])
	}
}

func TestTestParamsRaw_EmptyMap(t *testing.T) {
	p := TestParamsRaw(map[string]any{})
	if p.Raw() == nil {
		t.Error("Raw() should not be nil for empty map")
	}
	if len(p.Raw()) != 0 {
		t.Errorf("Raw() length = %d, want 0", len(p.Raw()))
	}
}

func TestTestParamsRaw_NilMap(t *testing.T) {
	p := TestParamsRaw(nil)
	if p.Raw() != nil {
		t.Errorf("Raw() = %v, want nil for nil map", p.Raw())
	}
}

func TestSteps(t *testing.T) {
	t.Run("nil before calls", func(t *testing.T) {
		run := TestRun(t)
		if run.Steps() != nil {
			t.Errorf("Steps() = %v, want nil", run.Steps())
		}
	})

	t.Run("records Step calls", func(t *testing.T) {
		run := TestRun(t)
		run.Step("first")
		run.Step("second")

		steps := run.Steps()
		if len(steps) != 2 {
			t.Fatalf("Steps() length = %d, want 2", len(steps))
		}
		if steps[0] != "first" {
			t.Errorf("Steps()[0] = %q, want %q", steps[0], "first")
		}
		if steps[1] != "second" {
			t.Errorf("Steps()[1] = %q, want %q", steps[1], "second")
		}
	})

	t.Run("records Progress calls", func(t *testing.T) {
		run := TestRun(t)
		run.Progress(1, 5, "Item %d", 1)
		run.Progress(2, 5, "Item %d", 2)

		steps := run.Steps()
		if len(steps) != 2 {
			t.Fatalf("Steps() length = %d, want 2", len(steps))
		}
		if steps[0] != "Item 1" {
			t.Errorf("Steps()[0] = %q, want %q", steps[0], "Item 1")
		}
		if steps[1] != "Item 2" {
			t.Errorf("Steps()[1] = %q, want %q", steps[1], "Item 2")
		}
	})

	t.Run("mixed Step and Progress", func(t *testing.T) {
		run := TestRun(t)
		run.Step("Starting")
		run.Progress(1, 3, "Processing")
		run.Step("Finishing")

		steps := run.Steps()
		want := []string{"Starting", "Processing", "Finishing"}
		if len(steps) != len(want) {
			t.Fatalf("Steps() length = %d, want %d", len(steps), len(want))
		}
		for i, s := range steps {
			if s != want[i] {
				t.Errorf("Steps()[%d] = %q, want %q", i, s, want[i])
			}
		}
	})
}
