package funtask

import (
	"context"
	"testing"
	"time"
)

func TestTaskFunc(t *testing.T) {
	var fn TaskFunc = func(ctx *Run, params Params) Result {
		return OK("done")
	}
	result := fn(&Run{Context: context.Background()}, Params{})
	if !result.Success {
		t.Error("expected success")
	}
	if result.Message != "done" {
		t.Errorf("Message = %q, want %q", result.Message, "done")
	}
}

func TestRun_Step(t *testing.T) {
	run := &Run{Context: context.Background()}
	run.Step("Fetching products")

	if run.step != "Fetching products" {
		t.Errorf("step = %q, want %q", run.step, "Fetching products")
	}
	if run.stepAt.IsZero() {
		t.Error("stepAt should be set after Step()")
	}
}

func TestRun_Step_OverwritesPrevious(t *testing.T) {
	run := &Run{Context: context.Background()}
	run.Step("Step one")
	run.Step("Step two")

	if run.step != "Step two" {
		t.Errorf("step = %q, want %q", run.step, "Step two")
	}
	if len(run.steps) != 2 {
		t.Fatalf("steps length = %d, want 2", len(run.steps))
	}
	if run.steps[0] != "Step one" {
		t.Errorf("steps[0] = %q, want %q", run.steps[0], "Step one")
	}
	if run.steps[1] != "Step two" {
		t.Errorf("steps[1] = %q, want %q", run.steps[1], "Step two")
	}
}

func TestRun_Step_FormatsMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		args []any
		want string
	}{
		{"no args", "Fetching data", nil, "Fetching data"},
		{"with args", "Processing %d of %d", []any{3, 10}, "Processing 3 of 10"},
		{"string arg", "Loading %s", []any{"users"}, "Loading users"},
		{"empty string", "", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &Run{Context: context.Background()}
			run.Step(tt.msg, tt.args...)
			if run.step != tt.want {
				t.Errorf("step = %q, want %q", run.step, tt.want)
			}
		})
	}
}

func TestRun_Step_PreservesProgressFields(t *testing.T) {
	run := &Run{Context: context.Background()}
	run.Progress(42, 100, "Processing")
	run.Step("Next phase")

	if run.current != 42 {
		t.Errorf("current = %d, want 42 (preserved after Step)", run.current)
	}
	if run.total != 100 {
		t.Errorf("total = %d, want 100 (preserved after Step)", run.total)
	}
	if run.percent != 42 {
		t.Errorf("percent = %f, want 42 (preserved after Step)", run.percent)
	}
	if run.step != "Next phase" {
		t.Errorf("step = %q, want %q", run.step, "Next phase")
	}
}

func TestRun_Progress(t *testing.T) {
	run := &Run{Context: context.Background()}
	run.Progress(42, 100, "Processing")

	if run.step != "Processing" {
		t.Errorf("step = %q, want %q", run.step, "Processing")
	}
	if run.current != 42 {
		t.Errorf("current = %d, want 42", run.current)
	}
	if run.total != 100 {
		t.Errorf("total = %d, want 100", run.total)
	}
	if run.percent != 42 {
		t.Errorf("percent = %f, want 42", run.percent)
	}
	if run.stepAt.IsZero() {
		t.Error("stepAt should be set after Progress()")
	}
	if len(run.steps) != 1 || run.steps[0] != "Processing" {
		t.Errorf("steps = %v, want [Processing]", run.steps)
	}
}

func TestRun_Progress_ComputesPercent(t *testing.T) {
	tests := []struct {
		name        string
		current     int
		total       int
		wantPercent float64
	}{
		{"42 of 100", 42, 100, 42},
		{"0 of 0", 0, 0, 0},
		{"1 of 3", 1, 3, float64(1) * 100 / float64(3)},
		{"100 of 100", 100, 100, 100},
		{"0 of 100", 0, 100, 0},
		{"negative total", 5, -1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &Run{Context: context.Background()}
			run.Progress(tt.current, tt.total, "msg")
			if run.current != tt.current {
				t.Errorf("current = %d, want %d", run.current, tt.current)
			}
			if run.total != tt.total {
				t.Errorf("total = %d, want %d", run.total, tt.total)
			}
			if run.percent != tt.wantPercent {
				t.Errorf("percent = %f, want %f", run.percent, tt.wantPercent)
			}
		})
	}
}

func TestRun_Progress_FormatsMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		args []any
		want string
	}{
		{"no args", "Processing", nil, "Processing"},
		{"with args", "Item %d of %d", []any{3, 10}, "Item 3 of 10"},
		{"string arg", "Loading %s", []any{"users"}, "Loading users"},
		{"empty string", "", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &Run{Context: context.Background()}
			run.Progress(1, 10, tt.msg, tt.args...)
			if run.step != tt.want {
				t.Errorf("step = %q, want %q", run.step, tt.want)
			}
		})
	}
}

func TestRun_Progress_OverwritesPrevious(t *testing.T) {
	run := &Run{Context: context.Background()}
	run.Progress(1, 10, "First")
	run.Progress(7, 50, "Second")

	if run.current != 7 {
		t.Errorf("current = %d, want 7", run.current)
	}
	if run.total != 50 {
		t.Errorf("total = %d, want 50", run.total)
	}
	if run.percent != 14 {
		t.Errorf("percent = %f, want 14", run.percent)
	}
	if run.step != "Second" {
		t.Errorf("step = %q, want %q", run.step, "Second")
	}
}

func TestRun_Progress_UpdatesStep(t *testing.T) {
	run := &Run{Context: context.Background()}
	run.Step("Initial step")
	run.Progress(1, 10, "Count: %d", 1)

	if run.step != "Count: 1" {
		t.Errorf("step = %q, want %q", run.step, "Count: 1")
	}
}

func TestRun_ContextEmbedding(t *testing.T) {
	t.Run("Deadline", func(t *testing.T) {
		deadline := time.Now().Add(5 * time.Minute)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		run := &Run{Context: ctx}

		got, ok := run.Deadline()
		if !ok {
			t.Fatal("expected deadline to be set")
		}
		if !got.Equal(deadline) {
			t.Errorf("Deadline() = %v, want %v", got, deadline)
		}
	})

	t.Run("Done", func(t *testing.T) {
		run := &Run{Context: context.Background()}
		select {
		case <-run.Done():
			t.Error("Done() should not be closed for background context")
		default:
			// expected
		}
	})

	t.Run("Err", func(t *testing.T) {
		run := &Run{Context: context.Background()}
		if run.Err() != nil {
			t.Errorf("Err() = %v, want nil", run.Err())
		}
	})

	t.Run("Value", func(t *testing.T) {
		type key struct{}
		ctx := context.WithValue(context.Background(), key{}, "hello")
		run := &Run{Context: ctx}
		if got := run.Value(key{}); got != "hello" {
			t.Errorf("Value() = %v, want %q", got, "hello")
		}
	})
}

func TestRun_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	run := &Run{Context: ctx}
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

func TestRun_StepsRecord(t *testing.T) {
	run := &Run{Context: context.Background()}
	run.Step("Step one")
	run.Progress(1, 10, "Progress %d", 1)
	run.Step("Step two")

	want := []string{"Step one", "Progress 1", "Step two"}
	if len(run.steps) != len(want) {
		t.Fatalf("steps length = %d, want %d", len(run.steps), len(want))
	}
	for i, s := range run.steps {
		if s != want[i] {
			t.Errorf("steps[%d] = %q, want %q", i, s, want[i])
		}
	}
}

func TestRun_ZeroValue(t *testing.T) {
	run := &Run{Context: context.Background()}

	if run.step != "" {
		t.Errorf("step = %q, want empty", run.step)
	}
	if !run.stepAt.IsZero() {
		t.Error("stepAt should be zero")
	}
	if run.current != 0 {
		t.Errorf("current = %d, want 0", run.current)
	}
	if run.total != 0 {
		t.Errorf("total = %d, want 0", run.total)
	}
	if run.percent != 0 {
		t.Errorf("percent = %f, want 0", run.percent)
	}
	if run.steps != nil {
		t.Errorf("steps = %v, want nil", run.steps)
	}
}
