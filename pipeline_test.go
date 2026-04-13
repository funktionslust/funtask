package funtask

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// makeOK returns a TaskFunc that records its invocation in calls and
// returns OK with the given message.
func makeOK(t *testing.T, calls *[]string, name, msg string) TaskFunc {
	t.Helper()
	return func(_ *Run, _ Params) Result {
		*calls = append(*calls, name)
		return OK("%s", msg)
	}
}

// makeFail returns a TaskFunc that records its invocation and returns
// a Fail with the given code and message.
func makeFail(t *testing.T, calls *[]string, name, code, msg string) TaskFunc {
	t.Helper()
	return func(_ *Run, _ Params) Result {
		*calls = append(*calls, name)
		return Fail(code, "%s", msg)
	}
}

func TestSeq_RunsAllSteps(t *testing.T) {
	var calls []string
	tasks := map[string]TaskFunc{
		"a": makeOK(t, &calls, "a", "alpha"),
		"b": makeOK(t, &calls, "b", "beta"),
		"c": makeOK(t, &calls, "c", "gamma"),
	}

	step := Seq(Ref("a"), Ref("b"), Ref("c"))
	result := step.resolve(TestRun(t), Params{}, tasks)

	if !result.Success {
		t.Fatalf("Success = false, want true; message=%q", result.Message)
	}
	if got := strings.Join(calls, ","); got != "a,b,c" {
		t.Errorf("call order = %q, want %q", got, "a,b,c")
	}
	if result.Message != "alpha | beta | gamma" {
		t.Errorf("Message = %q, want %q", result.Message, "alpha | beta | gamma")
	}
}

func TestSeq_FailFastSkipsRemaining(t *testing.T) {
	var calls []string
	tasks := map[string]TaskFunc{
		"a": makeOK(t, &calls, "a", "alpha"),
		"b": makeFail(t, &calls, "b", "boom", "broken"),
		"c": makeOK(t, &calls, "c", "gamma"),
	}

	step := Seq(Ref("a"), Ref("b"), Ref("c"))
	result := step.resolve(TestRun(t), Params{}, tasks)

	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorCode != "boom" {
		t.Errorf("ErrorCode = %q, want %q", result.ErrorCode, "boom")
	}
	if got := strings.Join(calls, ","); got != "a,b" {
		t.Errorf("call order = %q, want %q (c must not run)", got, "a,b")
	}
}

func TestSeq_CancellationStopsBetweenSteps(t *testing.T) {
	var calls []string
	ctx, cancel := context.WithCancel(context.Background())

	tasks := map[string]TaskFunc{
		"a": func(_ *Run, _ Params) Result {
			calls = append(calls, "a")
			cancel()
			return OK("alpha")
		},
		"b": makeOK(t, &calls, "b", "beta"),
	}

	step := Seq(Ref("a"), Ref("b"))
	result := step.resolve(TestRunWithContext(t, ctx), Params{}, tasks)

	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorCode != "cancelled" {
		t.Errorf("ErrorCode = %q, want %q", result.ErrorCode, "cancelled")
	}
	if got := strings.Join(calls, ","); got != "a" {
		t.Errorf("call order = %q, want %q (b must not run)", got, "a")
	}
}

func TestPar_RunsAllStepsConcurrently(t *testing.T) {
	const sleepDur = 80 * time.Millisecond
	const stepCount = 4

	var counter atomic.Int32
	tasks := map[string]TaskFunc{
		"slow": func(_ *Run, _ Params) Result {
			counter.Add(1)
			time.Sleep(sleepDur)
			return OK("ok")
		},
	}

	steps := make([]Step, stepCount)
	for i := range steps {
		steps[i] = Ref("slow")
	}

	start := time.Now()
	result := Par(steps...).resolve(TestRun(t), Params{}, tasks)
	elapsed := time.Since(start)

	if !result.Success {
		t.Fatalf("Success = false, want true; message=%q", result.Message)
	}
	if counter.Load() != stepCount {
		t.Errorf("invocations = %d, want %d", counter.Load(), stepCount)
	}
	// Sequential execution would take stepCount*sleepDur. Allow generous
	// slack for slow CI but still well under serial wall time.
	if maxParallel := time.Duration(stepCount-1) * sleepDur; elapsed >= maxParallel {
		t.Errorf("elapsed = %v, want < %v (par must overlap)", elapsed, maxParallel)
	}
}

func TestPar_ReturnsFirstFailureByOrder(t *testing.T) {
	tasks := map[string]TaskFunc{
		"slow_fail": func(_ *Run, _ Params) Result {
			time.Sleep(40 * time.Millisecond)
			return Fail("slow", "slow failure")
		},
		"fast_fail": func(_ *Run, _ Params) Result {
			return Fail("fast", "fast failure")
		},
		"ok": func(_ *Run, _ Params) Result { return OK("done") },
	}

	// slow_fail is declared first; even though fast_fail completes
	// earlier, the result must reflect declaration order.
	step := Par(Ref("slow_fail"), Ref("fast_fail"), Ref("ok"))
	result := step.resolve(TestRun(t), Params{}, tasks)

	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorCode != "slow" {
		t.Errorf("ErrorCode = %q, want %q (must return first by order, not by completion)", result.ErrorCode, "slow")
	}
}

func TestPar_AggregatesAdditionalFailures(t *testing.T) {
	tasks := map[string]TaskFunc{
		"fail_a": func(_ *Run, _ Params) Result { return Fail("A", "first failure") },
		"fail_b": func(_ *Run, _ Params) Result { return Fail("B", "second failure") },
		"fail_c": func(_ *Run, _ Params) Result { return Fail("C", "third failure") },
		"ok":     func(_ *Run, _ Params) Result { return OK("done") },
	}

	step := Par(Ref("fail_a"), Ref("ok"), Ref("fail_b"), Ref("fail_c"))
	result := step.resolve(TestRun(t), Params{}, tasks)

	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorCode != "A" {
		t.Errorf("ErrorCode = %q, want %q (must be the first failure by declaration order)", result.ErrorCode, "A")
	}
	if !strings.Contains(result.Message, "first failure") {
		t.Errorf("Message = %q, want it to contain the original first failure message", result.Message)
	}
	if !strings.Contains(result.Message, "B: second failure") {
		t.Errorf("Message = %q, want it to mention the second failure", result.Message)
	}
	if !strings.Contains(result.Message, "C: third failure") {
		t.Errorf("Message = %q, want it to mention the third failure", result.Message)
	}
}

func TestPar_SingleFailureMessageNotMutated(t *testing.T) {
	tasks := map[string]TaskFunc{
		"only_fail": func(_ *Run, _ Params) Result { return Fail("X", "the only failure") },
		"ok_a":      func(_ *Run, _ Params) Result { return OK("a done") },
		"ok_b":      func(_ *Run, _ Params) Result { return OK("b done") },
	}

	result := Par(Ref("ok_a"), Ref("only_fail"), Ref("ok_b")).resolve(TestRun(t), Params{}, tasks)

	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.Message != "the only failure" {
		t.Errorf("Message = %q, want %q (no aggregation suffix when there is only one failure)", result.Message, "the only failure")
	}
}

func TestPar_EmptyReturnsOK(t *testing.T) {
	result := Par().resolve(TestRun(t), Params{}, nil)
	if !result.Success {
		t.Errorf("Success = false, want true")
	}
}

func TestSeq_EmptyReturnsOK(t *testing.T) {
	result := Seq().resolve(TestRun(t), Params{}, nil)
	if !result.Success {
		t.Errorf("Success = false, want true")
	}
}

func TestPar_WaitsForAllStepsBeforeReturning(t *testing.T) {
	var laggardDone atomic.Bool
	tasks := map[string]TaskFunc{
		"fast_fail": func(_ *Run, _ Params) Result {
			return Fail("fast", "broken")
		},
		"laggard": func(_ *Run, _ Params) Result {
			time.Sleep(50 * time.Millisecond)
			laggardDone.Store(true)
			return OK("done")
		},
	}

	step := Par(Ref("fast_fail"), Ref("laggard"))
	step.resolve(TestRun(t), Params{}, tasks)

	if !laggardDone.Load() {
		t.Error("Par returned before laggard finished; partial side effects would be left in flight")
	}
}

func TestPar_RecoversPanicInGoroutine(t *testing.T) {
	tasks := map[string]TaskFunc{
		"panicky": func(_ *Run, _ Params) Result {
			panic("boom")
		},
		"ok": func(_ *Run, _ Params) Result { return OK("done") },
	}

	result := Par(Ref("panicky"), Ref("ok")).resolve(TestRun(t), Params{}, tasks)

	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorCode != "panic" {
		t.Errorf("ErrorCode = %q, want %q", result.ErrorCode, "panic")
	}
	if !strings.Contains(result.Message, "boom") {
		t.Errorf("Message = %q, want it to contain %q", result.Message, "boom")
	}
}

func TestRef_UnknownTaskFailsGracefully(t *testing.T) {
	result := Ref("missing").resolve(TestRun(t), Params{}, map[string]TaskFunc{})

	if result.Success {
		t.Fatal("Success = true, want false")
	}
	if result.ErrorCode != "unknown_task" {
		t.Errorf("ErrorCode = %q, want %q", result.ErrorCode, "unknown_task")
	}
	if !strings.Contains(result.Message, "missing") {
		t.Errorf("Message = %q, want it to mention the task name", result.Message)
	}
}

func TestInline_ExecutesAndReportsName(t *testing.T) {
	run := TestRun(t)
	step := Inline("custom-step", func(r *Run, _ Params) Result {
		return OK("inline-ok")
	})

	result := step.resolve(run, Params{}, nil)

	if !result.Success {
		t.Fatalf("Success = false, want true; message=%q", result.Message)
	}
	if result.Message != "inline-ok" {
		t.Errorf("Message = %q, want %q", result.Message, "inline-ok")
	}
	steps := run.Steps()
	if len(steps) == 0 || steps[len(steps)-1] != "custom-step" {
		t.Errorf("steps = %v, want last entry %q", steps, "custom-step")
	}
}

func TestPipeline_LateBindingResolvesSubTasksRegisteredLater(t *testing.T) {
	// Pipeline references "alpha" and "beta" - both registered AFTER the
	// pipeline option in the New() call. This catches order-dependence
	// regressions in TaskDef.apply.
	var calls []string

	f := New("late-binding",
		WithAuthToken("t"),
		WithDeadLetterDir("/tmp/dl"),
		Pipeline("combo", Seq(Ref("alpha"), Ref("beta"))),
		Task("alpha", makeOK(t, &calls, "alpha", "a")),
		Task("beta", makeOK(t, &calls, "beta", "b")),
	)

	combo, ok := f.tasks["combo"]
	if !ok {
		t.Fatal("pipeline task 'combo' not registered")
	}

	result := combo(TestRun(t), Params{})
	if !result.Success {
		t.Fatalf("Success = false, want true; message=%q", result.Message)
	}
	if got := strings.Join(calls, ","); got != "alpha,beta" {
		t.Errorf("call order = %q, want %q", got, "alpha,beta")
	}
}

func TestPipeline_NestedComposition(t *testing.T) {
	var (
		mu    sync.Mutex
		calls []string
	)
	addCall := func(name string) TaskFunc {
		return func(_ *Run, _ Params) Result {
			mu.Lock()
			calls = append(calls, name)
			mu.Unlock()
			return OK("%s", name)
		}
	}

	f := New("nested",
		WithAuthToken("t"),
		WithDeadLetterDir("/tmp/dl"),
		Task("a", addCall("a")),
		Task("b", addCall("b")),
		Task("c", addCall("c")),
		Task("d", addCall("d")),
		Task("e", addCall("e")),
		Pipeline("pipe",
			Seq(
				Par(Ref("a"), Ref("b")),
				Ref("c"),
				Par(Ref("d"), Ref("e")),
			),
		),
	)

	result := f.tasks["pipe"](TestRun(t), Params{})
	if !result.Success {
		t.Fatalf("Success = false, want true; message=%q", result.Message)
	}
	if len(calls) != 5 {
		t.Errorf("calls len = %d, want 5; calls=%v", len(calls), calls)
	}
	// "c" must come after both a and b and before both d and e.
	idx := func(name string) int {
		for i, c := range calls {
			if c == name {
				return i
			}
		}
		return -1
	}
	if idx("c") < idx("a") || idx("c") < idx("b") {
		t.Errorf("c must run after a and b; calls=%v", calls)
	}
	if idx("c") > idx("d") || idx("c") > idx("e") {
		t.Errorf("c must run before d and e; calls=%v", calls)
	}
}

func TestPipeline_DescriptionAndKeepResultsAreApplied(t *testing.T) {
	f := New("meta",
		WithAuthToken("t"),
		WithDeadLetterDir("/tmp/dl"),
		Task("noop", func(_ *Run, _ Params) Result { return OK("") }),
		Pipeline("pipe", Ref("noop")).
			Description("a multi-step pipeline").
			KeepResults(7),
	)

	if got := f.taskDescriptions["pipe"]; got != "a multi-step pipeline" {
		t.Errorf("description = %q, want %q", got, "a multi-step pipeline")
	}
	if got := f.taskResultSizes["pipe"]; got != 7 {
		t.Errorf("keepResults = %d, want 7", got)
	}
}

func TestPipeline_ServedViaRunHandler(t *testing.T) {
	f := testServerWith(t,
		Task("first", func(_ *Run, _ Params) Result { return OK("one") }),
		Task("second", func(_ *Run, _ Params) Result { return OK("two") }),
		Pipeline("seq-of-two", Seq(Ref("first"), Ref("second"))),
	)

	req := httptest.NewRequest(http.MethodPost, "/run/seq-of-two", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer test-secret")
	w := httptest.NewRecorder()
	f.routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp jobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
	}
	if !resp.Success {
		t.Errorf("Success = false, want true; error=%+v", resp.Error)
	}
	if resp.Message != "one | two" {
		t.Errorf("Message = %q, want %q", resp.Message, "one | two")
	}
}
