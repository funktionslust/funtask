package funtask

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Run is the execution context for a single job. It embeds
// context.Context so it works anywhere a context.Context is expected —
// pass it directly to database drivers, HTTP clients, etc.
type Run struct {
	context.Context

	mu       sync.RWMutex // guards step, stepAt, current, total, percent
	step     string
	stepAt   time.Time
	current  int
	total    int
	percent  float64
	steps    []string             // records all Step/Progress messages for test assertions
	logf     func(string, ...any) // optional log function, set by TestRun
	onChange func()               // called after Step/Progress; nil = no-op
}

// Step reports what the task is currently doing. The step description is
// visible via GET /health and included in error results for diagnostic
// context. Only the latest step is retained.
func (r *Run) Step(msg string, args ...any) {
	s := fmt.Sprintf(msg, args...)
	r.mu.Lock()
	r.step = s
	r.stepAt = time.Now()
	r.steps = append(r.steps, s)
	r.mu.Unlock()
	if r.logf != nil {
		r.logf("step: %s", s)
	}
	if r.onChange != nil {
		r.onChange()
	}
}

// currentStep returns the current step description safely for concurrent access.
func (r *Run) currentStep() string {
	r.mu.RLock()
	s := r.step
	r.mu.RUnlock()
	return s
}

// runSnapshot holds a point-in-time copy of step/progress state.
// Used by the health handler to read Run state without holding the lock.
type runSnapshot struct {
	step    string
	stepAt  time.Time
	current int
	total   int
	percent float64
}

// snapshot returns a consistent point-in-time copy of step/progress fields.
func (r *Run) snapshot() runSnapshot {
	r.mu.RLock()
	s := runSnapshot{
		step:    r.step,
		stepAt:  r.stepAt,
		current: r.current,
		total:   r.total,
		percent: r.percent,
	}
	r.mu.RUnlock()
	return s
}

// Progress reports structured progress for countable iterations. It
// updates the step description and records current/total/percent, all
// visible via GET /health. Use this instead of Step when iterating over
// a countable set of items.
func (r *Run) Progress(current, total int, msg string, args ...any) {
	s := fmt.Sprintf(msg, args...)
	r.mu.Lock()
	r.step = s
	r.stepAt = time.Now()
	r.current = current
	r.total = total
	if total > 0 {
		r.percent = float64(current) * 100 / float64(total)
	} else {
		r.percent = 0
	}
	r.steps = append(r.steps, s)
	r.mu.Unlock()
	if r.logf != nil {
		r.logf("step: %s", s)
	}
	if r.onChange != nil {
		r.onChange()
	}
}
