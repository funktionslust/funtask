package funtask

import (
	"fmt"
	"strings"
	"sync"
)

// Step is a composable element of a Pipeline. Construct a step tree with
// Ref, Inline, Seq and Par, then pass the root to Pipeline.
//
// Step is a closed interface (the resolve method is unexported) so the
// runtime can guarantee that every step participates in cancellation,
// panic recovery and step history. Wrap arbitrary task functions with
// Inline if you need ad-hoc logic that is not registered as a top-level
// task.
type Step interface {
	resolve(run *Run, params Params, tasks map[string]TaskFunc) Result
}

// Ref references a task already registered with the same Server by name.
// The lookup happens at execution time, so referenced tasks may be
// registered before or after the Pipeline that uses them. If the task is
// not registered when the pipeline runs, the step fails with error code
// "unknown_task".
func Ref(name string) Step {
	return refStep(name)
}

type refStep string

func (r refStep) resolve(run *Run, params Params, tasks map[string]TaskFunc) Result {
	name := string(r)
	fn, ok := tasks[name]
	if !ok {
		return Fail("unknown_task", "task %q is not registered", name)
	}
	run.Step("%s", name)
	return runStep(name, fn, run, params)
}

// Inline wraps a TaskFunc as a pipeline step without requiring it to be
// registered as a top-level task. The name is reported via Run.Step
// before the function runs and used as LastStep on panic. An empty name
// is allowed for anonymous steps.
func Inline(name string, fn TaskFunc) Step {
	return inlineStep{name: name, fn: fn}
}

type inlineStep struct {
	name string
	fn   TaskFunc
}

func (i inlineStep) resolve(run *Run, params Params, _ map[string]TaskFunc) Result {
	if i.name != "" {
		run.Step("%s", i.name)
	}
	return runStep(i.name, i.fn, run, params)
}

// Seq runs steps sequentially. If any step fails the pipeline returns
// that failure immediately and later steps are skipped. Success messages
// from completed steps are joined with " | " and returned as the final
// message. Cancellation is checked between steps so a stopped pipeline
// does not start new work.
func Seq(steps ...Step) Step {
	return seqStep(steps)
}

type seqStep []Step

func (s seqStep) resolve(run *Run, params Params, tasks map[string]TaskFunc) Result {
	var messages []string
	for i, step := range s {
		if err := run.Err(); err != nil {
			return Fail("cancelled", "pipeline cancelled at seq step %d: %v", i, err)
		}
		result := step.resolve(run, params, tasks)
		if !result.Success {
			return result
		}
		if result.Message != "" {
			messages = append(messages, result.Message)
		}
	}
	return OK("%s", strings.Join(messages, " | "))
}

// Par runs steps concurrently and waits for all of them to finish, even
// when some fail, so partial side effects are not abandoned mid-flight.
// When one or more steps fail the first failure by declaration order is
// returned; messages from any additional failing steps are appended to
// the returned message so callers do not lose diagnostic context. Success
// messages from all steps are joined with " | ".
//
// Steps share the parent Run, so progress reports from parallel branches
// interleave in the step history. Panics in individual goroutines are
// recovered and converted to failures with code "panic"; they do not
// crash sibling steps or the pipeline.
func Par(steps ...Step) Step {
	return parStep(steps)
}

type parStep []Step

func (p parStep) resolve(run *Run, params Params, tasks map[string]TaskFunc) Result {
	switch len(p) {
	case 0:
		return OK("")
	case 1:
		return p[0].resolve(run, params, tasks)
	}

	results := make([]Result, len(p))
	var wg sync.WaitGroup
	for i, step := range p {
		wg.Add(1)
		go func(i int, step Step) {
			defer wg.Done()
			defer func() {
				if v := recover(); v != nil {
					results[i] = Fail("panic", "%v", v)
				}
			}()
			results[i] = step.resolve(run, params, tasks)
		}(i, step)
	}
	wg.Wait()

	var (
		messages     []string
		firstFailIdx = -1
		otherFails   []string
	)
	for i, r := range results {
		if r.Success {
			if r.Message != "" {
				messages = append(messages, r.Message)
			}
			continue
		}
		if firstFailIdx < 0 {
			firstFailIdx = i
			continue
		}
		otherFails = append(otherFails, fmt.Sprintf("%s: %s", r.ErrorCode, r.Message))
	}
	if firstFailIdx >= 0 {
		failure := results[firstFailIdx]
		if len(otherFails) > 0 {
			failure.Message = failure.Message + " (also failed: " + strings.Join(otherFails, "; ") + ")"
		}
		return failure
	}
	return OK("%s", strings.Join(messages, " | "))
}

// Pipeline registers a composed task. The step tree is resolved against
// the server's task registry at execution time, so referenced sub-tasks
// can be registered in any order relative to the pipeline.
//
// The returned *TaskDef behaves like any other task: it is triggered via
// POST /run/{name}, appears on the dashboard, participates in duplicate
// detection and result history, and supports callbacks. Sub-tasks
// invoked through Ref are called as Go functions and do not acquire
// their own slot, so the pipeline can run while the sub-tasks remain
// individually triggerable from the dashboard or other clients.
//
// Example:
//
//	funtask.Pipeline("full-sync",
//	    funtask.Seq(
//	        funtask.Par(funtask.Ref("sync-akeneo"), funtask.Ref("sync-sap")),
//	        funtask.Ref("sync-shopify"),
//	    ),
//	).Description("Full product sync")
func Pipeline(name string, root Step) *TaskDef {
	return &TaskDef{name: name, pipelineRoot: root}
}

// runStep executes a TaskFunc inside a goroutine-local panic boundary.
// The outer server safeRunTask wrapper already covers panics on the
// pipeline goroutine itself, but Par children run in their own
// goroutines and need their own recovery.
func runStep(name string, fn TaskFunc, run *Run, params Params) (result Result) {
	defer func() {
		if v := recover(); v != nil {
			result = Fail("panic", "%v", v)
			result.LastStep = name
		}
	}()
	return fn(run, params)
}
