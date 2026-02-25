// Counter is a reference example for funtask.
//
// It registers three tasks that showcase different patterns:
//   - greet      — instant success with parameters
//   - fail-demo  — instant failure with a custom error code
//   - countdown  — long-running task with progress reporting
//
// The dashboard (enabled with WithDashboard) provides a browser UI at
// /dashboard for triggering tasks, watching progress, and inspecting
// errors. Open http://localhost:8080/dashboard after starting the server.
//
// Usage:
//
//	export FUNTASK_AUTH_TOKEN=secret
//	export FUNTASK_DEAD_LETTER_DIR=/tmp/dead-letters
//	mkdir -p $FUNTASK_DEAD_LETTER_DIR
//	go run ./example
//
//	# Open the dashboard:
//	open http://localhost:8080/dashboard
//
//	# Or use curl — sync request (blocks until done):
//	curl -H "Authorization: Bearer secret" \
//	  -d '{"params":{"start":5}}' localhost:8080/run/countdown
//
//	# Async request with callback:
//	curl -H "Authorization: Bearer secret" \
//	  -d '{"params":{"start":5},"callbackUrl":"https://hooks.example.com/done"}' \
//	  localhost:8080/run/countdown
//
//	# Check progress while running:
//	curl -H "Authorization: Bearer secret" localhost:8080/health | jq .
package main

import (
	"log"
	"os"
	"time"

	"github.com/funktionslust/funtask"
)

func main() {
	w := funtask.New("counter",
		funtask.Task("greet", greet).
			Description("Returns a greeting with optional name").
			Example(map[string]any{"name": "World"}),
		funtask.Task("fail-demo", failDemo).
			Description("Always fails with a custom error code").
			Example(map[string]any{"code": "out_of_coffee"}),
		funtask.Task("countdown", countdown).
			Description("Counts down from start to 1, one step per second").
			Example(map[string]any{"start": 5}),
		funtask.WithAuthToken(os.Getenv("FUNTASK_AUTH_TOKEN")),
		funtask.WithDeadLetterDir(os.Getenv("FUNTASK_DEAD_LETTER_DIR")),
		funtask.WithDashboard(),
	)
	log.Fatal(w.ListenAndServe(":8080"))
}

func greet(_ *funtask.Run, params funtask.Params) funtask.Result {
	name, _ := params.String("name")
	if name == "" {
		name = "World"
	}
	return funtask.OK("Hello, %s!", name).WithData("name", name)
}

func failDemo(_ *funtask.Run, params funtask.Params) funtask.Result {
	code, _ := params.String("code")
	if code == "" {
		code = "simulated_error"
	}
	return funtask.Fail(code, "This task always fails on purpose.")
}

func countdown(ctx *funtask.Run, params funtask.Params) funtask.Result {
	start, err := params.Int("start")
	if err != nil {
		return funtask.Fail("invalid_params", "%v", err)
	}

	for i := range start {
		remaining := start - i
		ctx.Progress(i+1, start, "T-%d", remaining)

		select {
		case <-ctx.Done():
			return funtask.Fail("cancelled", "Countdown cancelled at T-%d.", remaining)
		case <-time.After(1 * time.Second):
		}
	}

	return funtask.OK("Countdown complete.").WithData("start", start)
}
