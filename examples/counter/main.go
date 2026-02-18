// Counter is a reference example for funtask.
//
// It registers two tasks — count-up and count-down — that share a single
// count function. This demonstrates multi-task servers, shared business logic,
// progress reporting, and parameter handling.
//
// Both tasks run concurrently. While count-up is running, you can trigger
// count-down independently. GET /health shows progress for both.
//
// ListenAndServe handles SIGTERM automatically: it stops accepting new jobs,
// cancels in-flight work, delivers pending results, and exits cleanly.
//
// Usage:
//
//	export FUNTASK_AUTH_TOKEN=secret
//	export FUNTASK_DEAD_LETTER_DIR=/tmp/dead-letters
//	mkdir -p $FUNTASK_DEAD_LETTER_DIR
//	go run ./examples/counter
//
//	# Sync request (blocks until done):
//	curl -H "Authorization: Bearer secret" \
//	  -d '{"params":{"limit":15}}' localhost:8080/run/count-up
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
		funtask.WithTask("count-up", countUp),
		funtask.WithTask("count-down", countDown),
		funtask.WithAuthToken(os.Getenv("FUNTASK_AUTH_TOKEN")),
		funtask.WithDeadLetterDir(os.Getenv("FUNTASK_DEAD_LETTER_DIR")),
	)
	log.Fatal(w.ListenAndServe(":8080"))
}

// count is the shared business logic. Both tasks call this with different
// arguments. In a real server, this would be the domain code — calling APIs,
// querying databases, transforming data. The task functions below are thin
// wrappers that parse params and call this.
func count(ctx *funtask.Run, from, to int) funtask.Result {
	step := 1
	if from > to {
		step = -1
	}

	n := (to-from)*step + 1 // total iterations
	for i := range n {
		value := from + i*step
		ctx.Progress(i+1, n, "Count: %d", value)

		select {
		case <-ctx.Done():
			return funtask.Fail("cancelled", "Counting cancelled at %d.", value)
		case <-time.After(1 * time.Second):
		}
	}

	return funtask.OK("Counted from %d to %d.", from, to).
		WithData("from", from).
		WithData("to", to)
}

func countUp(ctx *funtask.Run, params funtask.Params) funtask.Result {
	limit, err := params.Int("limit")
	if err != nil {
		return funtask.Fail("invalid_params", "%v", err)
	}
	return count(ctx, 1, limit)
}

func countDown(ctx *funtask.Run, params funtask.Params) funtask.Result {
	limit, err := params.Int("limit")
	if err != nil {
		return funtask.Fail("invalid_params", "%v", err)
	}
	return count(ctx, limit, 1)
}
