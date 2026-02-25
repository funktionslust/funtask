package funtask

import (
	"net/http"
	"time"
)

type healthResponse struct {
	Name   string                     `json:"name"`
	Status string                     `json:"status"`
	Uptime string                     `json:"uptime"`
	Tasks  map[string]healthTaskState `json:"tasks"`
}

type healthTaskState struct {
	Status      string         `json:"status"`
	Description string         `json:"description,omitempty"`
	Example     map[string]any `json:"example,omitempty"`
	CurrentJob  *healthRunning `json:"currentJob,omitempty"`
	LastJob     *healthLastJob `json:"lastJob,omitempty"`
	Results     []healthResult `json:"results,omitempty"`
}

type healthRunning struct {
	JobID       string          `json:"jobId"`
	Running     string          `json:"running"`
	CurrentStep string          `json:"currentStep,omitempty"`
	Progress    *healthProgress `json:"progress,omitempty"`
	LastStepAt  string          `json:"lastStepAt,omitempty"`
}

type healthProgress struct {
	Current int     `json:"current"`
	Total   int     `json:"total"`
	Percent float64 `json:"percent"`
}

type healthLastJob struct {
	JobID    string `json:"jobId"`
	Success  bool   `json:"success"`
	Finished string `json:"finished"`
	Duration string `json:"duration"`
}

type healthResult struct {
	JobID    string    `json:"jobId"`
	Success  bool      `json:"success"`
	Message  string    `json:"message,omitempty"`
	Duration string    `json:"duration"`
	Finished string    `json:"finished"`
	Error    *jobError `json:"error,omitempty"`
}

func (f *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (f *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if f.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "server is shutting down"})
		return
	}
	if f.readiness != nil {
		if err := f.readiness(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (f *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, f.buildHealthResponse())
}

// buildHealthResponse assembles a point-in-time health snapshot.
// Used by both GET /health and the SSE event stream.
func (f *Server) buildHealthResponse() healthResponse {
	status := "ok"
	if f.draining.Load() {
		status = "draining"
	}

	resp := healthResponse{
		Name:   f.name,
		Status: status,
		Uptime: time.Since(f.startedAt).Round(time.Second).String(),
		Tasks:  make(map[string]healthTaskState, len(f.slots)),
	}

	for name, slot := range f.slots {
		// Copy slot fields under slot lock, then release before reading run snapshot.
		slot.mu.Lock()
		running := slot.running
		slotJobID := slot.jobID
		slotStartedAt := slot.startedAt
		runPtr := slot.run
		lastJobID := slot.lastJobID
		lastSuccess := slot.lastSuccess
		lastCompletedAt := slot.lastCompletedAt
		lastDuration := slot.lastDuration
		slot.mu.Unlock()

		var ts healthTaskState
		if running {
			cj := &healthRunning{
				JobID:   slotJobID,
				Running: time.Since(slotStartedAt).Round(time.Second).String(),
			}

			// Read run snapshot outside slot lock (avoids nested locking).
			if runPtr != nil {
				snap := runPtr.snapshot()
				if snap.step != "" {
					cj.CurrentStep = snap.step
					cj.LastStepAt = snap.stepAt.Format(time.RFC3339)
				}
				if snap.total > 0 {
					cj.Progress = &healthProgress{
						Current: snap.current,
						Total:   snap.total,
						Percent: snap.percent,
					}
				}
			}

			ts = healthTaskState{
				Status:     "running",
				CurrentJob: cj,
			}
		} else {
			ts = healthTaskState{Status: "idle"}
			if lastJobID != "" {
				ts.LastJob = &healthLastJob{
					JobID:    lastJobID,
					Success:  lastSuccess,
					Finished: lastCompletedAt.Format(time.RFC3339),
					Duration: lastDuration,
				}
			}
		}

		if desc := f.taskDescriptions[name]; desc != "" {
			ts.Description = desc
		}
		if ex := f.taskExamples[name]; ex != nil {
			cp := make(map[string]any, len(ex))
			for k, v := range ex {
				cp[k] = v
			}
			ts.Example = cp
		}
		ts.Results = f.history.healthResults(name)
		resp.Tasks[name] = ts
	}

	return resp
}
