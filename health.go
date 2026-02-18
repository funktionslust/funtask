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
	Status     string         `json:"status"`
	CurrentJob *healthRunning `json:"currentJob,omitempty"`
	LastJob    *healthLastJob `json:"lastJob,omitempty"`
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

			resp.Tasks[name] = healthTaskState{
				Status:     "running",
				CurrentJob: cj,
			}
			continue
		}

		// Idle task.
		ts := healthTaskState{Status: "idle"}
		if lastJobID != "" {
			ts.LastJob = &healthLastJob{
				JobID:    lastJobID,
				Success:  lastSuccess,
				Finished: lastCompletedAt.Format(time.RFC3339),
				Duration: lastDuration,
			}
		}
		resp.Tasks[name] = ts
	}

	writeJSON(w, http.StatusOK, resp)
}
