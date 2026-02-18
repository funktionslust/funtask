package funtask

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// retrySchedule defines delays between delivery attempts. The first attempt
// runs immediately; subsequent attempts sleep retrySchedule[attempt-1].
// The 5th entry (120s) supports deployments configuring more than 5 retries.
var retrySchedule = []time.Duration{
	1 * time.Second,
	5 * time.Second,
	15 * time.Second,
	60 * time.Second,
	120 * time.Second,
}

type deliverer struct {
	deadLetterDir string
	logger        *slog.Logger
	retries       int

	// Function fields for testability — swap in tests for fault injection.
	writeFunc  func(name string, data []byte, perm os.FileMode) error
	removeFunc func(name string) error
	postFunc   func(url string, data []byte) (int, error)
	sleepFunc  func(time.Duration)
}

func newDeliverer(deadLetterDir string, logger *slog.Logger, retries int, callbackTimeout time.Duration) *deliverer {
	client := &http.Client{Timeout: callbackTimeout}
	return &deliverer{
		deadLetterDir: deadLetterDir,
		logger:        logger,
		retries:       retries,
		writeFunc:     os.WriteFile,
		removeFunc:    os.Remove,
		postFunc: func(url string, data []byte) (int, error) {
			resp, err := client.Post(url, "application/json", bytes.NewReader(data))
			if err != nil {
				return 0, err
			}
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, resp.Body)
			return resp.StatusCode, nil
		},
		sleepFunc: time.Sleep,
	}
}

func (d *deliverer) deadLetterPath(jobID string) (string, error) {
	path := filepath.Join(d.deadLetterDir, jobID+".json")
	dir := filepath.Clean(d.deadLetterDir) + string(filepath.Separator)
	if !strings.HasPrefix(path, dir) {
		return "", fmt.Errorf("job ID escapes dead letter directory")
	}
	return path, nil
}

func (d *deliverer) writeDeadLetter(jobID string, data []byte) error {
	path, err := d.deadLetterPath(jobID)
	if err != nil {
		return err
	}
	if err := d.writeFunc(path, data, 0o600); err != nil {
		return err
	}
	d.logger.Debug("dead letter file written", "jobId", jobID, "path", path)
	return nil
}

func (d *deliverer) removeDeadLetter(jobID string) error {
	path, err := d.deadLetterPath(jobID)
	if err != nil {
		return err
	}
	if err := d.removeFunc(path); err != nil {
		return err
	}
	d.logger.Debug("dead letter file removed", "jobId", jobID, "path", path)
	return nil
}

// deliver POSTs data to callbackURL with retries. On success it deletes
// the dead letter file. On failure the file is retained for recovery.
func (d *deliverer) deliver(jobID, callbackURL string, data []byte) error {
	var lastErr error
	for attempt := 0; attempt < d.retries; attempt++ {
		if attempt > 0 {
			idx := attempt - 1
			if idx >= len(retrySchedule) {
				idx = len(retrySchedule) - 1
			}
			d.sleepFunc(retrySchedule[idx])
		}

		status, err := d.postFunc(callbackURL, data)
		if err != nil {
			lastErr = err
			d.logger.Warn("callback attempt failed",
				"jobId", jobID, "attempt", attempt+1, "error", err)
			continue
		}
		if status < 200 || status >= 300 {
			lastErr = fmt.Errorf("HTTP %d", status)
			d.logger.Warn("callback attempt failed",
				"jobId", jobID, "attempt", attempt+1, "status", status)
			continue
		}

		if err := d.removeDeadLetter(jobID); err != nil {
			d.logger.Warn("dead letter removal failed",
				"jobId", jobID, "error", err)
		}
		d.logger.Info("callback delivered",
			"jobId", jobID, "status", status, "attempt", attempt+1)
		return nil
	}

	d.logger.Error("callback delivery failed",
		"jobId", jobID, "attempts", d.retries, "callbackUrl", callbackURL)
	return fmt.Errorf("callback delivery failed for job %s after %d attempts: %w", jobID, d.retries, lastErr)
}

// deliverOnce POSTs data to callbackURL with a single attempt (no retries).
// Used during graceful shutdown where the full retry schedule (~3 minutes)
// exceeds the shutdown timeout. On success it deletes the dead letter file.
// On failure the file is retained for recovery.
func (d *deliverer) deliverOnce(jobID, callbackURL string, data []byte) error {
	status, err := d.postFunc(callbackURL, data)
	if err != nil {
		return fmt.Errorf("shutdown callback failed for job %s: %w", jobID, err)
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("shutdown callback failed for job %s: HTTP %d", jobID, status)
	}

	if err := d.removeDeadLetter(jobID); err != nil {
		d.logger.Warn("dead letter removal failed",
			"jobId", jobID, "error", err)
	}
	d.logger.Info("shutdown callback delivered",
		"jobId", jobID, "status", status)
	return nil
}

// marshalResult serializes a jobResponse to JSON. If marshalling fails
// (e.g., Data contains an unsupported type), the response is replaced
// with a result_serialization_failed error result and serialized again.
func marshalResult(resp jobResponse) ([]byte, jobResponse) {
	data, err := json.Marshal(resp)
	if err != nil {
		fallback := jobResponse{
			JobID:    resp.JobID,
			Success:  false,
			Duration: resp.Duration,
			Error: &jobError{
				Code:    "result_serialization_failed",
				Message: "result could not be serialized",
			},
		}
		data, _ = json.Marshal(fallback)
		return data, fallback
	}
	return data, resp
}
