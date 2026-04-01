package funtask

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func testPersister(path string) *historyPersister {
	return &historyPersister{
		path:   path,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		readFunc: func(string) ([]byte, error) {
			return nil, errors.New("readFunc not configured")
		},
		statFunc: func(string) (os.FileInfo, error) {
			return nil, errors.New("statFunc not configured")
		},
		mkdirFunc: func(string, os.FileMode) error {
			return errors.New("mkdirFunc not configured")
		},
		createFunc: func(string) (syncWriteCloser, error) {
			return nil, errors.New("createFunc not configured")
		},
		renameFunc: func(string, string) error {
			return errors.New("renameFunc not configured")
		},
	}
}

func TestHistoryPersister_LoadFileNotExist(t *testing.T) {
	p := testPersister("/tmp/does-not-exist.json")
	p.statFunc = func(string) (os.FileInfo, error) {
		return nil, os.ErrNotExist
	}

	ph, err := p.load()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ph != nil {
		t.Fatalf("expected nil history, got %+v", ph)
	}
}

func TestHistoryPersister_LoadStatError(t *testing.T) {
	p := testPersister("/tmp/history.json")
	p.statFunc = func(string) (os.FileInfo, error) {
		return nil, errors.New("permission denied")
	}

	_, err := p.load()
	if err == nil {
		t.Fatal("expected error for stat failure")
	}
}

func TestHistoryPersister_LoadCorruptJSON(t *testing.T) {
	p := testPersister("/tmp/history.json")
	p.statFunc = func(string) (os.FileInfo, error) { return nil, nil }
	p.readFunc = func(string) ([]byte, error) {
		return []byte("{not valid json"), nil
	}

	_, err := p.load()
	if err == nil {
		t.Fatal("expected error for corrupt JSON")
	}
}

func TestHistoryPersister_LoadSuccess(t *testing.T) {
	want := persistedHistory{
		Version: 1,
		Tasks: map[string][]persistedEntry{
			"echo": {
				{
					JobID: "j1",
					Response: jobResponse{
						JobID:    "j1",
						Success:  true,
						Duration: "50ms",
					},
					FinishedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
	}
	data, _ := json.Marshal(want)

	p := testPersister("/tmp/history.json")
	p.statFunc = func(string) (os.FileInfo, error) { return nil, nil }
	p.readFunc = func(string) ([]byte, error) { return data, nil }

	got, err := p.load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Version != 1 {
		t.Errorf("version = %d, want 1", got.Version)
	}
	if len(got.Tasks["echo"]) != 1 {
		t.Fatalf("expected 1 echo entry, got %d", len(got.Tasks["echo"]))
	}
	e := got.Tasks["echo"][0]
	if e.JobID != "j1" || !e.Response.Success || e.Response.Duration != "50ms" {
		t.Errorf("unexpected entry: %+v", e)
	}
}

func TestHistoryPersister_Populate_RespectsLimit(t *testing.T) {
	h := &resultHistory{
		tasks: map[string]*taskResults{
			"echo": {limit: 3},
		},
	}
	ph := &persistedHistory{
		Tasks: map[string][]persistedEntry{
			"echo": make([]persistedEntry, 10),
		},
	}
	for i := range ph.Tasks["echo"] {
		ph.Tasks["echo"][i] = persistedEntry{
			JobID:    "j" + string(rune('0'+i)),
			Response: jobResponse{JobID: "j" + string(rune('0'+i))},
		}
	}

	p := testPersister("")
	p.populate(h, ph)

	if got := len(h.tasks["echo"].entries); got != 3 {
		t.Fatalf("expected 3 entries after trim, got %d", got)
	}
	// Should keep the last 3 (newest).
	if h.tasks["echo"].entries[0].jobID != ph.Tasks["echo"][7].JobID {
		t.Errorf("expected oldest kept entry to be index 7, got %s",
			h.tasks["echo"].entries[0].jobID)
	}
}

func TestHistoryPersister_Populate_UnknownTask(t *testing.T) {
	h := &resultHistory{
		tasks: map[string]*taskResults{
			"echo": {limit: 10},
		},
	}
	ph := &persistedHistory{
		Tasks: map[string][]persistedEntry{
			"removed-task": {{JobID: "j1"}},
		},
	}

	p := testPersister("")
	p.populate(h, ph)

	if len(h.tasks["echo"].entries) != 0 {
		t.Error("echo should remain empty")
	}
}

func TestHistoryPersister_Populate_TaskInServerNotInFile(t *testing.T) {
	h := &resultHistory{
		tasks: map[string]*taskResults{
			"echo":    {limit: 10},
			"process": {limit: 5},
		},
	}
	ph := &persistedHistory{
		Tasks: map[string][]persistedEntry{
			"echo": {{JobID: "j1", Response: jobResponse{JobID: "j1", Success: true}}},
		},
	}

	p := testPersister("")
	p.populate(h, ph)

	if len(h.tasks["echo"].entries) != 1 {
		t.Errorf("echo should have 1 entry, got %d", len(h.tasks["echo"].entries))
	}
	if len(h.tasks["process"].entries) != 0 {
		t.Error("process should remain empty")
	}
}

func TestHistoryPersister_SaveWritesJSON(t *testing.T) {
	h := &resultHistory{
		tasks: map[string]*taskResults{
			"echo": {
				limit: 10,
				entries: []resultEntry{
					{
						jobID: "j1",
						response: jobResponse{
							JobID:    "j1",
							Success:  true,
							Message:  "done",
							Duration: "100ms",
						},
						finishedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}

	var buf tempFileBuffer
	p := testPersister("/tmp/history.json")
	p.mkdirFunc = func(string, os.FileMode) error { return nil }

	if err := p.saveToWriter(&buf, h); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got persistedHistory
	if err := json.Unmarshal(buf.buf, &got); err != nil {
		t.Fatalf("saved data is not valid JSON: %v", err)
	}
	if got.Version != persistVersion {
		t.Errorf("version = %d, want %d", got.Version, persistVersion)
	}
	entries := got.Tasks["echo"]
	if len(entries) != 1 {
		t.Fatalf("expected 1 echo entry, got %d", len(entries))
	}
	if entries[0].JobID != "j1" || !entries[0].Response.Success {
		t.Errorf("unexpected entry: %+v", entries[0])
	}
}

func TestHistoryPersister_SaveAtomicWrite(t *testing.T) {
	h := &resultHistory{
		tasks: map[string]*taskResults{
			"echo": {
				limit: 10,
				entries: []resultEntry{
					{jobID: "j1", response: jobResponse{JobID: "j1", Success: true}},
				},
			},
		},
	}

	var (
		mkdirCalled bool
		renameSrc   string
		renameDst   string
		buf         tempFileBuffer
	)

	p := testPersister("/data/history.json")
	p.mkdirFunc = func(path string, _ os.FileMode) error {
		mkdirCalled = true
		if path != "/data" {
			t.Errorf("mkdir path = %q, want /data", path)
		}
		return nil
	}
	p.createFunc = func(name string) (syncWriteCloser, error) {
		buf.name = name
		return &buf, nil
	}
	p.renameFunc = func(src, dst string) error {
		renameSrc = src
		renameDst = dst
		return nil
	}

	if err := p.save(h); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mkdirCalled {
		t.Error("mkdirFunc was not called")
	}
	if renameSrc != "/data/history.json.tmp" {
		t.Errorf("rename src = %q, want /data/history.json.tmp", renameSrc)
	}
	if renameDst != "/data/history.json" {
		t.Errorf("rename dst = %q, want /data/history.json", renameDst)
	}
	if len(buf.buf) == 0 {
		t.Error("no data written to temp file")
	}
}

func TestHistoryPersister_SaveError_Mkdir(t *testing.T) {
	h := &resultHistory{tasks: map[string]*taskResults{}}

	p := testPersister("/tmp/history.json")
	p.mkdirFunc = func(string, os.FileMode) error {
		return errors.New("no permission")
	}

	err := p.save(h)
	if err == nil {
		t.Fatal("expected error for mkdir failure")
	}
}

func TestHistoryPersister_RoundTrip(t *testing.T) {
	// Build history with entries.
	h := &resultHistory{
		tasks: map[string]*taskResults{
			"echo": {limit: 10},
			"sync": {limit: 5},
		},
	}
	h.store("echo", "j1", jobResponse{
		JobID: "j1", Success: true, Duration: "50ms", Message: "ok",
	})
	h.store("echo", "j2", jobResponse{
		JobID: "j2", Success: false, Duration: "1s",
		Error: &jobError{Code: "timeout", Message: "timed out"},
	})
	h.store("sync", "j3", jobResponse{
		JobID: "j3", Success: true, Duration: "200ms",
		Data: map[string]any{"count": float64(42)},
	})

	// Save to buffer.
	var buf tempFileBuffer
	p := testPersister("/tmp/history.json")
	p.mkdirFunc = func(string, os.FileMode) error { return nil }

	if err := p.saveToWriter(&buf, h); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Load from buffer.
	p.statFunc = func(string) (os.FileInfo, error) { return nil, nil }
	p.readFunc = func(string) ([]byte, error) { return buf.buf, nil }

	ph, err := p.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// Populate into fresh history.
	h2 := &resultHistory{
		tasks: map[string]*taskResults{
			"echo": {limit: 10},
			"sync": {limit: 5},
		},
	}
	p.populate(h2, ph)

	// Verify lookups work.
	resp, ok := h2.lookup("echo", "j1")
	if !ok || !resp.Success || resp.Message != "ok" {
		t.Errorf("j1 lookup failed: ok=%v resp=%+v", ok, resp)
	}
	resp, ok = h2.lookup("echo", "j2")
	if !ok || resp.Success || resp.Error.Code != "timeout" {
		t.Errorf("j2 lookup failed: ok=%v resp=%+v", ok, resp)
	}
	resp, ok = h2.lookup("sync", "j3")
	if !ok || !resp.Success {
		t.Errorf("j3 lookup failed: ok=%v resp=%+v", ok, resp)
	}
	if resp.Data["count"] != float64(42) {
		t.Errorf("j3 data mismatch: %v", resp.Data)
	}
}

func TestHistoryPersister_SaveEmptyHistory(t *testing.T) {
	h := &resultHistory{
		tasks: map[string]*taskResults{
			"echo": {limit: 10},
		},
	}

	var buf tempFileBuffer
	p := testPersister("/tmp/history.json")
	p.mkdirFunc = func(string, os.FileMode) error { return nil }

	if err := p.saveToWriter(&buf, h); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got persistedHistory
	if err := json.Unmarshal(buf.buf, &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(got.Tasks) != 0 {
		t.Errorf("expected empty tasks, got %d", len(got.Tasks))
	}
}

func TestToPersisted(t *testing.T) {
	ts := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	e := resultEntry{
		jobID:      "j1",
		response:   jobResponse{JobID: "j1", Success: true, Duration: "1s"},
		finishedAt: ts,
	}
	pe := toPersisted(e)
	if pe.JobID != "j1" || !pe.Response.Success || !pe.FinishedAt.Equal(ts) {
		t.Errorf("toPersisted mismatch: %+v", pe)
	}
}

func TestFromPersisted(t *testing.T) {
	ts := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	pe := persistedEntry{
		JobID:      "j1",
		Response:   jobResponse{JobID: "j1", Success: true, Duration: "1s"},
		FinishedAt: ts,
	}
	e := fromPersisted(pe)
	if e.jobID != "j1" || !e.response.Success || !e.finishedAt.Equal(ts) {
		t.Errorf("fromPersisted mismatch: %+v", e)
	}
}

// tempFileBuffer implements io.Writer and provides Sync/Close/Name for
// testing the save path without touching the filesystem.
type tempFileBuffer struct {
	name string
	buf  []byte
}

func (f *tempFileBuffer) Write(p []byte) (int, error) {
	f.buf = append(f.buf, p...)
	return len(p), nil
}

func (f *tempFileBuffer) Sync() error  { return nil }
func (f *tempFileBuffer) Close() error { return nil }
func (f *tempFileBuffer) Name() string { return f.name }
