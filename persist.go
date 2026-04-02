package funtask

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

const persistVersion = 1

type persistedEntry struct {
	JobID      string      `json:"jobId"`
	Response   jobResponse `json:"response"`
	FinishedAt time.Time   `json:"finishedAt"`
}

type persistedHistory struct {
	Version int                         `json:"version"`
	Tasks   map[string][]persistedEntry `json:"tasks"`
}

type historyPersister struct {
	path   string
	logger *slog.Logger

	// Function fields for testability.
	readFunc   func(string) ([]byte, error)
	statFunc   func(string) (os.FileInfo, error)
	mkdirFunc  func(string, os.FileMode) error
	createFunc func(string) (syncWriteCloser, error)
	renameFunc func(string, string) error
}

func newHistoryPersister(path string, logger *slog.Logger) *historyPersister {
	return &historyPersister{
		path:       path,
		logger:     logger,
		readFunc:   os.ReadFile,
		statFunc:   os.Stat,
		mkdirFunc:  os.MkdirAll,
		createFunc: func(name string) (syncWriteCloser, error) { return os.Create(name) },
		renameFunc: os.Rename,
	}
}

// load reads and unmarshals the history file. If the file does not
// exist, it returns nil, nil (start with empty history). If the file
// is corrupt or unreadable, it returns an error.
func (p *historyPersister) load() (*persistedHistory, error) {
	_, err := p.statFunc(p.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", p.path, err)
	}
	data, err := p.readFunc(p.path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", p.path, err)
	}
	var ph persistedHistory
	if err := json.Unmarshal(data, &ph); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", p.path, err)
	}
	return &ph, nil
}

// populate merges loaded history into h. Only tasks that are
// registered in h receive entries; unknown tasks are discarded.
// Entries exceeding a task's limit are trimmed (oldest dropped).
func (p *historyPersister) populate(h *resultHistory, ph *persistedHistory) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for name, entries := range ph.Tasks {
		tr, ok := h.tasks[name]
		if !ok {
			continue
		}
		loaded := make([]resultEntry, len(entries))
		for i, e := range entries {
			loaded[i] = fromPersisted(e)
		}
		if len(loaded) > tr.limit {
			loaded = loaded[len(loaded)-tr.limit:]
		}
		tr.entries = loaded
	}
}

// syncWriteCloser is the subset of *os.File used by save.
type syncWriteCloser interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Name() string
}

// save writes the current history to disk atomically. It creates a
// temporary file in the same directory, writes JSON, syncs to disk,
// and renames over the target path.
func (p *historyPersister) save(h *resultHistory) error {
	dir := filepath.Dir(p.path)
	if err := p.mkdirFunc(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	tmp, err := p.createFunc(p.path + ".tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	if err := p.saveToWriter(tmp, h); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}

	if err := p.renameFunc(tmp.Name(), p.path); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("rename %s to %s: %w", tmp.Name(), p.path, err)
	}
	return nil
}

func (p *historyPersister) saveToWriter(w syncWriteCloser, h *resultHistory) error {
	ph := p.snapshot(h)
	data, err := json.MarshalIndent(ph, "", "  ")
	if err != nil {
		_ = w.Close()
		return fmt.Errorf("marshal history: %w", err)
	}
	data = append(data, '\n')

	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err := w.Sync(); err != nil {
		_ = w.Close()
		return fmt.Errorf("sync: %w", err)
	}
	return w.Close()
}

func (p *historyPersister) snapshot(h *resultHistory) persistedHistory {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ph := persistedHistory{
		Version: persistVersion,
		Tasks:   make(map[string][]persistedEntry, len(h.tasks)),
	}
	for name, tr := range h.tasks {
		if len(tr.entries) == 0 {
			continue
		}
		entries := make([]persistedEntry, len(tr.entries))
		for i, e := range tr.entries {
			entries[i] = toPersisted(e)
		}
		ph.Tasks[name] = entries
	}
	return ph
}

func toPersisted(e resultEntry) persistedEntry {
	return persistedEntry{
		JobID:      e.jobID,
		Response:   e.response,
		FinishedAt: e.finishedAt,
	}
}

func fromPersisted(e persistedEntry) resultEntry {
	return resultEntry{
		jobID:      e.JobID,
		response:   e.Response,
		finishedAt: e.FinishedAt,
	}
}
