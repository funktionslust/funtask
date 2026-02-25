package funtask

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// NOTE on SSE authentication: EventSource cannot send custom headers,
// so /events accepts a ?token= query parameter as a fallback. This
// means the bearer token may appear in browser history and proxy logs.
// The server's own structured logging does NOT log query strings.

// eventBroker fans out notifications to SSE clients. Each client gets a
// buffered(1) channel; notify drops the send if the buffer is full,
// which naturally coalesces rapid-fire updates into a single wakeup.
type eventBroker struct {
	mu      sync.Mutex
	clients map[chan struct{}]struct{}
}

// subscribe returns a notification channel and registers it with the broker.
func (b *eventBroker) subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// unsubscribe removes a client channel from the broker.
func (b *eventBroker) unsubscribe(ch chan struct{}) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
}

// notify sends a non-blocking signal to all subscribed clients.
// The mutex is held during iteration; each send is non-blocking (buffered
// channel), so lock duration is O(n) with negligible per-client cost.
// Acceptable for the expected client count (dashboard users).
func (b *eventBroker) notify() {
	b.mu.Lock()
	for ch := range b.clients {
		select {
		case ch <- struct{}{}:
		default: // buffer full — coalesced
		}
	}
	b.mu.Unlock()
}

// handleEvents streams Server-Sent Events to the client. Each event is a
// full health snapshot (same JSON as GET /health). Auth accepts either the
// Authorization header or a ?token= query parameter because EventSource
// cannot send custom headers.
func (f *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if f.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "server is shutting down"})
		return
	}

	// Dual-mode auth: header (shared helper) or query-param fallback.
	authorized := checkBearerHeader(r, f.tokenBytes)
	if !authorized {
		if tok := r.URL.Query().Get("token"); tok != "" {
			authorized = subtle.ConstantTimeCompare([]byte(tok), f.tokenBytes) == 1
		}
	}
	if !authorized {
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "unauthorized"})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := f.events.subscribe()
	defer f.events.unsubscribe(ch)

	// Helper: build and send one SSE data frame.
	sendSnapshot := func() error {
		data, err := json.Marshal(f.buildHealthResponse())
		if err != nil {
			f.logger.Error("SSE snapshot marshal failed", "error", err)
			return err
		}
		_, err = fmt.Fprintf(w, "data: %s\n\n", data)
		if err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	// Send initial snapshot immediately. Events carry no id: field because
	// each frame is a full health snapshot — clients reconnect with a fresh
	// state rather than replaying missed deltas.
	if err := sendSnapshot(); err != nil {
		return
	}

	keepalive := time.NewTicker(30 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ch:
			if err := sendSnapshot(); err != nil {
				return
			}
		case <-keepalive.C:
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
