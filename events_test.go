package funtask

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEvents_RequiresAuth(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestEvents_AuthViaHeader(t *testing.T) {
	f := testServer(t)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/events", nil)
	req.Header.Set("Authorization", "Bearer test-secret")

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q, want %q", ct, "text/event-stream")
	}
}

func TestEvents_AuthViaQueryParam(t *testing.T) {
	f := testServer(t)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(srv.URL + "/events?token=test-secret")
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestEvents_InvalidToken(t *testing.T) {
	srv := startTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events?token=wrong")
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestEvents_StreamsInitialSnapshot(t *testing.T) {
	f := testServer(t)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/events?token=test-secret", nil)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer closeBody(resp)

	scanner := bufio.NewScanner(resp.Body)
	deadline := time.After(2 * time.Second)
	var dataLine string
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for initial SSE event")
		default:
		}
		if !scanner.Scan() {
			t.Fatalf("scanner ended: %v", scanner.Err())
		}
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			dataLine = line[len("data: "):]
			break
		}
	}

	var health healthResponse
	if err := json.Unmarshal([]byte(dataLine), &health); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if health.Name != "test-server" {
		t.Errorf("name = %q, want %q", health.Name, "test-server")
	}
	if health.Status != "ok" {
		t.Errorf("status = %q, want %q", health.Status, "ok")
	}
	if _, ok := health.Tasks["echo"]; !ok {
		t.Error("tasks should contain 'echo'")
	}
}

func TestEvents_NotifySendsUpdate(t *testing.T) {
	f := testServer(t)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/events?token=test-secret", nil)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer closeBody(resp)

	scanner := bufio.NewScanner(resp.Body)

	// Read initial snapshot.
	readEvent := func() string {
		deadline := time.After(3 * time.Second)
		for {
			select {
			case <-deadline:
				return ""
			default:
			}
			if !scanner.Scan() {
				return ""
			}
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				return line[len("data: "):]
			}
		}
	}

	initial := readEvent()
	if initial == "" {
		t.Fatal("no initial event received")
	}

	// Trigger a notification.
	f.events.notify()

	// Read the update event.
	update := readEvent()
	if update == "" {
		t.Fatal("no update event received after notify")
	}

	var health healthResponse
	if err := json.Unmarshal([]byte(update), &health); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if health.Name != "test-server" {
		t.Errorf("name = %q, want %q", health.Name, "test-server")
	}
}

func TestEventBroker_NotifyCoalesces(t *testing.T) {
	b := &eventBroker{clients: make(map[chan struct{}]struct{})}
	ch := b.subscribe()
	defer b.unsubscribe(ch)

	// Two rapid notifies should coalesce into one signal.
	b.notify()
	b.notify()

	select {
	case <-ch:
	default:
		t.Fatal("expected signal on channel")
	}

	// Channel should be empty after one read.
	select {
	case <-ch:
		t.Fatal("expected empty channel after single read (coalescing)")
	default:
	}
}

func TestEventBroker_MultipleClients(t *testing.T) {
	b := &eventBroker{clients: make(map[chan struct{}]struct{})}
	ch1 := b.subscribe()
	ch2 := b.subscribe()
	defer b.unsubscribe(ch1)
	defer b.unsubscribe(ch2)

	b.notify()

	select {
	case <-ch1:
	default:
		t.Error("client 1 did not receive notification")
	}
	select {
	case <-ch2:
	default:
		t.Error("client 2 did not receive notification")
	}
}

func TestEvents_DrainingRejects(t *testing.T) {
	f := testServer(t)
	f.draining.Store(true)
	srv := httptest.NewServer(f.routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events?token=test-secret")
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestEvents_ReservedRoute(t *testing.T) {
	t.Setenv("FUNTASK_AUTH_TOKEN", "")
	t.Setenv("FUNTASK_DEAD_LETTER_DIR", "")
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	f := New("n",
		Task("t", dummyTask),
		WithAuthToken("secret"),
		WithDeadLetterDir("/tmp/dl"),
		WithHandler("GET /events", h),
	)
	err := f.validate()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "/events") {
		t.Errorf("error = %q, want mention of /events", err.Error())
	}
}
