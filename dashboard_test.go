package funtask

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestWithDashboard_ServesHTML(t *testing.T) {
	ts := testServerWithTasks(t,
		Task("echo", dummyTask),
		WithDashboard(),
	)
	defer ts.Close()

	// Dashboard HTML is served without auth — the page itself handles
	// auth client-side by prompting for a token and calling /health.
	resp, err := http.DefaultClient.Get(ts.URL + "/dashboard")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content-type = %q, want text/html", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "funtask") {
		t.Error("body does not contain 'funtask' marker")
	}
	if !strings.Contains(string(body), "data-version") {
		t.Error("body does not contain version identifier")
	}
}

func TestWithDashboard_ConflictsWithCustomHandler(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	f := New("n",
		Task("echo", dummyTask),
		WithAuthToken("secret"),
		WithDeadLetterDir("/tmp/dl"),
		WithDashboard(),
		WithHandler("GET /dashboard", h),
	)
	err := f.validate()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "/dashboard") {
		t.Errorf("error = %q, want mention of /dashboard", err.Error())
	}
}

func TestWithDashboard_ReservedRoute(t *testing.T) {
	// /dashboard is reserved only when WithDashboard() is enabled,
	// validated via the dashboard-specific check in validate().
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	f := New("n",
		Task("echo", dummyTask),
		WithAuthToken("secret"),
		WithDeadLetterDir("/tmp/dl"),
		WithDashboard(),
		WithHandler("GET /dashboard", h),
	)
	err := f.validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "dashboard") {
		t.Errorf("error = %q, want mention of dashboard", err.Error())
	}

	// Without WithDashboard(), /dashboard is NOT reserved.
	f2 := New("n",
		Task("echo", dummyTask),
		WithAuthToken("secret"),
		WithDeadLetterDir("/tmp/dl"),
		WithHandler("GET /dashboard", h),
	)
	if err := f2.validate(); err != nil {
		t.Errorf("unexpected error without WithDashboard(): %v", err)
	}
}

func TestWithoutDashboard_NoRoute(t *testing.T) {
	ts := testServerWithTasks(t,
		Task("echo", dummyTask),
	)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/dashboard", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode == http.StatusOK {
		t.Error("expected non-200 for dashboard without WithDashboard()")
	}
}

func TestWithDashboard_ETag(t *testing.T) {
	ts := testServerWithTasks(t,
		Task("echo", dummyTask),
		WithDashboard(),
	)
	defer ts.Close()

	resp, err := http.DefaultClient.Get(ts.URL + "/dashboard")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer closeBody(resp)

	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q", cc, "no-cache")
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("ETag header is empty")
	}

	// Second request with If-None-Match should return 304.
	req2, _ := http.NewRequest("GET", ts.URL+"/dashboard", nil)
	req2.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer closeBody(resp2)

	if resp2.StatusCode != http.StatusNotModified {
		t.Errorf("status = %d, want 304", resp2.StatusCode)
	}
}
