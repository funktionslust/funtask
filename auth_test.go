package funtask

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func okHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
	}
}

func TestRequireToken_NoHeader(t *testing.T) {
	handler := requireToken([]byte("secret"), okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	var body errorResponse
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body.Error != "unauthorized" {
		t.Errorf("error = %q, want %q", body.Error, "unauthorized")
	}
}

func TestRequireToken_WrongToken(t *testing.T) {
	handler := requireToken([]byte("secret"), okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	r.Header.Set("Authorization", "Bearer wrong-token")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	var body errorResponse
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body.Error != "unauthorized" {
		t.Errorf("error = %q, want %q", body.Error, "unauthorized")
	}
}

func TestRequireToken_ValidToken(t *testing.T) {
	handler := requireToken([]byte("secret"), okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	r.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	var body map[string]string
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body["ok"] != "true" {
		t.Errorf("handler not called, body = %v", body)
	}
}

func TestRequireToken_EmptyBearer(t *testing.T) {
	handler := requireToken([]byte("secret"), okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	r.Header.Set("Authorization", "Bearer ")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	var body errorResponse
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body.Error != "unauthorized" {
		t.Errorf("error = %q, want %q", body.Error, "unauthorized")
	}
}

func TestRequireToken_WrongScheme(t *testing.T) {
	handler := requireToken([]byte("secret"), okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	r.Header.Set("Authorization", "Basic c2VjcmV0")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	var body errorResponse
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body.Error != "unauthorized" {
		t.Errorf("error = %q, want %q", body.Error, "unauthorized")
	}
}

func TestValidateCallbackURL_ValidSchemeHostMatch(t *testing.T) {
	err := validateCallbackURL("https://hooks.example.com/webhook/123", []string{"https://hooks.example.com"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateCallbackURL_ValidWithPathPrefix(t *testing.T) {
	allowlist := []string{"https://hooks.example.com/hooks/"}

	// Should match — path starts with /hooks/
	if err := validateCallbackURL("https://hooks.example.com/hooks/abc", allowlist); err != nil {
		t.Errorf("unexpected error for matching path prefix: %v", err)
	}

	// Should reject — different path
	if err := validateCallbackURL("https://hooks.example.com/other/abc", allowlist); err == nil {
		t.Error("expected error for non-matching path prefix, got nil")
	}
}

func TestValidateCallbackURL_RejectDifferentHost(t *testing.T) {
	err := validateCallbackURL("https://evil.com/steal", []string{"https://hooks.example.com"})
	if err == nil {
		t.Error("expected error for different host, got nil")
	}
}

func TestValidateCallbackURL_RejectHostSuffix(t *testing.T) {
	err := validateCallbackURL("https://hooks.example.com.evil.com/steal", []string{"https://hooks.example.com"})
	if err == nil {
		t.Error("expected error for host suffix attack, got nil")
	}
}

func TestValidateCallbackURL_RejectDifferentScheme(t *testing.T) {
	err := validateCallbackURL("http://hooks.example.com/webhook", []string{"https://hooks.example.com"})
	if err == nil {
		t.Error("expected error for http vs https, got nil")
	}
}

func TestValidateCallbackURL_RejectUserInfo(t *testing.T) {
	err := validateCallbackURL("https://evil.com@hooks.example.com/webhook", []string{"https://hooks.example.com"})
	if err == nil {
		t.Error("expected error for user info bypass, got nil")
	}
}

func TestValidateCallbackURL_RejectMissingScheme(t *testing.T) {
	err := validateCallbackURL("hooks.example.com/webhook", []string{"https://hooks.example.com"})
	if err == nil {
		t.Error("expected error for missing scheme, got nil")
	}
}

func TestValidateCallbackURL_RejectEmptyURL(t *testing.T) {
	err := validateCallbackURL("", []string{"https://hooks.example.com"})
	if err == nil {
		t.Error("expected error for empty URL, got nil")
	}
}

func TestValidateCallbackURL_HostPortMatch(t *testing.T) {
	// Port included in allowlist — should match
	if err := validateCallbackURL("https://hooks.example.com:8443/webhook", []string{"https://hooks.example.com:8443"}); err != nil {
		t.Errorf("unexpected error for matching port: %v", err)
	}

	// Port in URL but not in allowlist — should reject (Host includes port)
	if err := validateCallbackURL("https://hooks.example.com:8443/webhook", []string{"https://hooks.example.com"}); err == nil {
		t.Error("expected error for port mismatch, got nil")
	}
}

func TestValidateCallbackURL_MultipleAllowlistEntries(t *testing.T) {
	allowlist := []string{
		"https://primary.example.com",
		"https://secondary.example.com",
	}
	// Matches second entry
	if err := validateCallbackURL("https://secondary.example.com/webhook/123", allowlist); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateCallbackURL_RejectNonHTTPScheme(t *testing.T) {
	err := validateCallbackURL("ftp://hooks.example.com/webhook", []string{"ftp://hooks.example.com"})
	if err == nil {
		t.Error("expected error for ftp scheme, got nil")
	}
}

func TestValidateCallbackURL_CaseInsensitiveSchemeAndHost(t *testing.T) {
	err := validateCallbackURL("HTTPS://HOOKS.EXAMPLE.COM/webhook", []string{"https://hooks.example.com"})
	if err != nil {
		t.Errorf("unexpected error for case-insensitive match: %v", err)
	}
}

func TestRequireToken_CaseSensitiveBearer(t *testing.T) {
	handler := requireToken([]byte("secret"), okHandler())
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	// "bearer" lowercase — RFC 6750 specifies "Bearer" with capital B
	r.Header.Set("Authorization", "bearer secret")
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 for lowercase 'bearer'", w.Code)
	}
	var body errorResponse
	_ = json.NewDecoder(w.Body).Decode(&body)
	if body.Error != "unauthorized" {
		t.Errorf("error = %q, want %q", body.Error, "unauthorized")
	}
}
