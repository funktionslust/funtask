package funtask

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// validateCallbackURL checks that rawURL matches at least one entry
// in the allowlist. Comparison is by parsed URL components (scheme,
// host, optional path prefix) — not raw string prefix.
func validateCallbackURL(rawURL string, allowlist []string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("callback URL not allowed")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("callback URL not allowed")
	}
	if u.Host == "" {
		return fmt.Errorf("callback URL not allowed")
	}
	if u.User != nil {
		return fmt.Errorf("callback URL not allowed")
	}
	host := strings.ToLower(u.Host)
	for _, entry := range allowlist {
		allowed, err := url.Parse(entry)
		if err != nil {
			continue
		}
		if strings.ToLower(allowed.Scheme) != scheme {
			continue
		}
		if strings.ToLower(allowed.Host) != host {
			continue
		}
		if allowed.Path != "" && allowed.Path != "/" {
			if !strings.HasPrefix(u.Path, allowed.Path) {
				continue
			}
			// Enforce path segment boundary: /hooks must not match /hooks-evil
			if !strings.HasSuffix(allowed.Path, "/") && len(u.Path) > len(allowed.Path) && u.Path[len(allowed.Path)] != '/' {
				continue
			}
		}
		return nil
	}
	return fmt.Errorf("callback URL not allowed")
}

// requireToken returns middleware that enforces bearer token
// authentication. Requests without a valid token receive 401.
func requireToken(token string, next http.Handler) http.Handler {
	tokenBytes := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if len(auth) < len(prefix) || auth[:len(prefix)] != prefix {
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "unauthorized"})
			return
		}
		provided := []byte(auth[len(prefix):])
		if subtle.ConstantTimeCompare(provided, tokenBytes) != 1 {
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
