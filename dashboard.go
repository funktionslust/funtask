package funtask

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
)

//go:embed dashboard.html
var dashboardFS embed.FS

var (
	dashboardData []byte
	dashboardETag string
)

// init pre-computes the dashboard bytes and ETag. The //go:embed directive
// causes a compile error if dashboard.html is absent, so ReadFile cannot
// fail in a successfully-built binary.
func init() {
	data, err := fs.ReadFile(dashboardFS, "dashboard.html")
	if err != nil {
		panic("funtask: embedded dashboard.html missing: " + err.Error())
	}
	// Inject content hash as version identifier, replacing the dev placeholder.
	raw := sha256.Sum256(data)
	ver := fmt.Sprintf("%x", raw[:4])
	placeholder := []byte(`data-version="dev"`)
	if !bytes.Contains(data, placeholder) {
		panic(`funtask: dashboard.html missing data-version="dev" placeholder`)
	}
	dashboardData = bytes.Replace(data, placeholder, []byte(`data-version="`+ver+`"`), 1)
	h := sha256.Sum256(dashboardData)
	dashboardETag = fmt.Sprintf(`"%x"`, h[:8])
}

func serveDashboard(w http.ResponseWriter, r *http.Request) {
	if match := r.Header.Get("If-None-Match"); match == dashboardETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("ETag", dashboardETag)
	_, _ = w.Write(dashboardData)
}
