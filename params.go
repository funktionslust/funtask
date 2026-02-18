package funtask

import (
	"fmt"
	"strings"
	"time"
)

// Params holds the input parameters from the orchestrator. Since params
// arrive as JSON, all numbers are float64 internally. The typed accessors
// handle conversion — e.g. Int("limit") accepts float64(500) from JSON
// and returns int(500). Non-integer floats like 3.7 are rejected by Int().
type Params struct {
	m map[string]any
}

// String returns the string parameter for the given key.
// It returns an error if the key is missing or the value is not a string.
func (p Params) String(key string) (string, error) {
	v, ok := p.m[key]
	if !ok {
		return "", fmt.Errorf("missing parameter: %s", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("parameter %s: expected string, got %T", key, v)
	}
	return s, nil
}

// Int returns the integer parameter for the given key. JSON numbers
// arrive as float64, so this method accepts whole-number floats and
// converts them. Non-integer floats like 3.7 are rejected.
func (p Params) Int(key string) (int, error) {
	v, ok := p.m[key]
	if !ok {
		return 0, fmt.Errorf("missing parameter: %s", key)
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("parameter %s: expected int, got %T", key, v)
	}
	i := int(f)
	if float64(i) != f {
		return 0, fmt.Errorf("parameter %s: expected int, got float %v", key, f)
	}
	return i, nil
}

// Float returns the float64 parameter for the given key.
// It returns an error if the key is missing or the value is not a number.
func (p Params) Float(key string) (float64, error) {
	v, ok := p.m[key]
	if !ok {
		return 0, fmt.Errorf("missing parameter: %s", key)
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("parameter %s: expected float, got %T", key, v)
	}
	return f, nil
}

// Bool returns the boolean parameter for the given key.
// It returns an error if the key is missing or the value is not a bool.
func (p Params) Bool(key string) (bool, error) {
	v, ok := p.m[key]
	if !ok {
		return false, fmt.Errorf("missing parameter: %s", key)
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("parameter %s: expected bool, got %T", key, v)
	}
	return b, nil
}

// Time returns the time parameter for the given key. The value must be
// a string in RFC 3339 format (e.g. "2026-01-01T00:00:00Z").
func (p Params) Time(key string) (time.Time, error) {
	v, ok := p.m[key]
	if !ok {
		return time.Time{}, fmt.Errorf("missing parameter: %s", key)
	}
	s, ok := v.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("parameter %s: expected time string, got %T", key, v)
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parameter %s: invalid time format: %v", key, err)
	}
	return t, nil
}

// Raw returns the underlying parameter map. This is useful for custom
// parameter handling that goes beyond the typed accessors.
func (p Params) Raw() map[string]any {
	return p.m
}

// Reader returns a ParamReader that collects errors from multiple
// parameter reads, avoiding repetitive if-err-nil blocks.
func (p Params) Reader() *ParamReader {
	return &ParamReader{p: p}
}

// ParamReader reads parameters and collects errors. After the first
// error, subsequent reads still execute and collect additional errors
// (returning zero values). This way Err reports all invalid parameters
// at once, not just the first one.
type ParamReader struct {
	p    Params
	errs []string
}

// String returns the string parameter for the given key. If the key is
// missing or the value is not a string, the error is collected and an
// empty string is returned.
func (r *ParamReader) String(key string) string {
	v, err := r.p.String(key)
	if err != nil {
		r.errs = append(r.errs, key+" (expected string)")
	}
	return v
}

// Int returns the integer parameter for the given key. If the key is
// missing or the value is not an integer, the error is collected and
// zero is returned.
func (r *ParamReader) Int(key string) int {
	v, err := r.p.Int(key)
	if err != nil {
		r.errs = append(r.errs, key+" (expected int)")
	}
	return v
}

// Float returns the float64 parameter for the given key. If the key is
// missing or the value is not a number, the error is collected and zero
// is returned.
func (r *ParamReader) Float(key string) float64 {
	v, err := r.p.Float(key)
	if err != nil {
		r.errs = append(r.errs, key+" (expected float)")
	}
	return v
}

// Bool returns the boolean parameter for the given key. If the key is
// missing or the value is not a bool, the error is collected and false
// is returned.
func (r *ParamReader) Bool(key string) bool {
	v, err := r.p.Bool(key)
	if err != nil {
		r.errs = append(r.errs, key+" (expected bool)")
	}
	return v
}

// Time returns the time parameter for the given key. If the key is
// missing or the value is not a valid RFC 3339 string, the error is
// collected and the zero time is returned.
func (r *ParamReader) Time(key string) time.Time {
	v, err := r.p.Time(key)
	if err != nil {
		r.errs = append(r.errs, key+" (expected time)")
	}
	return v
}

// Err returns a combined error for all failed parameter reads, or nil
// if all reads succeeded. The error message lists all invalid
// parameters, e.g. "missing or invalid parameters: since (expected
// time), limit (expected int)".
func (r *ParamReader) Err() error {
	if len(r.errs) == 0 {
		return nil
	}
	return fmt.Errorf("missing or invalid parameters: %s", strings.Join(r.errs, ", "))
}
