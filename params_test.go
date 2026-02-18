package funtask

import (
	"strings"
	"testing"
	"time"
)

func TestParams_String(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		want    string
		wantErr string
	}{
		{
			name:   "valid string",
			params: map[string]any{"name": "test"},
			key:    "name",
			want:   "test",
		},
		{
			name:   "empty string value",
			params: map[string]any{"name": ""},
			key:    "name",
			want:   "",
		},
		{
			name:    "missing key",
			params:  map[string]any{},
			key:     "missing",
			wantErr: "missing parameter: missing",
		},
		{
			name:    "wrong type (int)",
			params:  map[string]any{"name": float64(42)},
			key:     "name",
			wantErr: "expected string, got float64",
		},
		{
			name:    "wrong type (bool)",
			params:  map[string]any{"name": true},
			key:     "name",
			wantErr: "expected string, got bool",
		},
		{
			name:    "nil value",
			params:  map[string]any{"name": nil},
			key:     "name",
			wantErr: "expected string, got <nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Params{m: tt.params}
			got, err := p.String(tt.key)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("String(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestParams_Int(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		want    int
		wantErr string
	}{
		{
			name:   "valid int from float64",
			params: map[string]any{"count": float64(5)},
			key:    "count",
			want:   5,
		},
		{
			name:   "zero value",
			params: map[string]any{"count": float64(0)},
			key:    "count",
			want:   0,
		},
		{
			name:   "negative int",
			params: map[string]any{"offset": float64(-10)},
			key:    "offset",
			want:   -10,
		},
		{
			name:   "large int",
			params: map[string]any{"big": float64(1000000)},
			key:    "big",
			want:   1000000,
		},
		{
			name:    "missing key",
			params:  map[string]any{},
			key:     "count",
			wantErr: "missing parameter: count",
		},
		{
			name:    "wrong type (string)",
			params:  map[string]any{"count": "five"},
			key:     "count",
			wantErr: "expected int, got string",
		},
		{
			name:    "non-integer float",
			params:  map[string]any{"rate": float64(3.7)},
			key:     "rate",
			wantErr: "expected int, got float 3.7",
		},
		{
			name:    "negative non-integer float",
			params:  map[string]any{"offset": float64(-3.7)},
			key:     "offset",
			wantErr: "expected int, got float -3.7",
		},
		{
			name:    "wrong type (bool)",
			params:  map[string]any{"count": true},
			key:     "count",
			wantErr: "expected int, got bool",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Params{m: tt.params}
			got, err := p.Int(tt.key)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Int(%q) = %d, want %d", tt.key, got, tt.want)
			}
		})
	}
}

func TestParams_Float(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		want    float64
		wantErr string
	}{
		{
			name:   "valid float",
			params: map[string]any{"rate": float64(1.5)},
			key:    "rate",
			want:   1.5,
		},
		{
			name:   "whole number float",
			params: map[string]any{"rate": float64(5)},
			key:    "rate",
			want:   5.0,
		},
		{
			name:   "zero value",
			params: map[string]any{"rate": float64(0)},
			key:    "rate",
			want:   0.0,
		},
		{
			name:    "missing key",
			params:  map[string]any{},
			key:     "rate",
			wantErr: "missing parameter: rate",
		},
		{
			name:    "wrong type (string)",
			params:  map[string]any{"rate": "fast"},
			key:     "rate",
			wantErr: "expected float, got string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Params{m: tt.params}
			got, err := p.Float(tt.key)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Float(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestParams_Bool(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		want    bool
		wantErr string
	}{
		{
			name:   "true value",
			params: map[string]any{"enabled": true},
			key:    "enabled",
			want:   true,
		},
		{
			name:   "false value",
			params: map[string]any{"enabled": false},
			key:    "enabled",
			want:   false,
		},
		{
			name:    "missing key",
			params:  map[string]any{},
			key:     "enabled",
			wantErr: "missing parameter: enabled",
		},
		{
			name:    "wrong type (string)",
			params:  map[string]any{"enabled": "true"},
			key:     "enabled",
			wantErr: "expected bool, got string",
		},
		{
			name:    "wrong type (number)",
			params:  map[string]any{"enabled": float64(1)},
			key:     "enabled",
			wantErr: "expected bool, got float64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Params{m: tt.params}
			got, err := p.Bool(tt.key)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Bool(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestParams_Time(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		want    time.Time
		wantErr string
	}{
		{
			name:   "valid RFC3339",
			params: map[string]any{"deadline": "2026-01-01T00:00:00Z"},
			key:    "deadline",
			want:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "RFC3339 with offset",
			params: map[string]any{"deadline": "2026-06-15T14:30:00+02:00"},
			key:    "deadline",
			want:   time.Date(2026, 6, 15, 14, 30, 0, 0, time.FixedZone("", 2*60*60)),
		},
		{
			name:    "missing key",
			params:  map[string]any{},
			key:     "deadline",
			wantErr: "missing parameter: deadline",
		},
		{
			name:    "wrong type (number)",
			params:  map[string]any{"deadline": float64(1234567890)},
			key:     "deadline",
			wantErr: "expected time string, got float64",
		},
		{
			name:    "invalid time string",
			params:  map[string]any{"deadline": "not-a-time"},
			key:     "deadline",
			wantErr: "invalid time format",
		},
		{
			name:    "non-RFC3339 format",
			params:  map[string]any{"deadline": "2026-01-01"},
			key:     "deadline",
			wantErr: "invalid time format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Params{m: tt.params}
			got, err := p.Time(tt.key)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("Time(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestParams_Raw(t *testing.T) {
	m := map[string]any{"name": "test", "count": float64(5)}
	p := Params{m: m}
	raw := p.Raw()

	if raw == nil {
		t.Fatal("Raw() returned nil")
	}
	if raw["name"] != "test" {
		t.Errorf("Raw()[name] = %v, want test", raw["name"])
	}
	if raw["count"] != float64(5) {
		t.Errorf("Raw()[count] = %v, want 5", raw["count"])
	}

	// Verify it returns the same map, not a copy
	raw["extra"] = "added"
	if p.Raw()["extra"] != "added" {
		t.Error("Raw() should return the underlying map, not a copy")
	}
}

func TestParamReader(t *testing.T) {
	p := Params{m: map[string]any{
		"name":    "test",
		"count":   float64(10),
		"rate":    float64(1.5),
		"enabled": true,
		"since":   "2026-01-01T00:00:00Z",
	}}

	r := p.Reader()
	name := r.String("name")
	count := r.Int("count")
	rate := r.Float("rate")
	enabled := r.Bool("enabled")
	since := r.Time("since")

	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "test" {
		t.Errorf("String(name) = %q, want test", name)
	}
	if count != 10 {
		t.Errorf("Int(count) = %d, want 10", count)
	}
	if rate != 1.5 {
		t.Errorf("Float(rate) = %v, want 1.5", rate)
	}
	if !enabled {
		t.Error("Bool(enabled) = false, want true")
	}
	wantTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !since.Equal(wantTime) {
		t.Errorf("Time(since) = %v, want %v", since, wantTime)
	}
}

func TestParamReader_CollectsAllErrors(t *testing.T) {
	p := Params{m: map[string]any{
		"name": float64(42), // wrong type for string
	}}

	r := p.Reader()
	s := r.String("name")      // type mismatch
	i := r.Int("missing_int")  // missing key
	f := r.Float("missing_f")  // missing key
	b := r.Bool("missing_b")   // missing key
	tm := r.Time("missing_tm") // missing key

	// M1: Verify zero values returned on error
	if s != "" {
		t.Errorf("String on error should return \"\", got %q", s)
	}
	if i != 0 {
		t.Errorf("Int on error should return 0, got %d", i)
	}
	if f != 0.0 {
		t.Errorf("Float on error should return 0.0, got %v", f)
	}
	if b != false {
		t.Errorf("Bool on error should return false, got %v", b)
	}
	if !tm.IsZero() {
		t.Errorf("Time on error should return zero time, got %v", tm)
	}

	err := r.Err()
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	msg := err.Error()
	if !strings.Contains(msg, "missing or invalid parameters:") {
		t.Errorf("error should start with 'missing or invalid parameters:', got %q", msg)
	}
	if !strings.Contains(msg, "name (expected string)") {
		t.Errorf("error should mention name, got %q", msg)
	}
	if !strings.Contains(msg, "missing_int (expected int)") {
		t.Errorf("error should mention missing_int, got %q", msg)
	}
	// M2: Verify Float error is collected
	if !strings.Contains(msg, "missing_f (expected float)") {
		t.Errorf("error should mention missing_f, got %q", msg)
	}
	if !strings.Contains(msg, "missing_b (expected bool)") {
		t.Errorf("error should mention missing_b, got %q", msg)
	}
	if !strings.Contains(msg, "missing_tm (expected time)") {
		t.Errorf("error should mention missing_tm, got %q", msg)
	}

	// L3: Verify errors appear in read order
	want := "missing or invalid parameters: name (expected string), missing_int (expected int), missing_f (expected float), missing_b (expected bool), missing_tm (expected time)"
	if msg != want {
		t.Errorf("error message ordering:\n got: %q\nwant: %q", msg, want)
	}
}

func TestParamReader_MixedValidAndInvalid(t *testing.T) {
	p := Params{m: map[string]any{
		"name":  "test",
		"count": "not_a_number",
		"rate":  float64(1.5),
		"flag":  float64(0), // wrong type for bool
	}}

	r := p.Reader()
	name := r.String("name")
	_ = r.Int("count")
	rate := r.Float("rate")
	_ = r.Bool("flag")

	err := r.Err()
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Valid reads should return correct values
	if name != "test" {
		t.Errorf("String(name) = %q, want test", name)
	}
	if rate != 1.5 {
		t.Errorf("Float(rate) = %v, want 1.5", rate)
	}

	// Error should only mention invalid params
	msg := err.Error()
	if !strings.Contains(msg, "count (expected int)") {
		t.Errorf("error should mention count, got %q", msg)
	}
	if !strings.Contains(msg, "flag (expected bool)") {
		t.Errorf("error should mention flag, got %q", msg)
	}
	if strings.Contains(msg, "name") {
		t.Errorf("error should NOT mention valid param name, got %q", msg)
	}
	if strings.Contains(msg, "rate") {
		t.Errorf("error should NOT mention valid param rate, got %q", msg)
	}
}

func TestParams_NilMap(t *testing.T) {
	p := Params{} // zero value, m is nil

	t.Run("String returns missing error", func(t *testing.T) {
		_, err := p.String("key")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "missing parameter: key") {
			t.Errorf("error = %q, want 'missing parameter: key'", err)
		}
	})

	t.Run("Int returns missing error", func(t *testing.T) {
		_, err := p.Int("key")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("Raw returns nil", func(t *testing.T) {
		if p.Raw() != nil {
			t.Error("Raw() on nil map should return nil")
		}
	})

	t.Run("Reader works on nil map", func(t *testing.T) {
		r := p.Reader()
		_ = r.String("key")
		err := r.Err()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestParamReader_DuplicateKey(t *testing.T) {
	p := Params{m: map[string]any{}}
	r := p.Reader()
	_ = r.Int("count")
	_ = r.Int("count") // duplicate read of same key

	err := r.Err()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Both reads should produce errors
	msg := err.Error()
	if strings.Count(msg, "count (expected int)") != 2 {
		t.Errorf("expected 2 occurrences of 'count (expected int)', got: %q", msg)
	}
}

func TestParamReader_NoReads(t *testing.T) {
	p := Params{m: map[string]any{"name": "test"}}
	r := p.Reader()
	if err := r.Err(); err != nil {
		t.Errorf("Err() with no reads should be nil, got %v", err)
	}
}
