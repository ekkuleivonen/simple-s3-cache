package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ekkuleivonen/simple-s3-cache/internal/config"
	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"
)

func TestHealthz(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	srv := New(config.Config{Listen: ":0"}, logger)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := rec.Body.String(); got != "{\"status\":\"ok\"}\n" {
		t.Fatalf("body = %q", got)
	}
}

func TestReadyz(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	srv := New(config.Config{Listen: ":0"}, logger)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := rec.Body.String(); got != "{\"status\":\"ready\"}\n" {
		t.Fatalf("body = %q", got)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	recorder := metrics.NewRecorder(4096)
	recorder.RecordPageHit("bucket")
	srv := New(config.Config{Listen: ":0"}, logger, nil, recorder.Handler())

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); !bytes.Contains([]byte(got), []byte(`simple_s3_cache_page_hits_total{bucket="bucket"} 1`)) {
		t.Fatalf("metrics body missing page hit:\n%s", got)
	}
}

func TestServerAppliesConfiguredTimeouts(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := config.Default()
	cfg.Listen = ":0"
	cfg.HTTP.ReadHeaderTimeout = 2 * time.Second
	cfg.HTTP.ReadTimeout = 3 * time.Second
	cfg.HTTP.WriteTimeout = 4 * time.Second
	cfg.HTTP.IdleTimeout = 5 * time.Second

	srv := New(cfg, logger)

	if srv.httpServer.ReadHeaderTimeout != 2*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want 2s", srv.httpServer.ReadHeaderTimeout)
	}
	if srv.httpServer.ReadTimeout != 3*time.Second {
		t.Fatalf("ReadTimeout = %s, want 3s", srv.httpServer.ReadTimeout)
	}
	if srv.httpServer.WriteTimeout != 4*time.Second {
		t.Fatalf("WriteTimeout = %s, want 4s", srv.httpServer.WriteTimeout)
	}
	if srv.httpServer.IdleTimeout != 5*time.Second {
		t.Fatalf("IdleTimeout = %s, want 5s", srv.httpServer.IdleTimeout)
	}
}

func TestRequestLoggerWritesStructuredLog(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	srv := New(config.Config{Listen: ":0"}, logger)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(rec, req)

	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatalf("decode log entry: %v\nlog: %s", err, logs.String())
	}

	if entry["msg"] != "request" {
		t.Fatalf("msg = %v, want request", entry["msg"])
	}
	if entry["method"] != http.MethodGet {
		t.Fatalf("method = %v, want GET", entry["method"])
	}
	if entry["path"] != "/healthz" {
		t.Fatalf("path = %v, want /healthz", entry["path"])
	}
	if entry["status"] != float64(http.StatusOK) {
		t.Fatalf("status = %v, want %d", entry["status"], http.StatusOK)
	}
}
