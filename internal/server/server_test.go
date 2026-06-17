package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if got := rec.Body.String(); got != "{\"status\":\"ok\",\"ready\":true}\n" {
		t.Fatalf("body = %q", got)
	}
}

func TestHealthzStaysLiveWhenReadinessFails(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	srv := New(config.Config{Listen: ":0"}, logger, failingReadiness{reason: "peer ring mismatch"})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); got != "{\"status\":\"ok\",\"ready\":false,\"reason\":\"peer ring mismatch\"}\n" {
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

func TestReadyzFailsWhenCheckerIsNotReady(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	srv := New(config.Config{Listen: ":0"}, logger, failingReadiness{reason: "local invalidation failed"})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Body.String(); got != "{\"status\":\"not_ready\",\"reason\":\"local invalidation failed\"}\n" {
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

func TestLoggingResponseWriterPreservesReaderFrom(t *testing.T) {
	base := &readerFromResponseWriter{header: http.Header{}}
	w := &loggingResponseWriter{ResponseWriter: base}

	n, err := w.ReadFrom(strings.NewReader("from optimized path"))
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}

	if n != int64(len("from optimized path")) {
		t.Fatalf("ReadFrom() bytes = %d, want %d", n, len("from optimized path"))
	}
	if base.readFromCalls != 1 {
		t.Fatalf("underlying ReadFrom calls = %d, want 1", base.readFromCalls)
	}
	if w.bytes != n {
		t.Fatalf("logged bytes = %d, want %d", w.bytes, n)
	}
	if w.status != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.status, http.StatusOK)
	}
}

func TestLoggingResponseWriterPreservesFlush(t *testing.T) {
	base := &flushingResponseWriter{readerFromResponseWriter: readerFromResponseWriter{header: http.Header{}}}
	w := &loggingResponseWriter{ResponseWriter: base}

	w.Flush()

	if base.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", base.flushes)
	}
	if w.status != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.status, http.StatusOK)
	}
}

type readerFromResponseWriter struct {
	header        http.Header
	body          bytes.Buffer
	status        int
	readFromCalls int
}

func (w *readerFromResponseWriter) Header() http.Header {
	return w.header
}

func (w *readerFromResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *readerFromResponseWriter) Write(data []byte) (int, error) {
	return w.body.Write(data)
}

func (w *readerFromResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	w.readFromCalls++
	return io.Copy(&w.body, r)
}

type flushingResponseWriter struct {
	readerFromResponseWriter
	flushes int
}

func (w *flushingResponseWriter) Flush() {
	w.flushes++
}

type failingReadiness struct {
	reason string
}

func (r failingReadiness) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (r failingReadiness) Readiness() (bool, string) {
	return false, r.reason
}
