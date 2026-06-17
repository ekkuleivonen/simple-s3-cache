package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ekkuleivonen/simple-s3-cache/internal/config"
	"github.com/ekkuleivonen/simple-s3-cache/internal/ops"
)

type Server struct {
	httpServer *http.Server
}

type readinessChecker interface {
	Readiness() (bool, string)
}

type readinessDetailChecker interface {
	ReadinessDetail() ops.Readiness
}

type peerStateProvider interface {
	PeerState() ops.PeerState
}

func New(cfg config.Config, logger *slog.Logger, handlers ...http.Handler) *Server {
	mux := http.NewServeMux()
	var readiness readinessChecker
	var readinessDetail readinessDetailChecker
	var peerState peerStateProvider
	if len(handlers) > 0 {
		readiness, _ = handlers[0].(readinessChecker)
		readinessDetail, _ = handlers[0].(readinessDetailChecker)
		peerState, _ = handlers[0].(peerStateProvider)
	}
	mux.HandleFunc("GET /healthz", healthz(readiness, readinessDetail))
	mux.HandleFunc("GET /readyz", readyz(readiness, readinessDetail))
	if len(handlers) > 1 && handlers[1] != nil {
		mux.Handle("GET /metrics", handlers[1])
	}
	if cfg.Operator.Enabled && peerState != nil {
		path := strings.TrimSpace(cfg.Operator.Path)
		mux.HandleFunc("GET "+path, operatorState(peerState, cfg.Operator.BearerToken))
	}
	if len(handlers) > 0 && handlers[0] != nil {
		mux.Handle("/", handlers[0])
	}

	handler := requestLogger(logger, mux, cfg.Logging)

	return &Server{
		httpServer: &http.Server{
			Addr:              cfg.Listen,
			Handler:           handler,
			ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
			ReadTimeout:       cfg.HTTP.ReadTimeout,
			WriteTimeout:      cfg.HTTP.WriteTimeout,
			IdleTimeout:       cfg.HTTP.IdleTimeout,
		},
	}
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func healthz(checker readinessChecker, detail readinessDetailChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if readiness := currentReadiness(checker, detail); !readiness.Ready {
			w.WriteHeader(http.StatusOK)
			writeReadinessJSON(w, "ok", readiness)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","ready":true}` + "\n"))
	}
}

func readyz(checker readinessChecker, detail readinessDetailChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if readiness := currentReadiness(checker, detail); !readiness.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeReadinessJSON(w, "not_ready", readiness)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}` + "\n"))
	}
}

func currentReadiness(checker readinessChecker, detail readinessDetailChecker) ops.Readiness {
	if detail != nil {
		readiness := detail.ReadinessDetail()
		if readiness.Ready {
			return ops.Readiness{Ready: true}
		}
		if readiness.Degraded == nil {
			readiness.Degraded = &ops.DegradedState{Reason: "degraded"}
		}
		return readiness
	}
	if checker != nil {
		if ok, reason := checker.Readiness(); !ok {
			return ops.Readiness{Ready: false, Degraded: &ops.DegradedState{Reason: reason}}
		}
	}
	return ops.Readiness{Ready: true}
}

func writeReadinessJSON(w http.ResponseWriter, status string, readiness ops.Readiness) {
	response := map[string]any{
		"status": status,
		"ready":  readiness.Ready,
	}
	if readiness.Degraded != nil {
		if readiness.Degraded.Reason != "" {
			response["reason"] = readiness.Degraded.Reason
		}
		if readiness.Degraded.Code != "" {
			response["reason_code"] = readiness.Degraded.Code
		}
		if !readiness.Degraded.Since.IsZero() {
			response["since"] = readiness.Degraded.Since.UTC().Format(time.RFC3339Nano)
		}
		if readiness.Degraded.PeerID != "" {
			response["peer_id"] = readiness.Degraded.PeerID
		}
		if readiness.Degraded.RingID != "" {
			response["ring_id"] = readiness.Degraded.RingID
		}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func operatorState(provider peerStateProvider, token string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if token = strings.TrimSpace(token); token != "" {
			if got := strings.TrimSpace(r.Header.Get("Authorization")); got != "Bearer "+token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(provider.PeerState())
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}

	n, err := w.ResponseWriter.Write(data)
	w.bytes += int64(n)
	return n, err
}

func (w *loggingResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if readerFrom, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err := readerFrom.ReadFrom(r)
		w.bytes += n
		return n, err
	}
	return io.Copy(loggingWriter{ResponseWriter: w}, r)
}

func (w *loggingResponseWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *loggingResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

type loggingWriter struct {
	ResponseWriter *loggingResponseWriter
}

func (w loggingWriter) Write(data []byte) (int, error) {
	return w.ResponseWriter.Write(data)
}

func requestLogger(logger *slog.Logger, next http.Handler, cfg config.LoggingConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &loggingResponseWriter{ResponseWriter: w}

		next.ServeHTTP(lw, r)

		status := lw.status
		if status == 0 {
			status = http.StatusOK
		}
		if !cfg.AccessLog {
			return
		}
		internalPeer := strings.HasPrefix(r.URL.Path, "/internal/v1/")
		if internalPeer && !cfg.InternalPeerAccessLog && status < http.StatusBadRequest {
			return
		}

		logger.InfoContext(r.Context(), "request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("query", r.URL.RawQuery),
			slog.Int("status", status),
			slog.Int64("bytes", lw.bytes),
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("user_agent", r.UserAgent()),
			slog.String("duration", time.Since(start).String()),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			slog.String("content_length", contentLength(r.ContentLength)),
			slog.Bool("internal_peer", internalPeer),
		)
	})
}

func contentLength(length int64) string {
	if length < 0 {
		return "unknown"
	}

	return strconv.FormatInt(length, 10)
}
