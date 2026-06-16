package server

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/ekkuleivonen/simple-s3-cache/internal/config"
)

type Server struct {
	httpServer *http.Server
}

func New(cfg config.Config, logger *slog.Logger, proxyHandler ...http.Handler) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	if len(proxyHandler) > 0 && proxyHandler[0] != nil {
		mux.Handle("/", proxyHandler[0])
	}

	handler := requestLogger(logger, mux)

	return &Server{
		httpServer: &http.Server{
			Addr:              cfg.Listen,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *loggingResponseWriter) WriteHeader(status int) {
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

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &loggingResponseWriter{ResponseWriter: w}

		next.ServeHTTP(lw, r)

		status := lw.status
		if status == 0 {
			status = http.StatusOK
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
		)
	})
}

func contentLength(length int64) string {
	if length < 0 {
		return "unknown"
	}

	return strconv.FormatInt(length, 10)
}
