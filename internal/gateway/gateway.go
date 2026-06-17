package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"
	"github.com/ekkuleivonen/simple-s3-cache/internal/peer"
	"github.com/ekkuleivonen/simple-s3-cache/internal/s3request"
)

var (
	errRouterNotConfigured      = errors.New("router_not_configured")
	errInvalidPath              = errors.New("invalid_path")
	errAmbiguousDeleteObjects   = errors.New("ambiguous_delete_objects")
	errDefaultPeerNotConfigured = errors.New("default_peer_not_configured")
	errInvalidPeerURL           = errors.New("invalid_peer_url")
	errCreatePeerRequest        = errors.New("create_peer_request_failed")
	errPeerRequestFailed        = errors.New("peer_request_failed")
)

type Options struct {
	Router         *peer.Router
	Client         *http.Client
	Logger         *slog.Logger
	Metrics        *metrics.Recorder
	ForwardTimeout time.Duration
}

type Gateway struct {
	router      *peer.Router
	client      *http.Client
	logger      *slog.Logger
	metrics     *metrics.Recorder
	defaultPeer peer.Peer
}

func New(opts Options) *Gateway {
	client := opts.Client
	if client == nil {
		timeout := opts.ForwardTimeout
		if timeout <= 0 {
			timeout = 10 * time.Minute
		}
		client = newHTTPClient(timeout)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	g := &Gateway{
		router:  opts.Router,
		client:  client,
		logger:  logger,
		metrics: opts.Metrics,
	}
	if opts.Router != nil {
		peers := opts.Router.Peers()
		if len(peers) > 0 {
			g.defaultPeer = peers[0]
		}
	}
	return g
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	stats := gatewayStats{
		method: r.Method,
		route:  "default",
	}
	status, bytesSent, err := g.serve(w, r, &stats)
	stats.status = status
	stats.bytesSent = bytesSent
	stats.duration = time.Since(start)
	if err != nil {
		stats.failure = failureReason(err)
	}
	g.recordMetrics(&stats)
	g.logRequest(r, &stats, err)
}

func (g *Gateway) serve(w http.ResponseWriter, r *http.Request, stats *gatewayStats) (int, int64, error) {
	if g.router == nil {
		http.Error(w, "gateway router is not configured", http.StatusInternalServerError)
		return http.StatusInternalServerError, 0, errRouterNotConfigured
	}
	target, ok := s3request.ParsePathStyle(r.URL.EscapedPath())
	if !ok && r.URL.EscapedPath() != "/" {
		http.Error(w, "invalid path-style S3 request", http.StatusBadRequest)
		return http.StatusBadRequest, 0, errInvalidPath
	}
	stats.bucket = target.Bucket

	owner, route, err := g.routePeer(r, target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return http.StatusBadRequest, 0, err
	}
	stats.route = route
	stats.peerID = owner.ID

	peerURL, err := peerRequestURL(owner, r.URL)
	if err != nil {
		http.Error(w, "invalid peer url", http.StatusBadGateway)
		return http.StatusBadGateway, 0, fmt.Errorf("%w: %v", errInvalidPeerURL, err)
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, peerURL.String(), r.Body)
	if err != nil {
		http.Error(w, "create peer request failed", http.StatusBadGateway)
		return http.StatusBadGateway, 0, fmt.Errorf("%w: %v", errCreatePeerRequest, err)
	}
	req.ContentLength = r.ContentLength
	req.Host = r.Host
	copyRequestHeaders(req.Header, r.Header)
	if route == "owner" {
		req.Header.Set(peer.ForwardedHeader, "1")
		req.Header.Set(peer.OwnerHeader, owner.ID)
		req.Header.Set(peer.FromHeader, "gateway")
	}

	headerStart := time.Now()
	resp, err := g.client.Do(req)
	stats.headerDuration = time.Since(headerStart)
	if err != nil {
		http.Error(w, "peer request failed", http.StatusBadGateway)
		return http.StatusBadGateway, 0, fmt.Errorf("%w: %v", errPeerRequestFailed, err)
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return resp.StatusCode, 0, nil
	}
	bodyReader := &timedBodyReader{src: resp.Body}
	copyStart := time.Now()
	n, copyErr := io.Copy(w, bodyReader)
	stats.copyDuration = time.Since(copyStart)
	stats.bodyReadDuration = bodyReader.duration
	stats.downstreamWriteDuration = stats.copyDuration - stats.bodyReadDuration
	if stats.downstreamWriteDuration < 0 {
		stats.downstreamWriteDuration = 0
	}
	stats.bodyReadChunks = bodyReader.chunks
	if copyErr != nil {
		return resp.StatusCode, n, copyErr
	}
	return resp.StatusCode, n, nil
}

func (g *Gateway) routePeer(r *http.Request, target s3request.Target) (peer.Peer, string, error) {
	if isAmbiguousDeleteObjects(r, target) {
		return peer.Peer{}, "rejected", errAmbiguousDeleteObjects
	}
	if target.IsObject() {
		return g.router.Owner(target.Bucket, target.Key), "owner", nil
	}
	if g.defaultPeer.ID == "" {
		return peer.Peer{}, "default", errDefaultPeerNotConfigured
	}
	return g.defaultPeer, "default", nil
}

func peerRequestURL(p peer.Peer, original *url.URL) (*url.URL, error) {
	base, err := url.Parse(p.URL)
	if err != nil {
		return nil, err
	}
	next := *base
	next.Path = joinURLPath(base.EscapedPath(), original.EscapedPath())
	next.RawQuery = original.RawQuery
	next.Fragment = ""
	return &next, nil
}

func joinURLPath(basePath, requestPath string) string {
	if basePath == "" || basePath == "/" {
		if requestPath == "" {
			return "/"
		}
		return requestPath
	}
	if requestPath == "" || requestPath == "/" {
		return basePath
	}
	return strings.TrimRight(basePath, "/") + "/" + strings.TrimLeft(requestPath, "/")
}

func isAmbiguousDeleteObjects(r *http.Request, target s3request.Target) bool {
	return r.Method == http.MethodPost && target.Bucket != "" && target.Key == "" && r.URL.Query().Has("delete")
}

func newHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

type timedBodyReader struct {
	src      io.Reader
	duration time.Duration
	chunks   int64
}

func (r *timedBodyReader) Read(data []byte) (int, error) {
	start := time.Now()
	n, err := r.src.Read(data)
	r.duration += time.Since(start)
	if n > 0 {
		r.chunks++
	}
	return n, err
}

func copyRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) || isPeerHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isPeerHeader(key string) bool {
	return strings.EqualFold(key, peer.ForwardedHeader) ||
		strings.EqualFold(key, peer.OwnerHeader) ||
		strings.EqualFold(key, peer.FromHeader)
}

func isHopByHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

type gatewayStats struct {
	bucket                  string
	method                  string
	route                   string
	peerID                  string
	status                  int
	bytesSent               int64
	duration                time.Duration
	headerDuration          time.Duration
	copyDuration            time.Duration
	bodyReadDuration        time.Duration
	downstreamWriteDuration time.Duration
	bodyReadChunks          int64
	failure                 string
}

func (g *Gateway) recordMetrics(stats *gatewayStats) {
	if g.metrics == nil {
		return
	}
	statusClass := statusClass(stats.status)
	g.metrics.RecordGatewayRequest(stats.bucket, stats.route, stats.peerID, stats.method, statusClass)
	if stats.failure != "" {
		g.metrics.RecordGatewayForwardFailure(stats.bucket, stats.peerID, stats.failure)
	}
	if stats.bytesSent > 0 {
		g.metrics.RecordGatewayResponseBytes(stats.bucket, stats.peerID, stats.bytesSent)
	}
	if stats.duration > 0 {
		g.metrics.ObserveGatewayForwardDuration(stats.bucket, stats.route, stats.peerID, statusClass, stats.duration)
	}
	if stats.headerDuration > 0 {
		g.metrics.ObserveGatewayResponseHeaderDuration(stats.bucket, stats.route, stats.peerID, statusClass, stats.headerDuration)
	}
	if stats.copyDuration > 0 {
		g.metrics.ObserveGatewayResponseCopyDuration(stats.bucket, stats.route, stats.peerID, statusClass, stats.copyDuration)
		g.metrics.ObserveGatewayResponseBodyReadDuration(stats.bucket, stats.route, stats.peerID, statusClass, stats.bodyReadDuration)
		g.metrics.ObserveGatewayDownstreamWriteDuration(stats.bucket, stats.route, stats.peerID, statusClass, stats.downstreamWriteDuration)
	}
}

func (g *Gateway) logRequest(r *http.Request, stats *gatewayStats, err error) {
	level := slog.LevelInfo
	if err != nil || stats.status >= 500 {
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("bucket", stats.bucket),
		slog.String("route", stats.route),
		slog.String("peer_id", stats.peerID),
		slog.Int("status", stats.status),
		slog.Int64("bytes", stats.bytesSent),
		slog.Int64("duration_ms", stats.duration.Milliseconds()),
		slog.Int64("peer_response_header_duration_ms", stats.headerDuration.Milliseconds()),
		slog.Int64("peer_response_copy_duration_ms", stats.copyDuration.Milliseconds()),
		slog.Int64("peer_response_body_read_duration_ms", stats.bodyReadDuration.Milliseconds()),
		slog.Int64("downstream_write_duration_ms", stats.downstreamWriteDuration.Milliseconds()),
		slog.Int64("peer_response_body_read_chunks", stats.bodyReadChunks),
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	g.logger.LogAttrs(context.Background(), level, "gateway_request", attrs...)
}

func statusClass(status int) string {
	if status <= 0 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}

func failureReason(err error) string {
	if err == nil {
		return ""
	}
	for _, known := range []error{
		errRouterNotConfigured,
		errInvalidPath,
		errAmbiguousDeleteObjects,
		errDefaultPeerNotConfigured,
		errInvalidPeerURL,
		errCreatePeerRequest,
		errPeerRequestFailed,
	} {
		if errors.Is(err, known) {
			return known.Error()
		}
	}
	return "unknown"
}
