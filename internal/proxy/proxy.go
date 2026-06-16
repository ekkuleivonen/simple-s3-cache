package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/ekkuleivonen/simple-s3-cache/internal/cache"
	"github.com/ekkuleivonen/simple-s3-cache/internal/cacheplan"
	appconfig "github.com/ekkuleivonen/simple-s3-cache/internal/config"
	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"
	peerrouter "github.com/ekkuleivonen/simple-s3-cache/internal/peer"
	"github.com/ekkuleivonen/simple-s3-cache/internal/s3request"
)

const unsignedPayload = "UNSIGNED-PAYLOAD"

const (
	peerForwardedHeader = "X-Simple-S3-Cache-Peer-Forwarded"
	peerOwnerHeader     = "X-Simple-S3-Cache-Peer-Owner"
	peerFromHeader      = "X-Simple-S3-Cache-Peer-From"
)

const (
	upstreamDialTimeout           = 10 * time.Second
	upstreamKeepAlive             = 30 * time.Second
	upstreamTLSHandshakeTimeout   = 10 * time.Second
	upstreamExpectContinueTimeout = time.Second
	upstreamIdleConnTimeout       = 90 * time.Second
	upstreamMaxIdleConns          = 100
	upstreamMaxIdleConnsPerHost   = 100
)

var (
	errObjectChanged             = errors.New("cached object changed upstream")
	errMetadataStore             = errors.New("cache metadata store failed")
	errSpoolLimit                = errors.New("upload body exceeds max spool size")
	errRefetchedRangeUnsatisfied = errors.New("refetched object does not satisfy requested range")
)

type uploadOptions struct {
	spoolPath    string
	maxSpoolSize int64
}

type cacheStore interface {
	PutObject(context.Context, cache.ObjectMetadata) (cache.Object, error)
	GetObject(context.Context, string, string) (cache.Object, bool, error)
	DeleteObject(context.Context, string, string) error
	StorePage(context.Context, cache.PageWrite) (cache.Page, error)
	ListPages(context.Context, string) ([]cache.Page, error)
	OpenPage(context.Context, string, int64, string, int64) (io.ReadCloser, bool, error)
	Close() error
}

type Proxy struct {
	upstreamEndpoint *url.URL
	region           string
	credentials      aws.CredentialsProvider
	signer           *v4.Signer
	client           *http.Client
	logger           *slog.Logger
	cache            cacheStore
	pageSize         int64
	pageSizeByBucket map[string]int64
	upload           uploadOptions
	metrics          *metrics.Recorder
	peerRouter       *peerrouter.Router
	peerClient       *http.Client
	peerTimeout      time.Duration
	pageFillMu       sync.Mutex
	pageFills        map[pageFillKey]*pageFillCall
}

type pageFillKey struct {
	objectID string
	index    int64
	etag     string
	epoch    int64
}

type pageFillCall struct {
	done chan struct{}
	err  error
}

type requestStats struct {
	method               string
	bucket               string
	key                  string
	cacheResult          string
	requestedRange       string
	bytesRequested       int64
	pagesRequested       int64
	pagesHit             int64
	pagesMissed          int64
	bytesSent            int64
	bytesFetchedUpstream int64
	upstreamDuration     time.Duration
	cacheServeDuration   time.Duration
	status               int
	peerMode             string
	peerLocalID          string
	peerOwnerID          string
	peerDecision         string
	peerForwarded        bool
	peerForwardFailure   string
	peerForwardDuration  time.Duration
}

func New(ctx context.Context, cfg appconfig.Config, logger *slog.Logger) (*Proxy, error) {
	upstreamEndpoint, err := url.Parse(cfg.Upstream.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse upstream endpoint: %w", err)
	}

	recorder := metrics.NewRecorder(cfg.Cache.MaxSize)
	cacheStore, err := cache.Open(ctx, cache.Options{
		CachePath:                cfg.Cache.CachePath,
		MetaPath:                 cfg.Cache.MetaPath,
		MaxSize:                  cfg.Cache.MaxSize,
		MaxSizeByBucket:          maxSizeByBucket(cfg.Cache.Buckets),
		MetadataGCInterval:       cfg.Cache.MetadataGCInterval,
		MetadataMaxAge:           cfg.Cache.MetadataMaxAge,
		MetadataGCBatchSize:      cfg.Cache.MetadataGCBatchSize,
		SQLiteCheckpointInterval: cfg.Cache.SQLiteCheckpointInterval,
		Metrics:                  recorder,
	})
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}

	credentialProvider := credentials.NewStaticCredentialsProvider(
		cfg.Upstream.AccessKey,
		cfg.Upstream.SecretKey,
		cfg.Upstream.SessionToken,
	)
	if _, err := credentialProvider.Retrieve(ctx); err != nil {
		_ = cacheStore.Close()
		return nil, fmt.Errorf("load upstream credentials: %w", err)
	}

	var router *peerrouter.Router
	if cfg.Peer.Mode == "peer" {
		router, err = newPeerRouter(cfg.Peer)
		if err != nil {
			_ = cacheStore.Close()
			return nil, fmt.Errorf("create peer router: %w", err)
		}
	}

	return &Proxy{
		upstreamEndpoint: upstreamEndpoint,
		region:           cfg.Upstream.Region,
		credentials:      credentialProvider,
		signer:           v4.NewSigner(),
		client:           newUpstreamHTTPClient(cfg.Upstream),
		logger:           logger,
		cache:            cacheStore,
		pageSize:         cfg.Cache.PageSize,
		pageSizeByBucket: pageSizeByBucket(cfg.Cache.Buckets),
		upload: uploadOptions{
			spoolPath:    cfg.Upload.SpoolPath,
			maxSpoolSize: cfg.Upload.MaxSpoolSize,
		},
		metrics:     recorder,
		peerRouter:  router,
		peerClient:  newPeerHTTPClient(cfg.Peer.ForwardTimeout),
		peerTimeout: cfg.Peer.ForwardTimeout,
	}, nil
}

func newUpstreamHTTPClient(cfg appconfig.UpstreamConfig) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   upstreamDialTimeout,
			KeepAlive: upstreamKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          upstreamMaxIdleConns,
		MaxIdleConnsPerHost:   upstreamMaxIdleConnsPerHost,
		IdleConnTimeout:       upstreamIdleConnTimeout,
		TLSHandshakeTimeout:   upstreamTLSHandshakeTimeout,
		ExpectContinueTimeout: upstreamExpectContinueTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
	return &http.Client{Transport: transport}
}

func newPeerRouter(cfg appconfig.PeerConfig) (*peerrouter.Router, error) {
	peers := make([]peerrouter.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, peerrouter.Peer{ID: p.ID, URL: p.URL})
	}
	return peerrouter.NewRouter(cfg.LocalID, peers)
}

func newPeerHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   upstreamDialTimeout,
			KeepAlive: upstreamKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          upstreamMaxIdleConns,
		MaxIdleConnsPerHost:   upstreamMaxIdleConnsPerHost,
		IdleConnTimeout:       upstreamIdleConnTimeout,
		TLSHandshakeTimeout:   upstreamTLSHandshakeTimeout,
		ExpectContinueTimeout: upstreamExpectContinueTimeout,
		ResponseHeaderTimeout: timeout,
	}
	return &http.Client{Transport: transport}
}

func maxSizeByBucket(buckets map[string]appconfig.BucketCacheConfig) map[string]int64 {
	out := make(map[string]int64)
	for bucket, cfg := range buckets {
		if cfg.MaxSize > 0 {
			out[bucket] = cfg.MaxSize
		}
	}
	return out
}

func pageSizeByBucket(buckets map[string]appconfig.BucketCacheConfig) map[string]int64 {
	out := make(map[string]int64)
	for bucket, cfg := range buckets {
		if cfg.PageSize > 0 {
			out[bucket] = cfg.PageSize
		}
	}
	return out
}

func (p *Proxy) MetricsHandler() http.Handler {
	if p.metrics == nil {
		return http.NotFoundHandler()
	}
	return p.metrics.Handler()
}

func (p *Proxy) Close() error {
	if p.cache == nil {
		return nil
	}
	return p.cache.Close()
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target, ok := s3request.ParsePathStyle(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}

	classification := s3request.Classify(s3request.Request{
		Method:   r.Method,
		Target:   target,
		RawQuery: r.URL.RawQuery,
		Header:   r.Header,
	})

	stats := newRequestStats(r, target)
	if p.shouldForwardToPeer(w, r, target, stats) {
		return
	}
	start := time.Now()
	status, bytesWritten, err := p.handle(w, r, target, classification, stats)
	stats.status = status
	stats.bytesSent = bytesWritten
	if stats.cacheResult == "" {
		stats.cacheResult = "pass_through"
	}
	p.recordMetrics(stats)
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("bucket", target.Bucket),
		slog.String("key", target.Key),
		slog.String("classification", string(classification.Disposition)),
		slog.String("classification_reason", classification.Reason),
		slog.String("cache_result", stats.cacheResult),
		slog.String("requested_range", stats.requestedRange),
		slog.Int64("bytes_requested", stats.bytesRequested),
		slog.Int64("pages_requested", stats.pagesRequested),
		slog.Int64("pages_hit", stats.pagesHit),
		slog.Int64("pages_missed", stats.pagesMissed),
		slog.Int("status", status),
		slog.Int64("bytes_sent", bytesWritten),
		slog.Int64("bytes_fetched_upstream", stats.bytesFetchedUpstream),
		slog.Float64("read_amplification", stats.readAmplification()),
		slog.Int("upstream_status", status),
		slog.Int64("upstream_duration_ms", stats.upstreamDuration.Milliseconds()),
		slog.Int64("request_duration_ms", time.Since(start).Milliseconds()),
		slog.Int64("upstream_bytes", stats.bytesFetchedUpstream),
	}
	if stats.peerMode != "" {
		attrs = append(attrs,
			slog.String("peer_mode", stats.peerMode),
			slog.String("peer_local_id", stats.peerLocalID),
			slog.String("peer_owner_id", stats.peerOwnerID),
			slog.Bool("peer_forwarded", stats.peerForwarded),
			slog.Int64("peer_forward_duration_ms", stats.peerForwardDuration.Milliseconds()),
		)
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		p.logger.LogAttrs(r.Context(), slog.LevelError, "proxy request failed", attrs...)
		return
	}

	p.logger.LogAttrs(r.Context(), slog.LevelInfo, "proxy request", attrs...)
}

func (p *Proxy) shouldForwardToPeer(w http.ResponseWriter, r *http.Request, target s3request.Target, stats *requestStats) bool {
	if p.peerRouter == nil || !target.IsObject() {
		return false
	}

	owner := p.peerRouter.Owner(target.Bucket, target.Key)
	stats.peerMode = "peer"
	stats.peerLocalID = p.peerRouter.LocalID()
	stats.peerOwnerID = owner.ID
	if owner.ID == p.peerRouter.LocalID() {
		stats.peerDecision = "local"
		p.recordPeerMetrics(stats)
		return false
	}

	if r.Header.Get(peerForwardedHeader) != "" {
		stats.peerForwarded = true
		stats.cacheResult = "peer_routing_mismatch"
		stats.peerForwardFailure = "routing_mismatch"
		p.recordPeerMetrics(stats)
		http.Error(w, "peer routing mismatch", http.StatusBadGateway)
		return true
	}

	stats.peerForwarded = true
	stats.cacheResult = "peer_forward"
	stats.peerDecision = "remote"
	start := time.Now()
	status, bytesWritten, err := p.forwardToPeer(w, r, owner)
	stats.peerForwardDuration = time.Since(start)
	stats.status = status
	stats.bytesSent = bytesWritten
	if err != nil {
		stats.peerForwardFailure = "request_failed"
		p.recordPeerMetrics(stats)
		p.logger.ErrorContext(r.Context(), "peer forward failed",
			slog.String("bucket", target.Bucket),
			slog.String("key", target.Key),
			slog.String("peer_owner_id", owner.ID),
			slog.String("error", err.Error()),
		)
		return true
	}
	p.recordPeerMetrics(stats)
	p.logger.InfoContext(r.Context(), "peer request forwarded",
		slog.String("method", r.Method),
		slog.String("bucket", target.Bucket),
		slog.String("key", target.Key),
		slog.Int("status", status),
		slog.Int64("bytes_sent", bytesWritten),
		slog.String("peer_local_id", p.peerRouter.LocalID()),
		slog.String("peer_owner_id", owner.ID),
		slog.Int64("peer_forward_duration_ms", stats.peerForwardDuration.Milliseconds()),
	)
	return true
}

func (p *Proxy) handle(w http.ResponseWriter, r *http.Request, target s3request.Target, classification s3request.Classification, stats *requestStats) (int, int64, error) {
	if p.cache == nil {
		stats.cacheResult = "pass_through"
		return p.forward(w, r, stats)
	}

	switch classification.Disposition {
	case s3request.CacheableHeadObject:
		return p.serveCachedHead(w, r, target, stats)
	case s3request.CacheableRangeObject:
		return p.serveCachedRange(w, r, target, stats)
	case s3request.CacheableFullObject:
		return p.serveCachedFullObject(w, r, target, stats)
	default:
		stats.cacheResult = "pass_through"
		status, bytesWritten, err := p.forward(w, r, stats)
		if err == nil && isSuccessfulStatus(status) && shouldInvalidateAfterWrite(r, target) {
			if deleteErr := p.cache.DeleteObject(r.Context(), target.Bucket, target.Key); deleteErr != nil {
				p.logger.WarnContext(r.Context(), "cache invalidation failed after successful write",
					slog.String("bucket", target.Bucket),
					slog.String("key", target.Key),
					slog.String("error", deleteErr.Error()),
				)
			} else if p.metrics != nil {
				p.metrics.RecordInvalidation(target.Bucket)
			}
		}
		return status, bytesWritten, err
	}
}

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, stats *requestStats) (int, int64, error) {
	upstreamURL := p.upstreamURL(r)
	body, contentLength, getBody, cleanup, decodedAWSChunked, err := forwardBody(r, p.upload)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		if errors.Is(err, errSpoolLimit) {
			http.Error(w, "upload body exceeds max spool size", http.StatusRequestEntityTooLarge)
			return 0, 0, err
		}
		http.Error(w, "prepare upstream request body", http.StatusBadGateway)
		return 0, 0, err
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL.String(), body)
	if err != nil {
		http.Error(w, "build upstream request", http.StatusInternalServerError)
		return 0, 0, err
	}
	req.ContentLength = contentLength
	req.GetBody = getBody
	copyRequestHeaders(req.Header, r.Header)
	if decodedAWSChunked {
		removeAWSChunkedHeaders(req.Header)
	}
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)

	if err := p.sign(req); err != nil {
		http.Error(w, "sign upstream request", http.StatusBadGateway)
		return 0, 0, err
	}
	if getBody != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		req.Body, err = getBody()
		if err != nil {
			http.Error(w, "prepare upstream request body", http.StatusBadGateway)
			return 0, 0, err
		}
		if seeker, ok := req.Body.(io.Seeker); ok {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				http.Error(w, "prepare upstream request body", http.StatusBadGateway)
				return 0, 0, err
			}
		}
		req.ContentLength = contentLength
		req.GetBody = getBody
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		stats.upstreamDuration += time.Since(start)
		p.recordUpstreamFailure(stats.bucket, "pass_through")
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return 0, 0, err
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	if r.Method == http.MethodHead {
		stats.upstreamDuration += time.Since(start)
		return resp.StatusCode, 0, nil
	}

	bytesWritten, copyErr := io.Copy(w, resp.Body)
	stats.upstreamDuration += time.Since(start)
	if copyErr != nil {
		p.recordUpstreamFailure(stats.bucket, "pass_through")
		return resp.StatusCode, bytesWritten, copyErr
	}

	return resp.StatusCode, bytesWritten, nil
}

func (p *Proxy) forwardToPeer(w http.ResponseWriter, r *http.Request, owner peerrouter.Peer) (int, int64, error) {
	peerURL := strings.TrimRight(owner.URL, "/") + r.URL.RequestURI()
	ctx := r.Context()
	if p.peerTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.peerTimeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, r.Method, peerURL, r.Body)
	if err != nil {
		http.Error(w, "build peer request", http.StatusInternalServerError)
		return 0, 0, err
	}
	req.ContentLength = r.ContentLength
	copyPeerRequestHeaders(req.Header, r.Header)
	if p.peerRouter != nil {
		req.Header.Set(peerFromHeader, p.peerRouter.LocalID())
	}
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, owner.ID)

	resp, err := p.peerClient.Do(req)
	if err != nil {
		http.Error(w, "peer request failed", http.StatusBadGateway)
		return 0, 0, err
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return resp.StatusCode, 0, nil
	}
	bytesWritten, copyErr := io.Copy(w, resp.Body)
	return resp.StatusCode, bytesWritten, copyErr
}

func forwardBody(r *http.Request, opts uploadOptions) (io.Reader, int64, func() (io.ReadCloser, error), func(), bool, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, 0, nil, nil, false, nil
	}
	if isAWSChunked(r.Header) {
		if decodedLength, ok := decodedContentLength(r.Header); ok {
			return newAWSChunkedDecodeReader(r.Body), decodedLength, nil, nil, true, nil
		}
		return spoolForwardBody(r.Body, decodeAWSChunkedBody, true, opts)
	}
	if r.ContentLength >= 0 {
		return r.Body, r.ContentLength, nil, nil, false, nil
	}

	return spoolForwardBody(r.Body, io.Copy, false, opts)
}

func spoolForwardBody(src io.Reader, copyBody func(io.Writer, io.Reader) (int64, error), decodedAWSChunked bool, opts uploadOptions) (io.Reader, int64, func() (io.ReadCloser, error), func(), bool, error) {
	if opts.maxSpoolSize <= 0 {
		return nil, 0, nil, nil, false, errSpoolLimit
	}
	if opts.spoolPath != "" {
		if err := os.MkdirAll(opts.spoolPath, 0o700); err != nil {
			return nil, 0, nil, nil, false, err
		}
	}
	tmp, err := os.CreateTemp(opts.spoolPath, "simple-s3-cache-upload-*")
	if err != nil {
		return nil, 0, nil, nil, false, err
	}
	cleanup := func() {
		name := tmp.Name()
		_ = tmp.Close()
		_ = os.Remove(name)
	}

	limited := &limitedSpoolWriter{dst: tmp, limit: opts.maxSpoolSize}
	size, err := copyBody(limited, src)
	if err != nil {
		cleanup()
		return nil, 0, nil, nil, false, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, nil, nil, false, err
	}
	getBody := func() (io.ReadCloser, error) {
		return io.NopCloser(io.NewSectionReader(tmp, 0, size)), nil
	}

	body := io.NopCloser(io.NewSectionReader(tmp, 0, size))
	return body, size, getBody, cleanup, decodedAWSChunked, nil
}

type limitedSpoolWriter struct {
	dst     io.Writer
	limit   int64
	written int64
}

func (w *limitedSpoolWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-w.written {
		allowed := w.limit - w.written
		if allowed > 0 {
			n, err := w.dst.Write(p[:allowed])
			w.written += int64(n)
			if err != nil {
				return n, err
			}
			return n, errSpoolLimit
		}
		return 0, errSpoolLimit
	}
	n, err := w.dst.Write(p)
	w.written += int64(n)
	return n, err
}

type awsChunkedDecodeReader struct {
	src        io.ReadCloser
	reader     *bufio.Reader
	remaining  int64
	done       bool
	pendingErr error
}

func newAWSChunkedDecodeReader(src io.ReadCloser) io.ReadCloser {
	return &awsChunkedDecodeReader{
		src:    src,
		reader: bufio.NewReader(src),
	}
}

func (r *awsChunkedDecodeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.pendingErr != nil {
		err := r.pendingErr
		r.pendingErr = nil
		return 0, err
	}
	if r.done {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		if err := r.nextChunk(); err != nil {
			return 0, err
		}
		if r.done {
			return 0, io.EOF
		}
	}

	want := minInt64(int64(len(p)), r.remaining)
	n, err := io.ReadFull(r.reader, p[:want])
	r.remaining -= int64(n)
	if err != nil {
		return n, err
	}
	if r.remaining == 0 {
		if err := readCRLF(r.reader); err != nil {
			if n > 0 {
				r.pendingErr = err
				return n, nil
			}
			return 0, err
		}
	}
	return n, nil
}

func (r *awsChunkedDecodeReader) Close() error {
	return r.src.Close()
}

func (r *awsChunkedDecodeReader) nextChunk() error {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	sizeText, _, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(sizeText, 16, 64)
	if err != nil {
		return fmt.Errorf("parse aws-chunked size %q: %w", sizeText, err)
	}
	if size == 0 {
		if err := discardAWSChunkedTrailers(r.reader); err != nil {
			return err
		}
		r.done = true
		return nil
	}
	r.remaining = size
	return nil
}

func decodedContentLength(header http.Header) (int64, bool) {
	value := strings.TrimSpace(header.Get("X-Amz-Decoded-Content-Length"))
	if value == "" {
		return 0, false
	}
	length, err := strconv.ParseInt(value, 10, 64)
	if err != nil || length < 0 {
		return 0, false
	}
	return length, true
}

func decodeAWSChunkedBody(dst io.Writer, src io.Reader) (int64, error) {
	reader := bufio.NewReader(src)
	var written int64
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return written, err
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		sizeText, _, _ := strings.Cut(line, ";")
		size, err := strconv.ParseInt(sizeText, 16, 64)
		if err != nil {
			return written, fmt.Errorf("parse aws-chunked size %q: %w", sizeText, err)
		}
		if size == 0 {
			if err := discardAWSChunkedTrailers(reader); err != nil {
				return written, err
			}
			return written, nil
		}
		n, err := io.CopyN(dst, reader, size)
		written += n
		if err != nil {
			return written, err
		}
		if err := readCRLF(reader); err != nil {
			return written, err
		}
	}
}

func discardAWSChunkedTrailers(reader *bufio.Reader) error {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if line == "\r\n" || line == "\n" {
			return nil
		}
	}
}

func readCRLF(reader *bufio.Reader) error {
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if line != "\r\n" {
		return fmt.Errorf("invalid aws-chunked chunk terminator %q", line)
	}
	return nil
}

func isAWSChunked(header http.Header) bool {
	for _, value := range header.Values("Content-Encoding") {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "aws-chunked") {
				return true
			}
		}
	}
	return false
}

func removeAWSChunkedHeaders(header http.Header) {
	var encodings []string
	for _, value := range header.Values("Content-Encoding") {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" && !strings.EqualFold(part, "aws-chunked") {
				encodings = append(encodings, part)
			}
		}
	}
	header.Del("Content-Encoding")
	if len(encodings) > 0 {
		header.Set("Content-Encoding", strings.Join(encodings, ", "))
	}
	header.Del("X-Amz-Decoded-Content-Length")
	header.Del("X-Amz-Trailer")
}

func shouldInvalidateAfterWrite(r *http.Request, target s3request.Target) bool {
	if !target.IsObject() {
		return false
	}

	query := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		if query.Get("uploadId") != "" && query.Get("partNumber") != "" {
			return false
		}
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			return true
		}
		return query.Get("uploadId") == "" && query.Get("partNumber") == ""
	case http.MethodDelete:
		return true
	case http.MethodPost:
		return query.Get("uploadId") != ""
	default:
		return false
	}
}

func isSuccessfulStatus(status int) bool {
	return status >= http.StatusOK && status < http.StatusMultipleChoices
}

func newRequestStats(r *http.Request, target s3request.Target) *requestStats {
	return &requestStats{
		method:         r.Method,
		bucket:         target.Bucket,
		key:            target.Key,
		requestedRange: r.Header.Get("Range"),
	}
}

func (s *requestStats) readAmplification() float64 {
	if s.bytesRequested <= 0 {
		return 0
	}
	return float64(s.bytesFetchedUpstream) / float64(s.bytesRequested)
}

func (s *requestStats) finishCachedResult() {
	if s.pagesRequested == 0 {
		return
	}
	switch {
	case s.pagesMissed == 0:
		s.cacheResult = "hit"
	case s.pagesHit == 0:
		s.cacheResult = "miss"
	default:
		s.cacheResult = "mixed"
	}
}

func (p *Proxy) recordMetrics(stats *requestStats) {
	if p.metrics == nil {
		return
	}
	switch stats.cacheResult {
	case "pass_through", "fallback":
		p.metrics.RecordPassThrough(stats.bucket, stats.method)
		if stats.bytesSent > 0 {
			p.metrics.RecordBytesServedFromUpstream(stats.bucket, stats.bytesSent)
		}
	case "hit", "mixed", "miss":
		if stats.bytesSent > 0 {
			p.metrics.RecordBytesServedFromCache(stats.bucket, stats.bytesSent)
		}
	}
	if stats.bytesRequested > 0 {
		p.metrics.ObserveRequestedBytes(stats.bucket, stats.bytesRequested)
		p.metrics.ObserveReadAmplification(stats.bucket, stats.readAmplification())
	}
	if stats.pagesRequested > 0 {
		p.metrics.ObservePagesTouched(stats.bucket, stats.pagesRequested)
	}
	if stats.cacheServeDuration > 0 {
		p.metrics.ObserveCacheServeDuration(stats.bucket, stats.cacheServeDuration)
	}
}

func (p *Proxy) recordPeerMetrics(stats *requestStats) {
	if p.metrics == nil {
		return
	}
	if stats.peerDecision != "" {
		p.metrics.RecordPeerDecision(stats.bucket, stats.peerDecision, stats.peerOwnerID)
	}
	if !stats.peerForwarded {
		return
	}
	if stats.peerForwardFailure != "" {
		p.metrics.RecordPeerForwardFailure(stats.bucket, stats.peerOwnerID, stats.peerForwardFailure)
	}
	statusClass := peerStatusClass(stats.status)
	if stats.peerForwardDuration > 0 {
		p.metrics.ObservePeerForwardDuration(stats.bucket, stats.peerOwnerID, statusClass, stats.peerForwardDuration)
	}
	if stats.peerForwardFailure == "" {
		p.metrics.RecordPeerForward(stats.bucket, stats.peerOwnerID, stats.method, statusClass)
		if stats.bytesSent > 0 {
			p.metrics.RecordPeerForwardResponseBytes(stats.bucket, stats.peerOwnerID, stats.bytesSent)
		}
	}
}

func peerStatusClass(status int) string {
	if status < 100 {
		return "error"
	}
	return strconv.Itoa(status/100) + "xx"
}

func (p *Proxy) recordUpstreamFailure(bucket, operation string) {
	if p.metrics != nil {
		p.metrics.RecordUpstreamFailure(bucket, operation)
	}
}

func (p *Proxy) serveCachedHead(w http.ResponseWriter, r *http.Request, target s3request.Target, stats *requestStats) (int, int64, error) {
	obj, ok, err := p.cache.GetObject(r.Context(), target.Bucket, target.Key)
	if err != nil {
		stats.cacheResult = "error"
		http.Error(w, "read cached metadata", http.StatusInternalServerError)
		return 0, 0, err
	}
	if !ok {
		stats.cacheResult = "miss"
		obj, status, headers, ok, err := p.fetchMetadata(r.Context(), r, target, stats)
		if err != nil {
			if errors.Is(err, errMetadataStore) {
				stats.cacheResult = "fallback"
				return p.forward(w, r, stats)
			}
			stats.cacheResult = "error"
			http.Error(w, "fetch upstream metadata", http.StatusBadGateway)
			return 0, 0, err
		}
		if !ok {
			copyResponseHeaders(w.Header(), headers)
			w.WriteHeader(status)
			return status, 0, nil
		}
		writeCachedObjectHeaders(w.Header(), obj, false)
		w.WriteHeader(http.StatusOK)
		return http.StatusOK, 0, nil
	}

	stats.cacheResult = "hit"
	if status, ok, err := cachedConditionalStatus(r, obj); err != nil {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	} else if ok {
		writeCachedConditionalHeaders(w.Header(), obj)
		w.WriteHeader(status)
		return status, 0, nil
	}
	writeCachedObjectHeaders(w.Header(), obj, false)
	w.WriteHeader(http.StatusOK)
	return http.StatusOK, 0, nil
}

func (p *Proxy) serveCachedRange(w http.ResponseWriter, r *http.Request, target s3request.Target, stats *requestStats) (int, int64, error) {
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target, stats)
	if err != nil {
		if errors.Is(err, errMetadataStore) {
			stats.cacheResult = "fallback"
			return p.forward(w, r, stats)
		}
		stats.cacheResult = "error"
		http.Error(w, "fetch upstream metadata", http.StatusBadGateway)
		return 0, 0, err
	}
	if !ok {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	}
	if status, ok, err := cachedConditionalStatus(r, obj); err != nil {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	} else if ok {
		stats.cacheResult = "hit"
		writeCachedConditionalHeaders(w.Header(), obj)
		w.WriteHeader(status)
		return status, 0, nil
	}

	byteRange, err := cacheplan.ParseRange(r.Header.Get("Range"), obj.Size)
	if err != nil {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	}
	stats.bytesRequested = byteRange.End - byteRange.Start + 1

	pages, firstPage, err := p.prepareFirstPage(r, target, obj, byteRange, stats)
	if errors.Is(err, errObjectChanged) {
		obj, byteRange, pages, firstPage, err = p.refetchAfterObjectChanged(r, target, byteRange, stats)
		stats.bytesRequested = byteRange.End - byteRange.Start + 1
	}
	if errors.Is(err, errRefetchedRangeUnsatisfied) {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	}
	if err != nil {
		stats.cacheResult = "error"
		http.Error(w, "fetch upstream page", http.StatusBadGateway)
		return 0, 0, err
	}
	stats.pagesRequested = int64(len(pages))

	writeCachedObjectHeaders(w.Header(), obj, true)
	w.Header().Set("Content-Length", strconv.FormatInt(byteRange.End-byteRange.Start+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, obj.Size))
	w.WriteHeader(http.StatusPartialContent)

	start := time.Now()
	bytesWritten, err := p.streamCachedPages(w, r, target, obj, pages, firstPage, stats)
	stats.cacheServeDuration += time.Since(start)
	stats.finishCachedResult()
	return http.StatusPartialContent, bytesWritten, err
}

func (p *Proxy) serveCachedFullObject(w http.ResponseWriter, r *http.Request, target s3request.Target, stats *requestStats) (int, int64, error) {
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target, stats)
	if err != nil {
		if errors.Is(err, errMetadataStore) {
			stats.cacheResult = "fallback"
			return p.forward(w, r, stats)
		}
		stats.cacheResult = "error"
		http.Error(w, "fetch upstream metadata", http.StatusBadGateway)
		return 0, 0, err
	}
	if !ok {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	}
	if status, ok, err := cachedConditionalStatus(r, obj); err != nil {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	} else if ok {
		stats.cacheResult = "hit"
		writeCachedConditionalHeaders(w.Header(), obj)
		w.WriteHeader(status)
		return status, 0, nil
	}
	stats.bytesRequested = obj.Size
	if obj.Size == 0 {
		stats.cacheResult = "hit"
		writeCachedObjectHeaders(w.Header(), obj, false)
		w.WriteHeader(http.StatusOK)
		return http.StatusOK, 0, nil
	}

	noCachedPages, err := p.objectHasNoCachedPages(r.Context(), obj)
	if err != nil {
		stats.cacheResult = "error"
		http.Error(w, "read cached pages", http.StatusInternalServerError)
		return 0, 0, err
	}
	if noCachedPages {
		status, bytesWritten, err := p.serveColdFullObject(w, r, target, obj, stats)
		if errors.Is(err, errObjectChanged) {
			if deleteErr := p.cache.DeleteObject(r.Context(), target.Bucket, target.Key); deleteErr != nil {
				stats.cacheResult = "error"
				http.Error(w, "invalidate changed object", http.StatusInternalServerError)
				return 0, 0, deleteErr
			}
			if p.metrics != nil {
				p.metrics.RecordInvalidation(target.Bucket)
			}
			refetched, ok, fetchErr := p.ensureObjectMetadata(r.Context(), r, target, stats)
			if fetchErr != nil {
				if errors.Is(fetchErr, errMetadataStore) {
					stats.cacheResult = "fallback"
					return p.forward(w, r, stats)
				}
				stats.cacheResult = "error"
				http.Error(w, "fetch upstream metadata", http.StatusBadGateway)
				return 0, 0, fetchErr
			}
			if !ok {
				stats.cacheResult = "fallback"
				return p.forward(w, r, stats)
			}
			obj = refetched
			stats.bytesRequested = obj.Size
			status, bytesWritten, err = p.serveColdFullObject(w, r, target, obj, stats)
			if errors.Is(err, errObjectChanged) {
				stats.cacheResult = "error"
				http.Error(w, "upstream object changed during fetch", http.StatusBadGateway)
				return 0, 0, err
			}
		}
		return status, bytesWritten, err
	}

	byteRange := cacheplan.ByteRange{Start: 0, End: obj.Size - 1}
	pages, firstPage, err := p.prepareFirstPage(r, target, obj, byteRange, stats)
	if errors.Is(err, errObjectChanged) {
		obj, byteRange, pages, firstPage, err = p.refetchAfterObjectChanged(r, target, byteRange, stats)
		stats.bytesRequested = obj.Size
	}
	if errors.Is(err, errRefetchedRangeUnsatisfied) {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	}
	if err != nil {
		stats.cacheResult = "error"
		http.Error(w, "fetch upstream page", http.StatusBadGateway)
		return 0, 0, err
	}
	stats.pagesRequested = int64(len(pages))

	writeCachedObjectHeaders(w.Header(), obj, false)
	w.WriteHeader(http.StatusOK)
	start := time.Now()
	bytesWritten, err := p.streamCachedPages(w, r, target, obj, pages, firstPage, stats)
	stats.cacheServeDuration += time.Since(start)
	stats.finishCachedResult()
	return http.StatusOK, bytesWritten, err
}

func (p *Proxy) objectHasNoCachedPages(ctx context.Context, obj cache.Object) (bool, error) {
	pages, err := p.cache.ListPages(ctx, obj.ID)
	if err != nil {
		return false, err
	}
	return len(pages) == 0, nil
}

func (p *Proxy) serveColdFullObject(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, stats *requestStats) (int, int64, error) {
	req, err := p.newUpstreamRequest(r.Context(), r, http.MethodGet, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Del("Range")
	req.Header.Set("If-Match", obj.ETag)

	pages, err := cacheplan.PagesForRange(cacheplan.ByteRange{Start: 0, End: obj.Size - 1}, obj.PageSize)
	if err != nil {
		return 0, 0, err
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		stats.upstreamDuration += time.Since(start)
		p.recordUpstreamFailure(target.Bucket, "full")
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPreconditionFailed {
		stats.upstreamDuration += time.Since(start)
		return 0, 0, errObjectChanged
	}
	if resp.StatusCode != http.StatusOK {
		stats.upstreamDuration += time.Since(start)
		p.recordUpstreamFailure(target.Bucket, "full")
		http.Error(w, "fetch upstream object", http.StatusBadGateway)
		return 0, 0, fmt.Errorf("fetch full object: upstream status %d", resp.StatusCode)
	}

	stats.pagesRequested = int64(len(pages))
	stats.pagesMissed += int64(len(pages))
	for range pages {
		if p.metrics != nil {
			p.metrics.RecordPageMiss(target.Bucket)
		}
	}
	writeCachedObjectHeaders(w.Header(), obj, false)
	w.WriteHeader(http.StatusOK)
	serveStart := time.Now()
	bytesWritten, err := p.streamColdFullObject(w, r.Context(), resp.Body, target, obj, stats)
	stats.upstreamDuration += time.Since(start)
	stats.cacheServeDuration += time.Since(serveStart)
	stats.finishCachedResult()
	if p.metrics != nil && stats.bytesFetchedUpstream > 0 {
		p.metrics.RecordUpstreamFillBytes(target.Bucket, stats.bytesFetchedUpstream)
		p.metrics.ObserveUpstreamDuration(target.Bucket, "full", time.Since(start))
	}
	return http.StatusOK, bytesWritten, err
}

func (p *Proxy) streamColdFullObject(w http.ResponseWriter, ctx context.Context, body io.Reader, target s3request.Target, obj cache.Object, stats *requestStats) (int64, error) {
	var total int64
	pageIndex := int64(0)
	pageBuffer, err := newTempPageBuffer()
	if err != nil {
		return 0, err
	}
	defer pageBuffer.Close()
	buf := make([]byte, 32*1024)

	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			written, writeErr := w.Write(chunk)
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
			stats.bytesFetchedUpstream += int64(n)
			var err error
			pageIndex, err = p.bufferColdFullPage(ctx, target, obj, pageIndex, pageBuffer, chunk, stats)
			if err != nil {
				return total, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			abortCommittedResponse(w, total)
			return total, readErr
		}
	}

	if pageBuffer.Size() > 0 {
		p.storeColdFullPage(ctx, target, obj, pageIndex, pageBuffer, stats)
	}
	if total != obj.Size {
		abortCommittedResponse(w, total)
		return total, fmt.Errorf("fetch full object: got %d bytes, want %d", total, obj.Size)
	}
	return total, nil
}

func (p *Proxy) bufferColdFullPage(ctx context.Context, target s3request.Target, obj cache.Object, pageIndex int64, pageBuffer *tempPageBuffer, chunk []byte, stats *requestStats) (int64, error) {
	for len(chunk) > 0 {
		remaining := int(obj.PageSize - pageBuffer.Size())
		if remaining > len(chunk) {
			remaining = len(chunk)
		}
		if _, err := pageBuffer.Write(chunk[:remaining]); err != nil {
			return pageIndex, err
		}
		chunk = chunk[remaining:]
		if pageBuffer.Size() == obj.PageSize {
			p.storeColdFullPage(ctx, target, obj, pageIndex, pageBuffer, stats)
			pageIndex++
			if err := pageBuffer.Reset(); err != nil {
				return pageIndex, err
			}
		}
	}
	return pageIndex, nil
}

func (p *Proxy) storeColdFullPage(ctx context.Context, target s3request.Target, obj cache.Object, pageIndex int64, pageBuffer *tempPageBuffer, stats *requestStats) {
	if _, err := pageBuffer.Seek(0, io.SeekStart); err != nil {
		p.logger.WarnContext(ctx, "cache page store failed",
			slog.String("bucket", target.Bucket),
			slog.String("key", target.Key),
			slog.Int64("page_index", pageIndex),
			slog.String("error", err.Error()),
		)
		return
	}
	if _, err := p.cache.StorePage(ctx, cache.PageWrite{
		ObjectID:      obj.ID,
		Index:         pageIndex,
		ETag:          obj.ETag,
		ExpectedEpoch: obj.Epoch,
		Size:          pageBuffer.Size(),
		Source:        pageBuffer,
	}); err != nil {
		if p.metrics != nil {
			p.metrics.RecordCacheWriteFailure(target.Bucket)
		}
		p.logger.WarnContext(ctx, "cache page store failed",
			slog.String("bucket", target.Bucket),
			slog.String("key", target.Key),
			slog.Int64("page_index", pageIndex),
			slog.String("error", err.Error()),
		)
	}
}

func (p *Proxy) ensureObjectMetadata(ctx context.Context, r *http.Request, target s3request.Target, stats *requestStats) (cache.Object, bool, error) {
	obj, ok, err := p.cache.GetObject(ctx, target.Bucket, target.Key)
	if err != nil || ok {
		return obj, ok, err
	}

	obj, _, _, ok, err = p.fetchMetadata(ctx, r, target, stats)
	return obj, ok, err
}

func (p *Proxy) fetchMetadata(ctx context.Context, r *http.Request, target s3request.Target, stats *requestStats) (cache.Object, int, http.Header, bool, error) {
	req, err := p.newUpstreamRequest(ctx, r, http.MethodHead, nil)
	if err != nil {
		return cache.Object{}, 0, nil, false, err
	}
	req.Header.Del("Range")

	start := time.Now()
	resp, err := p.client.Do(req)
	stats.upstreamDuration += time.Since(start)
	if err != nil {
		p.recordUpstreamFailure(target.Bucket, "metadata")
		return cache.Object{}, 0, nil, false, err
	}
	defer resp.Body.Close()

	headers := responseHeaders(resp)
	if resp.StatusCode >= http.StatusInternalServerError {
		p.recordUpstreamFailure(target.Bucket, "metadata")
	}
	if resp.StatusCode != http.StatusOK {
		return cache.Object{}, resp.StatusCode, headers, false, nil
	}

	size, err := responseSize(resp)
	if err != nil {
		return cache.Object{}, resp.StatusCode, headers, false, err
	}
	obj, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   target.Bucket,
		Key:      target.Key,
		ETag:     headers.Get("ETag"),
		Size:     size,
		PageSize: p.pageSizeForBucket(target.Bucket),
		Headers:  headers,
	})
	if err != nil {
		return cache.Object{}, resp.StatusCode, headers, false, fmt.Errorf("%w: %v", errMetadataStore, err)
	}

	return obj, resp.StatusCode, headers, true, nil
}

func (p *Proxy) pageSizeForBucket(bucket string) int64 {
	if p.pageSizeByBucket != nil {
		if pageSize := p.pageSizeByBucket[bucket]; pageSize > 0 {
			return pageSize
		}
	}
	return p.pageSize
}

func (p *Proxy) prepareFirstPage(r *http.Request, target s3request.Target, obj cache.Object, byteRange cacheplan.ByteRange, stats *requestStats) ([]cacheplan.PageSpan, io.ReadCloser, error) {
	pages, err := cacheplan.PagesForRange(byteRange, obj.PageSize)
	if err != nil {
		return nil, nil, err
	}
	if len(pages) == 0 {
		return pages, nil, nil
	}

	firstPage, err := p.pageReader(r.Context(), r, target, obj, pages[0].Index, stats)
	if err != nil {
		return nil, nil, err
	}

	return pages, firstPage, nil
}

func (p *Proxy) refetchAfterObjectChanged(r *http.Request, target s3request.Target, requestedRange cacheplan.ByteRange, stats *requestStats) (cache.Object, cacheplan.ByteRange, []cacheplan.PageSpan, io.ReadCloser, error) {
	if err := p.cache.DeleteObject(r.Context(), target.Bucket, target.Key); err != nil {
		return cache.Object{}, cacheplan.ByteRange{}, nil, nil, err
	}
	if p.metrics != nil {
		p.metrics.RecordInvalidation(target.Bucket)
	}
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target, stats)
	if err != nil {
		return cache.Object{}, cacheplan.ByteRange{}, nil, nil, err
	}
	if !ok {
		return cache.Object{}, cacheplan.ByteRange{}, nil, nil, errors.New("metadata missing after refetch")
	}

	if requestedRange.End >= obj.Size {
		requestedRange.End = obj.Size - 1
	}
	if requestedRange.Start > requestedRange.End {
		return cache.Object{}, cacheplan.ByteRange{}, nil, nil, errRefetchedRangeUnsatisfied
	}

	pages, firstPage, err := p.prepareFirstPage(r, target, obj, requestedRange, stats)
	return obj, requestedRange, pages, firstPage, err
}

func (p *Proxy) streamCachedPages(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, pages []cacheplan.PageSpan, firstPage io.ReadCloser, stats *requestStats) (int64, error) {
	var total int64
	for i, page := range pages {
		var n int64
		var err error
		if i == 0 {
			n, err = copyPageSpan(w, firstPage, page, obj.PageSize)
			if closeErr := firstPage.Close(); err == nil {
				err = closeErr
			}
		} else {
			n, err = p.streamPageSpan(w, r, target, obj, page, stats)
		}
		total += int64(n)
		if err != nil {
			abortCommittedResponse(w, total)
			return total, err
		}
	}

	return total, nil
}

func abortCommittedResponse(w http.ResponseWriter, bytesWritten int64) {
	if bytesWritten <= 0 {
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	_ = conn.Close()
}

func (p *Proxy) pageReader(ctx context.Context, r *http.Request, target s3request.Target, obj cache.Object, index int64, stats *requestStats) (io.ReadCloser, error) {
	body, ok, err := p.cache.OpenPage(ctx, obj.ID, index, obj.ETag, obj.Epoch)
	if err != nil {
		return nil, err
	}
	if ok {
		stats.pagesHit++
		if p.metrics != nil {
			p.metrics.RecordPageHit(target.Bucket)
		}
		return body, nil
	}
	stats.pagesMissed++
	if p.metrics != nil {
		p.metrics.RecordPageMiss(target.Bucket)
	}

	return p.fillMissingPage(ctx, r, target, obj, index, stats)
}

func (p *Proxy) fillMissingPage(ctx context.Context, r *http.Request, target s3request.Target, obj cache.Object, index int64, stats *requestStats) (io.ReadCloser, error) {
	key := pageFillKey{
		objectID: obj.ID,
		index:    index,
		etag:     obj.ETag,
		epoch:    obj.Epoch,
	}

	p.pageFillMu.Lock()
	if p.pageFills == nil {
		p.pageFills = make(map[pageFillKey]*pageFillCall)
	}
	if call, ok := p.pageFills[key]; ok {
		p.pageFillMu.Unlock()
		select {
		case <-call.done:
			if call.err != nil {
				return nil, call.err
			}
			body, ok, err := p.cache.OpenPage(ctx, obj.ID, index, obj.ETag, obj.Epoch)
			if err != nil {
				return nil, err
			}
			if !ok {
				return p.fillMissingPage(ctx, r, target, obj, index, stats)
			}
			return body, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &pageFillCall{done: make(chan struct{})}
	p.pageFills[key] = call
	p.pageFillMu.Unlock()

	reader, err := p.fetchAndStorePage(ctx, r, target, obj, index, nil, stats)
	call.err = err

	p.pageFillMu.Lock()
	delete(p.pageFills, key)
	p.pageFillMu.Unlock()
	close(call.done)

	return reader, err
}

func (p *Proxy) streamPageSpan(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, page cacheplan.PageSpan, stats *requestStats) (int64, error) {
	body, ok, err := p.cache.OpenPage(r.Context(), obj.ID, page.Index, obj.ETag, obj.Epoch)
	if err != nil {
		return 0, err
	}
	if ok {
		stats.pagesHit++
		if p.metrics != nil {
			p.metrics.RecordPageHit(target.Bucket)
		}
		defer body.Close()
		return copyPageSpan(w, body, page, obj.PageSize)
	}
	stats.pagesMissed++
	if p.metrics != nil {
		p.metrics.RecordPageMiss(target.Bucket)
	}

	return p.fillAndStreamMissingPage(w, r, target, obj, page, stats)
}

func (p *Proxy) fillAndStreamMissingPage(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, page cacheplan.PageSpan, stats *requestStats) (int64, error) {
	key := pageFillKey{
		objectID: obj.ID,
		index:    page.Index,
		etag:     obj.ETag,
		epoch:    obj.Epoch,
	}

	p.pageFillMu.Lock()
	if p.pageFills == nil {
		p.pageFills = make(map[pageFillKey]*pageFillCall)
	}
	if call, ok := p.pageFills[key]; ok {
		p.pageFillMu.Unlock()
		select {
		case <-call.done:
			if call.err != nil {
				return 0, call.err
			}
			body, ok, err := p.cache.OpenPage(r.Context(), obj.ID, page.Index, obj.ETag, obj.Epoch)
			if err != nil {
				return 0, err
			}
			if !ok {
				return p.fillAndStreamMissingPage(w, r, target, obj, page, stats)
			}
			defer body.Close()
			return copyPageSpan(w, body, page, obj.PageSize)
		case <-r.Context().Done():
			return 0, r.Context().Err()
		}
	}
	call := &pageFillCall{done: make(chan struct{})}
	p.pageFills[key] = call
	p.pageFillMu.Unlock()

	stream := newPageSpanStream(w, page, obj.PageSize)
	reader, err := p.fetchAndStorePage(r.Context(), r, target, obj, page.Index, stream, stats)
	if reader != nil {
		_ = reader.Close()
	}
	call.err = err

	p.pageFillMu.Lock()
	delete(p.pageFills, key)
	p.pageFillMu.Unlock()
	close(call.done)

	return stream.written, err
}

func (p *Proxy) fetchAndStorePage(ctx context.Context, r *http.Request, target s3request.Target, obj cache.Object, index int64, stream *pageSpanStream, stats *requestStats) (io.ReadCloser, error) {
	bounds, err := cacheplan.PageBounds(index, obj.PageSize, obj.Size)
	if err != nil {
		return nil, err
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", bounds.Start, bounds.End)
	req, err := p.newUpstreamRequest(ctx, r, http.MethodGet, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", rangeHeader)
	req.Header.Set("If-Match", obj.ETag)

	start := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		stats.upstreamDuration += time.Since(start)
		p.recordUpstreamFailure(target.Bucket, "fill")
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPreconditionFailed {
		stats.upstreamDuration += time.Since(start)
		return nil, errObjectChanged
	}
	if resp.StatusCode != http.StatusPartialContent {
		stats.upstreamDuration += time.Since(start)
		p.recordUpstreamFailure(target.Bucket, "fill")
		return nil, fmt.Errorf("fetch page %d: upstream status %d", index, resp.StatusCode)
	}
	tmp, size, err := copyPageResponseToTemp(resp.Body, stream)
	if err != nil {
		stats.upstreamDuration += time.Since(start)
		return nil, err
	}
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	stats.upstreamDuration += time.Since(start)
	if size != bounds.End-bounds.Start+1 {
		return nil, fmt.Errorf("fetch page %d: got %d bytes, want %d", index, size, bounds.End-bounds.Start+1)
	}
	stats.bytesFetchedUpstream += size
	if p.metrics != nil {
		p.metrics.RecordUpstreamFillBytes(target.Bucket, size)
		p.metrics.ObserveUpstreamDuration(target.Bucket, "fill", time.Since(start))
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := p.cache.StorePage(ctx, cache.PageWrite{
		ObjectID:      obj.ID,
		Index:         index,
		ETag:          obj.ETag,
		ExpectedEpoch: obj.Epoch,
		Size:          size,
		Source:        tmp,
	}); err != nil {
		if p.metrics != nil {
			p.metrics.RecordCacheWriteFailure(target.Bucket)
		}
		p.logger.WarnContext(ctx, "cache page store failed",
			slog.String("bucket", target.Bucket),
			slog.String("key", target.Key),
			slog.Int64("page_index", index),
			slog.String("error", err.Error()),
		)
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	cleanupTemp = false
	return &tempPageReader{File: tmp}, nil
}

type tempPageReader struct {
	*os.File
}

func (r *tempPageReader) Close() error {
	name := r.Name()
	err := r.File.Close()
	if removeErr := os.Remove(name); err == nil {
		err = removeErr
	}
	return err
}

type tempPageBuffer struct {
	file *os.File
	size int64
}

func newTempPageBuffer() (*tempPageBuffer, error) {
	file, err := os.CreateTemp("", "simple-s3-cache-page-*")
	if err != nil {
		return nil, err
	}
	return &tempPageBuffer{file: file}, nil
}

func (b *tempPageBuffer) Write(data []byte) (int, error) {
	n, err := b.file.Write(data)
	b.size += int64(n)
	return n, err
}

func (b *tempPageBuffer) Read(data []byte) (int, error) {
	return b.file.Read(data)
}

func (b *tempPageBuffer) Seek(offset int64, whence int) (int64, error) {
	return b.file.Seek(offset, whence)
}

func (b *tempPageBuffer) Size() int64 {
	return b.size
}

func (b *tempPageBuffer) Reset() error {
	if err := b.file.Truncate(0); err != nil {
		return err
	}
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	b.size = 0
	return nil
}

func (b *tempPageBuffer) Close() error {
	name := b.file.Name()
	err := b.file.Close()
	if removeErr := os.Remove(name); err == nil {
		err = removeErr
	}
	return err
}

type pageSpanStream struct {
	dst       io.Writer
	start     int64
	end       int64
	offset    int64
	written   int64
	pageIndex int64
}

func newPageSpanStream(dst io.Writer, page cacheplan.PageSpan, pageSize int64) *pageSpanStream {
	return &pageSpanStream{
		dst:       dst,
		start:     page.Start - page.Index*pageSize,
		end:       page.End - page.Index*pageSize + 1,
		pageIndex: page.Index,
	}
}

func (s *pageSpanStream) Write(data []byte) (int, error) {
	chunkStart := s.offset
	chunkEnd := s.offset + int64(len(data))
	s.offset = chunkEnd
	if chunkEnd <= s.start || chunkStart >= s.end {
		return len(data), nil
	}
	overlapStart := maxInt64(chunkStart, s.start)
	overlapEnd := minInt64(chunkEnd, s.end)
	from := overlapStart - chunkStart
	to := overlapEnd - chunkStart
	n, err := s.dst.Write(data[from:to])
	s.written += int64(n)
	if err != nil {
		return n, err
	}
	if n != int(to-from) {
		return n, io.ErrShortWrite
	}
	return len(data), nil
}

func copyPageResponseToTemp(src io.Reader, stream *pageSpanStream) (*os.File, int64, error) {
	tmp, err := os.CreateTemp("", "simple-s3-cache-page-*")
	if err != nil {
		return nil, 0, err
	}
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			written, writeErr := tmp.Write(chunk)
			total += int64(written)
			if writeErr != nil {
				_ = tmp.Close()
				_ = os.Remove(tmp.Name())
				return nil, total, writeErr
			}
			if written != n {
				_ = tmp.Close()
				_ = os.Remove(tmp.Name())
				return nil, total, io.ErrShortWrite
			}
			if stream != nil {
				if _, writeErr := stream.Write(chunk); writeErr != nil {
					_ = tmp.Close()
					_ = os.Remove(tmp.Name())
					return nil, total, writeErr
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			return tmp, total, nil
		}
		if readErr != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return nil, total, readErr
		}
	}
}

func copyPageSpan(dst io.Writer, src io.Reader, page cacheplan.PageSpan, pageSize int64) (int64, error) {
	start := page.Start - page.Index*pageSize
	end := page.End - page.Index*pageSize
	if start < 0 || end < start {
		return 0, fmt.Errorf("cached page %d has invalid span", page.Index)
	}
	if start > 0 {
		if _, err := io.CopyN(io.Discard, src, start); err != nil {
			return 0, fmt.Errorf("cached page %d too short for requested range: %w", page.Index, err)
		}
	}
	n, err := io.CopyN(dst, src, end-start+1)
	if err != nil {
		return n, err
	}
	return n, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (p *Proxy) newUpstreamRequest(ctx context.Context, r *http.Request, method string, body io.Reader) (*http.Request, error) {
	upstreamURL := p.upstreamURL(r)
	req, err := http.NewRequestWithContext(ctx, method, upstreamURL.String(), body)
	if err != nil {
		return nil, err
	}
	copyRequestHeaders(req.Header, r.Header)
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	if body == nil {
		req.ContentLength = 0
	}
	if err := p.sign(req); err != nil {
		return nil, err
	}
	return req, nil
}

func (p *Proxy) sign(req *http.Request) error {
	credentials, err := p.credentials.Retrieve(req.Context())
	if err != nil {
		return err
	}
	return p.signer.SignHTTP(req.Context(), credentials, req, unsignedPayload, "s3", p.region, time.Now(), func(options *v4.SignerOptions) {
		options.DisableURIPathEscaping = true
	})
}

func (p *Proxy) upstreamURL(r *http.Request) url.URL {
	upstreamURL := *p.upstreamEndpoint
	upstreamURL.Path = joinURLPath(p.upstreamEndpoint.Path, r.URL.Path)
	upstreamURL.RawPath = ""
	upstreamURL.RawQuery = r.URL.RawQuery
	return upstreamURL
}

func responseHeaders(resp *http.Response) http.Header {
	headers := http.Header{}
	copyResponseHeaders(headers, resp.Header)
	if resp.ContentLength >= 0 {
		headers.Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	return headers
}

func responseSize(resp *http.Response) (int64, error) {
	if resp.ContentLength >= 0 {
		return resp.ContentLength, nil
	}
	sizeText := resp.Header.Get("Content-Length")
	if sizeText == "" {
		return 0, fmt.Errorf("upstream metadata missing Content-Length")
	}
	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse Content-Length: %w", err)
	}
	return size, nil
}

func writeCachedObjectHeaders(dst http.Header, obj cache.Object, rangeResponse bool) {
	copyResponseHeaders(dst, obj.Headers)
	dst.Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	if rangeResponse {
		dst.Del("Content-Range")
		dropRangeResponseHeaders(dst)
	}
}

func writeCachedConditionalHeaders(dst http.Header, obj cache.Object) {
	for _, key := range []string{"ETag", "Last-Modified", "Cache-Control", "Expires"} {
		if value := obj.Headers.Get(key); value != "" {
			dst.Set(key, value)
		}
	}
}

func cachedConditionalStatus(r *http.Request, obj cache.Object) (int, bool, error) {
	ifMatch := r.Header.Get("If-Match")
	if ifMatch != "" && !strongETagMatches(ifMatch, obj.ETag) {
		return http.StatusPreconditionFailed, true, nil
	}

	ifUnmodifiedSince := r.Header.Get("If-Unmodified-Since")
	if ifUnmodifiedSince != "" && ifMatch == "" {
		modified, err := cachedLastModified(obj)
		if err != nil {
			return 0, false, err
		}
		since, err := http.ParseTime(ifUnmodifiedSince)
		if err != nil {
			return 0, false, err
		}
		if modified.After(since) {
			return http.StatusPreconditionFailed, true, nil
		}
	}

	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" {
		if weakETagMatches(ifNoneMatch, obj.ETag) {
			return http.StatusNotModified, true, nil
		}
		return 0, false, nil
	}

	ifModifiedSince := r.Header.Get("If-Modified-Since")
	if ifModifiedSince != "" {
		modified, err := cachedLastModified(obj)
		if err != nil {
			return 0, false, err
		}
		since, err := http.ParseTime(ifModifiedSince)
		if err != nil {
			return 0, false, err
		}
		if !modified.After(since) {
			return http.StatusNotModified, true, nil
		}
	}

	return 0, false, nil
}

func cachedLastModified(obj cache.Object) (time.Time, error) {
	value := obj.Headers.Get("Last-Modified")
	if value == "" {
		return time.Time{}, errors.New("cached metadata missing Last-Modified")
	}
	return http.ParseTime(value)
}

func strongETagMatches(condition, etag string) bool {
	for _, candidate := range strings.Split(condition, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == etag {
			return true
		}
	}
	return false
}

func weakETagMatches(condition, etag string) bool {
	etag = strings.TrimPrefix(strings.TrimSpace(etag), "W/")
	for _, candidate := range strings.Split(condition, ",") {
		candidate = strings.TrimPrefix(strings.TrimSpace(candidate), "W/")
		if candidate == "*" || candidate == etag {
			return true
		}
	}
	return false
}

func dropRangeResponseHeaders(header http.Header) {
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-checksum-") {
			header.Del(key)
		}
	}
}

func copyRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) || isClientSigningHeader(key) || isPeerHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyPeerRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) || isPeerHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isPeerHeader(key string) bool {
	return strings.EqualFold(key, peerForwardedHeader) ||
		strings.EqualFold(key, peerOwnerHeader) ||
		strings.EqualFold(key, peerFromHeader)
}

func isClientSigningHeader(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "x-amz-date", "x-amz-security-token", "x-amz-content-sha256":
		return true
	default:
		return false
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

func isHopByHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func joinURLPath(basePath, requestPath string) string {
	basePath = strings.TrimRight(basePath, "/")
	if basePath == "" {
		return requestPath
	}
	if requestPath == "" || requestPath == "/" {
		return basePath
	}
	return basePath + "/" + strings.TrimLeft(requestPath, "/")
}
