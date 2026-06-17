package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
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
	readShardingObject = "object"
	readShardingPage   = "page"
	readShardingAuto   = "auto"
)

const (
	peerForwardedHeader = peerrouter.ForwardedHeader
	peerOwnerHeader     = peerrouter.OwnerHeader
	peerFromHeader      = peerrouter.FromHeader
	peerRingHeader      = peerrouter.RingHeader
)

const (
	internalV1Prefix          = "/internal/v1/"
	internalObjectRoutePrefix = "/internal/v1/objects"
)

const (
	upstreamDialTimeout           = 10 * time.Second
	upstreamKeepAlive             = 30 * time.Second
	upstreamTLSHandshakeTimeout   = 10 * time.Second
	upstreamExpectContinueTimeout = time.Second
	upstreamIdleConnTimeout       = 90 * time.Second
	upstreamMaxIdleConns          = 100
	upstreamMaxIdleConnsPerHost   = 100
	upstreamErrorBodyLogLimit     = 4096
)

var (
	errObjectChanged             = errors.New("cached object changed upstream")
	errMetadataStore             = errors.New("cache metadata store failed")
	errSpoolLimit                = errors.New("upload body exceeds max spool size")
	errRefetchedRangeUnsatisfied = errors.New("refetched object does not satisfy requested range")
	errPeerFillOverloaded        = errors.New("peer page fill concurrency limit exceeded")
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
	BeginPageWrite(context.Context, cache.PageWriteOptions) (*cache.PageWriter, error)
	ListPages(context.Context, string) ([]cache.Page, error)
	OpenPage(context.Context, string, int64, string, int64) (io.ReadCloser, bool, error)
	Close() error
}

type Proxy struct {
	upstreamEndpoint *url.URL
	upstreamHost     string
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
	readSharding     string
	pageShardMin     int64
	pageFillMu       sync.Mutex
	pageFills        map[pageFillKey]*pageFillCall
	peerFillSem      chan struct{}
	objectFillMu     sync.Mutex
	objectFillSems   map[string]chan struct{}
	objectFillLimit  int
}

type readStrategySelection struct {
	configuredStrategy string
	strategy           string
	effectivePageSize  int64
	pageCount          int64
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

type internalPageReadRequest struct {
	Bucket     string  `json:"bucket"`
	Key        string  `json:"key"`
	ObjectSize int64   `json:"object_size"`
	PageSize   int64   `json:"page_size"`
	ETag       string  `json:"etag"`
	Epoch      int64   `json:"epoch"`
	Pages      []int64 `json:"pages"`
}

type coordinatorPageBatch struct {
	owner  peerrouter.Peer
	pages  []int64
	body   io.Closer
	reader *peerrouter.PageFrameReader
}

type coordinatorPageReader struct {
	batch  *coordinatorPageBatch
	frame  peerrouter.PageFrame
	loaded bool
}

type requestStats struct {
	method                       string
	bucket                       string
	key                          string
	cacheResult                  string
	requestedRange               string
	bytesRequested               int64
	pagesRequested               int64
	pagesHit                     int64
	pagesMissed                  int64
	bytesSent                    int64
	bytesFetchedUpstream         int64
	upstreamDuration             time.Duration
	cacheServeDuration           time.Duration
	cacheMetadataDuration        time.Duration
	cachePageOpenDuration        time.Duration
	cacheResponseCopyDuration    time.Duration
	cacheResponseBytes           int64
	status                       int
	peerMode                     string
	peerLocalID                  string
	peerOwnerID                  string
	peerRingID                   string
	peerForwardRingID            string
	peerDecision                 string
	peerForwarded                bool
	peerForwardFailure           string
	peerForwardDuration          time.Duration
	peerResponseHeaderDuration   time.Duration
	peerResponseCopyDuration     time.Duration
	peerResponseBodyReadDuration time.Duration
	peerDownstreamWriteDuration  time.Duration
	peerResponseBodyReadChunks   int64
	peerResponseBytes            int64
	configuredReadSharding       string
	readStrategy                 string
	effectivePageSize            int64
	objectPageCount              int64
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
		recorder.SetPeerRingInfo("peer", router.LocalID(), router.RingID())
	}

	return &Proxy{
		upstreamEndpoint: upstreamEndpoint,
		upstreamHost:     strings.TrimSpace(cfg.Upstream.Host),
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
		metrics:         recorder,
		peerRouter:      router,
		peerClient:      newPeerHTTPClient(cfg.Peer.ForwardTimeout),
		peerTimeout:     cfg.Peer.ForwardTimeout,
		readSharding:    strings.TrimSpace(cfg.Peer.ReadSharding),
		pageShardMin:    cfg.Peer.PageShardingMinPages,
		peerFillSem:     make(chan struct{}, cfg.Peer.MaxFillConcurrency),
		objectFillSems:  make(map[string]chan struct{}),
		objectFillLimit: cfg.Peer.MaxObjectFillConcurrency,
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
	if isInternalRoute(r.URL.Path) {
		p.serveInternalHTTP(w, r)
		return
	}

	stripPeerHeaders(r.Header)
	p.serveS3HTTP(w, r)
}

func (p *Proxy) serveInternalHTTP(w http.ResponseWriter, r *http.Request) {
	if p.peerRouter == nil {
		http.NotFound(w, r)
		return
	}
	if !p.validateInternalPeerHeaders(w, r) {
		return
	}

	switch {
	case strings.HasPrefix(r.URL.Path, internalObjectRoutePrefix+"/"):
		if r.Header.Get(peerForwardedHeader) == "" {
			http.Error(w, "missing peer forwarded header", http.StatusUnauthorized)
			return
		}
		if r.Header.Get(peerOwnerHeader) == "" {
			http.Error(w, "missing peer owner header", http.StatusUnauthorized)
			return
		}
		p.serveS3HTTP(w, internalObjectRequest(r))
	case r.URL.Path == "/internal/v1/pages/read":
		p.serveInternalPageRead(w, r)
	case r.URL.Path == "/internal/v1/invalidate":
		http.Error(w, "internal route not implemented", http.StatusNotImplemented)
	default:
		http.NotFound(w, r)
	}
}

func (p *Proxy) serveInternalPageRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req internalPageReadRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid page read request", http.StatusBadRequest)
		return
	}
	if err := validateInternalPageReadRequest(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	target := s3request.Target{Bucket: req.Bucket, Key: req.Key}
	obj, status, err := p.internalPageReadObject(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	stats := newRequestStats(r, target)
	stats.pagesRequested = int64(len(req.Pages))
	pages := make([][]byte, 0, len(req.Pages))
	for _, pageIndex := range req.Pages {
		body, ok, err := p.openCachedPage(r.Context(), target, obj, pageIndex, stats, true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			data, err := p.fillMissingPageBytes(r.Context(), p.internalPageReadUpstreamRequest(r, req), target, obj, pageIndex, stats)
			if errors.Is(err, errObjectChanged) {
				if deleteErr := p.cache.DeleteObject(r.Context(), req.Bucket, req.Key); deleteErr != nil {
					p.logger.WarnContext(r.Context(), "invalidate stale internal page identity failed",
						slog.String("bucket", req.Bucket),
						slog.String("key", req.Key),
						slog.String("error", deleteErr.Error()),
					)
				}
				http.Error(w, "object identity changed upstream", http.StatusPreconditionFailed)
				return
			}
			if errors.Is(err, errPeerFillOverloaded) {
				http.Error(w, "peer fill concurrency limit exceeded", http.StatusServiceUnavailable)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			pages = append(pages, data)
			continue
		}

		data, err := readAndClosePage(body, req.PageSize)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pages = append(pages, data)
	}

	w.Header().Set("Content-Type", peerrouter.PageFrameContentType)
	w.WriteHeader(http.StatusOK)
	frameWriter, err := peerrouter.NewPageFrameWriter(w)
	if err != nil {
		p.logger.WarnContext(r.Context(), "write internal page frame header failed",
			slog.String("bucket", req.Bucket),
			slog.String("key", req.Key),
			slog.String("error", err.Error()),
		)
		return
	}
	for i, pageIndex := range req.Pages {
		if err := frameWriter.WritePage(pageIndex, pages[i]); err != nil {
			p.logger.WarnContext(r.Context(), "write internal page frame failed",
				slog.String("bucket", req.Bucket),
				slog.String("key", req.Key),
				slog.Int64("page_index", pageIndex),
				slog.String("error", err.Error()),
			)
			return
		}
	}
	if err := frameWriter.WriteEnd(); err != nil {
		p.logger.WarnContext(r.Context(), "write internal page frame end failed",
			slog.String("bucket", req.Bucket),
			slog.String("key", req.Key),
			slog.String("error", err.Error()),
		)
	}
}

func validateInternalPageReadRequest(req internalPageReadRequest) error {
	if strings.TrimSpace(req.Bucket) == "" {
		return errors.New("bucket is required")
	}
	if req.Key == "" {
		return errors.New("key is required")
	}
	if req.ObjectSize <= 0 {
		return errors.New("object_size must be greater than zero")
	}
	if req.PageSize <= 0 {
		return errors.New("page_size must be greater than zero")
	}
	if req.ETag == "" {
		return errors.New("etag is required")
	}
	if req.Epoch < 0 {
		return errors.New("epoch must not be negative")
	}
	if len(req.Pages) == 0 {
		return errors.New("pages must not be empty")
	}
	if !sort.SliceIsSorted(req.Pages, func(i, j int) bool { return req.Pages[i] < req.Pages[j] }) {
		return errors.New("pages must be in increasing order")
	}
	for i, pageIndex := range req.Pages {
		if i > 0 && pageIndex == req.Pages[i-1] {
			return errors.New("pages must not contain duplicates")
		}
		if _, err := cacheplan.PageBounds(pageIndex, req.PageSize, req.ObjectSize); err != nil {
			return fmt.Errorf("invalid page %d: %w", pageIndex, err)
		}
	}
	return nil
}

func (p *Proxy) internalPageReadObject(ctx context.Context, req internalPageReadRequest) (cache.Object, int, error) {
	obj, ok, err := p.cache.GetObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return cache.Object{}, http.StatusInternalServerError, err
	}
	if ok {
		if obj.Epoch != req.Epoch {
			return cache.Object{}, http.StatusConflict, fmt.Errorf("object epoch mismatch: local %d request %d", obj.Epoch, req.Epoch)
		}
		if obj.ETag == req.ETag && obj.Size == req.ObjectSize && obj.PageSize == req.PageSize {
			return obj, http.StatusOK, nil
		}
	} else if req.Epoch != 0 {
		return cache.Object{}, http.StatusConflict, fmt.Errorf("object metadata missing for epoch %d", req.Epoch)
	}

	obj, err = p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   req.Bucket,
		Key:      req.Key,
		ETag:     req.ETag,
		Size:     req.ObjectSize,
		PageSize: req.PageSize,
		Headers: http.Header{
			"Content-Length": []string{strconv.FormatInt(req.ObjectSize, 10)},
			"ETag":           []string{req.ETag},
		},
	})
	if err != nil {
		return cache.Object{}, http.StatusInternalServerError, err
	}
	if obj.Epoch != req.Epoch {
		return cache.Object{}, http.StatusConflict, fmt.Errorf("object epoch mismatch: local %d request %d", obj.Epoch, req.Epoch)
	}
	return obj, http.StatusOK, nil
}

func (p *Proxy) internalPageReadUpstreamRequest(r *http.Request, req internalPageReadRequest) *http.Request {
	upstreamReq := r.Clone(r.Context())
	upstreamReq.URL = &url.URL{
		Path:    "/" + req.Bucket + "/" + req.Key,
		RawPath: "/" + url.PathEscape(req.Bucket) + "/" + escapeS3KeyPath(req.Key),
	}
	upstreamReq.Header = make(http.Header)
	upstreamReq.Host = ""
	return upstreamReq
}

func escapeS3KeyPath(key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func readAndClosePage(body io.ReadCloser, maxBytes int64) ([]byte, error) {
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("cached page exceeds page size %d", maxBytes)
	}
	return data, nil
}

func (p *Proxy) validateInternalPeerHeaders(w http.ResponseWriter, r *http.Request) bool {
	from := strings.TrimSpace(r.Header.Get(peerFromHeader))
	if from == "" {
		http.Error(w, "missing peer identity", http.StatusUnauthorized)
		return false
	}
	if _, ok := p.peerRouter.PeerByID(from); !ok {
		http.Error(w, "unknown peer identity", http.StatusForbidden)
		return false
	}

	ringID := strings.TrimSpace(r.Header.Get(peerRingHeader))
	if ringID == "" || ringID != p.peerRouter.RingID() {
		http.Error(w, "peer ring mismatch", http.StatusBadGateway)
		return false
	}
	return true
}

func internalObjectRequest(r *http.Request) *http.Request {
	cloned := r.Clone(r.Context())
	urlCopy := *r.URL
	urlCopy.Path = strings.TrimPrefix(r.URL.Path, internalObjectRoutePrefix)
	if urlCopy.Path == "" {
		urlCopy.Path = "/"
	}
	urlCopy.RawPath = strings.TrimPrefix(r.URL.EscapedPath(), internalObjectRoutePrefix)
	if urlCopy.RawPath == "" {
		urlCopy.RawPath = "/"
	}
	cloned.URL = &urlCopy
	return cloned
}

func isInternalRoute(path string) bool {
	return path == strings.TrimSuffix(internalV1Prefix, "/") || strings.HasPrefix(path, internalV1Prefix)
}

func (p *Proxy) serveS3HTTP(w http.ResponseWriter, r *http.Request) {
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
	if p.shouldForwardToPeer(w, r, target, classification, stats) {
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
		slog.Int64("cache_serve_duration_ms", stats.cacheServeDuration.Milliseconds()),
		slog.Int64("cache_metadata_duration_ms", stats.cacheMetadataDuration.Milliseconds()),
		slog.Int64("cache_page_open_duration_ms", stats.cachePageOpenDuration.Milliseconds()),
		slog.Int64("cache_response_copy_duration_ms", stats.cacheResponseCopyDuration.Milliseconds()),
		slog.Int64("cache_response_bytes", stats.cacheResponseBytes),
		slog.Int64("request_duration_ms", time.Since(start).Milliseconds()),
		slog.Int64("upstream_bytes", stats.bytesFetchedUpstream),
	}
	if stats.readStrategy != "" {
		attrs = append(attrs,
			slog.String("read_strategy", stats.readStrategy),
			slog.String("configured_read_sharding", stats.configuredReadSharding),
			slog.Int64("effective_page_size", stats.effectivePageSize),
			slog.Int64("object_page_count", stats.objectPageCount),
		)
	}
	if stats.peerMode != "" {
		attrs = append(attrs,
			slog.String("peer_mode", stats.peerMode),
			slog.String("peer_local_id", stats.peerLocalID),
			slog.String("peer_owner_id", stats.peerOwnerID),
			slog.String("peer_ring_id", stats.peerRingID),
			slog.String("peer_forward_ring_id", stats.peerForwardRingID),
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

func (p *Proxy) shouldForwardToPeer(w http.ResponseWriter, r *http.Request, target s3request.Target, classification s3request.Classification, stats *requestStats) bool {
	if p.peerRouter == nil || !target.IsObject() {
		return false
	}

	owner := p.peerRouter.Owner(target.Bucket, target.Key)
	stats.peerMode = "peer"
	stats.peerLocalID = p.peerRouter.LocalID()
	stats.peerOwnerID = owner.ID
	stats.peerRingID = p.peerRouter.RingID()
	forwardedRequest := r.Header.Get(peerForwardedHeader) != ""
	if forwardedRequest {
		stats.peerForwardRingID = r.Header.Get(peerRingHeader)
		if stats.peerForwardRingID != stats.peerRingID {
			stats.peerForwarded = true
			stats.cacheResult = "peer_ring_mismatch"
			stats.peerForwardFailure = "ring_mismatch"
			stats.status = http.StatusBadGateway
			p.recordPeerMetrics(stats)
			http.Error(w, "peer ring mismatch", http.StatusBadGateway)
			return true
		}
	}
	if owner.ID == p.peerRouter.LocalID() {
		if forwardedRequest && r.Header.Get(peerOwnerHeader) != owner.ID {
			stats.peerForwarded = true
			stats.cacheResult = "peer_owner_mismatch"
			stats.peerForwardFailure = "owner_mismatch"
			stats.status = http.StatusBadGateway
			p.recordPeerMetrics(stats)
			http.Error(w, "peer owner mismatch", http.StatusBadGateway)
			return true
		}
		stats.peerDecision = "local"
		p.recordPeerMetrics(stats)
		return false
	}

	if !forwardedRequest && isCoordinatorReadCandidate(classification) {
		stats.peerDecision = "coordinator"
		return false
	}

	if forwardedRequest {
		stats.peerForwarded = true
		stats.cacheResult = "peer_routing_mismatch"
		stats.peerForwardFailure = "routing_mismatch"
		stats.status = http.StatusBadGateway
		p.recordPeerMetrics(stats)
		http.Error(w, "peer routing mismatch", http.StatusBadGateway)
		return true
	}

	stats.peerForwarded = true
	stats.cacheResult = "peer_forward"
	stats.peerDecision = "remote"
	start := time.Now()
	status, bytesWritten, err := p.forwardToPeer(w, r, owner, stats)
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
		slog.Int64("peer_response_bytes", stats.peerResponseBytes),
		slog.String("peer_local_id", p.peerRouter.LocalID()),
		slog.String("peer_owner_id", owner.ID),
		slog.Int64("peer_forward_duration_ms", stats.peerForwardDuration.Milliseconds()),
		slog.Int64("peer_response_header_duration_ms", stats.peerResponseHeaderDuration.Milliseconds()),
		slog.Int64("peer_response_copy_duration_ms", stats.peerResponseCopyDuration.Milliseconds()),
		slog.Int64("peer_response_body_read_duration_ms", stats.peerResponseBodyReadDuration.Milliseconds()),
		slog.Int64("peer_downstream_write_duration_ms", stats.peerDownstreamWriteDuration.Milliseconds()),
		slog.Int64("peer_response_body_read_chunks", stats.peerResponseBodyReadChunks),
	)
	return true
}

func isCoordinatorReadCandidate(classification s3request.Classification) bool {
	return classification.Disposition == s3request.CacheableFullObject ||
		classification.Disposition == s3request.CacheableRangeObject
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
	p.applyUpstreamHost(req)

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

func (p *Proxy) forwardToPeer(w http.ResponseWriter, r *http.Request, owner peerrouter.Peer, stats *requestStats) (int, int64, error) {
	peerURL := strings.TrimRight(owner.URL, "/") + internalObjectRoutePrefix + r.URL.RequestURI()
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
	if p.peerRouter != nil {
		req.Header.Set(peerRingHeader, p.peerRouter.RingID())
	}

	headerStart := time.Now()
	resp, err := p.peerClient.Do(req)
	stats.peerResponseHeaderDuration = time.Since(headerStart)
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
	copyStart := time.Now()
	reader := &timedPeerBodyReader{src: resp.Body}
	bytesWritten, copyErr := io.Copy(w, reader)
	stats.peerResponseCopyDuration = time.Since(copyStart)
	stats.peerResponseBodyReadDuration = reader.duration
	stats.peerResponseBodyReadChunks = reader.chunks
	stats.peerDownstreamWriteDuration = stats.peerResponseCopyDuration - stats.peerResponseBodyReadDuration
	if stats.peerDownstreamWriteDuration < 0 {
		stats.peerDownstreamWriteDuration = 0
	}
	stats.peerResponseBytes = bytesWritten
	return resp.StatusCode, bytesWritten, copyErr
}

type timedPeerBodyReader struct {
	src      io.Reader
	duration time.Duration
	chunks   int64
}

func (r *timedPeerBodyReader) Read(data []byte) (int, error) {
	start := time.Now()
	n, err := r.src.Read(data)
	r.duration += time.Since(start)
	if n > 0 {
		r.chunks++
	}
	return n, err
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

func (s *requestStats) setReadStrategy(selection readStrategySelection) {
	s.configuredReadSharding = selection.configuredStrategy
	s.readStrategy = selection.strategy
	s.effectivePageSize = selection.effectivePageSize
	s.objectPageCount = selection.pageCount
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
	if stats.readStrategy != "" {
		p.metrics.RecordReadStrategy(stats.bucket, stats.readStrategy)
	}
	if stats.pagesRequested > 0 {
		p.metrics.ObservePagesTouched(stats.bucket, stats.pagesRequested)
	}
	if stats.cacheServeDuration > 0 {
		p.metrics.ObserveCacheServeDuration(stats.bucket, stats.cacheServeDuration)
	}
	if stats.cacheMetadataDuration > 0 {
		p.metrics.ObserveCacheMetadataDuration(stats.bucket, stats.cacheResult, stats.cacheMetadataDuration)
	}
	if stats.cachePageOpenDuration > 0 {
		p.metrics.ObserveCachePageOpenDuration(stats.bucket, stats.cacheResult, stats.cachePageOpenDuration)
	}
	if stats.cacheResponseCopyDuration > 0 {
		p.metrics.ObserveCacheResponseCopyDuration(stats.bucket, stats.cacheResult, stats.cacheResponseCopyDuration)
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
	if stats.peerResponseHeaderDuration > 0 {
		p.metrics.ObservePeerResponseHeaderDuration(stats.bucket, stats.peerOwnerID, statusClass, stats.peerResponseHeaderDuration)
	}
	if stats.peerResponseCopyDuration > 0 {
		p.metrics.ObservePeerResponseCopyDuration(stats.bucket, stats.peerOwnerID, statusClass, stats.peerResponseCopyDuration)
		p.metrics.ObservePeerResponseBodyReadDuration(stats.bucket, stats.peerOwnerID, statusClass, stats.peerResponseBodyReadDuration)
		p.metrics.ObservePeerDownstreamWriteDuration(stats.bucket, stats.peerOwnerID, statusClass, stats.peerDownstreamWriteDuration)
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

func (p *Proxy) forwardObjectReadToOwner(w http.ResponseWriter, r *http.Request, target s3request.Target, stats *requestStats) (bool, int, int64, error) {
	if p.peerRouter == nil || stats.readStrategy != readShardingObject || r.Header.Get(peerForwardedHeader) != "" {
		return false, 0, 0, nil
	}
	owner := p.peerRouter.Owner(target.Bucket, target.Key)
	if owner.ID == p.peerRouter.LocalID() {
		stats.peerDecision = "local"
		return false, 0, 0, nil
	}

	stats.peerMode = "peer"
	stats.peerLocalID = p.peerRouter.LocalID()
	stats.peerOwnerID = owner.ID
	stats.peerRingID = p.peerRouter.RingID()
	stats.peerForwarded = true
	stats.peerDecision = "remote"
	stats.cacheResult = "peer_forward"

	start := time.Now()
	status, bytesWritten, err := p.forwardToPeer(w, r, owner, stats)
	stats.peerForwardDuration = time.Since(start)
	stats.status = status
	stats.bytesSent = bytesWritten
	if err != nil {
		stats.peerForwardFailure = "request_failed"
		p.recordPeerMetrics(stats)
		return true, status, bytesWritten, err
	}
	p.recordPeerMetrics(stats)
	return true, status, bytesWritten, nil
}

func (p *Proxy) serveCachedHead(w http.ResponseWriter, r *http.Request, target s3request.Target, stats *requestStats) (int, int64, error) {
	metadataStart := time.Now()
	obj, ok, err := p.cache.GetObject(r.Context(), target.Bucket, target.Key)
	stats.cacheMetadataDuration += time.Since(metadataStart)
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
	metadataStart := time.Now()
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target, stats)
	stats.cacheMetadataDuration += time.Since(metadataStart)
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
	if p.peerRouter != nil {
		stats.setReadStrategy(p.selectReadStrategy(target.Bucket, obj.Size))
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
	if handled, status, bytesWritten, err := p.forwardObjectReadToOwner(w, r, target, stats); handled {
		return status, bytesWritten, err
	}

	byteRange, err := cacheplan.ParseRange(r.Header.Get("Range"), obj.Size)
	if err != nil {
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	}
	stats.bytesRequested = byteRange.End - byteRange.Start + 1

	if stats.readStrategy == readShardingPage {
		return p.serveDistributedPageRead(w, r, target, obj, byteRange, true, stats)
	}

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
	metadataStart := time.Now()
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target, stats)
	stats.cacheMetadataDuration += time.Since(metadataStart)
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
	if p.peerRouter != nil {
		stats.setReadStrategy(p.selectReadStrategy(target.Bucket, obj.Size))
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
	if handled, status, bytesWritten, err := p.forwardObjectReadToOwner(w, r, target, stats); handled {
		return status, bytesWritten, err
	}
	stats.bytesRequested = obj.Size
	if obj.Size == 0 {
		stats.cacheResult = "hit"
		writeCachedObjectHeaders(w.Header(), obj, false)
		w.WriteHeader(http.StatusOK)
		return http.StatusOK, 0, nil
	}

	byteRange := cacheplan.ByteRange{Start: 0, End: obj.Size - 1}
	if stats.readStrategy == readShardingPage {
		return p.serveDistributedPageRead(w, r, target, obj, byteRange, false, stats)
	}

	pages, err := cacheplan.PagesForRange(byteRange, obj.PageSize)
	if err != nil {
		return 0, 0, err
	}

	firstPage, ok, err := p.openCachedPage(r.Context(), target, obj, pages[0].Index, stats, false)
	if err != nil {
		stats.cacheResult = "error"
		http.Error(w, "open cached page", http.StatusInternalServerError)
		return 0, 0, err
	}
	if !ok {
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

	stats.pagesRequested = int64(len(pages))

	writeCachedObjectHeaders(w.Header(), obj, false)
	w.WriteHeader(http.StatusOK)
	start := time.Now()
	bytesWritten, err := p.streamCachedPages(w, r, target, obj, pages, firstPage, stats)
	stats.cacheServeDuration += time.Since(start)
	stats.finishCachedResult()
	return http.StatusOK, bytesWritten, err
}

func (p *Proxy) serveColdFullObject(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, stats *requestStats) (int, int64, error) {
	req, err := p.newUpstreamRequest(r.Context(), r, http.MethodGet, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Del("Range")
	req.Header.Set("If-Match", obj.ETag)
	if err := p.sign(req); err != nil {
		return 0, 0, err
	}

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
	var pageWriter *cache.PageWriter
	defer func() {
		if pageWriter != nil {
			_ = pageWriter.Abort()
		}
	}()
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
			pageIndex, pageWriter, err = p.bufferColdFullPage(ctx, target, obj, pageIndex, pageWriter, chunk, stats)
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

	if pageWriter != nil && pageWriter.Size() > 0 {
		p.commitCachePageWriter(ctx, target, pageIndex, pageWriter, stats)
	}
	if total != obj.Size {
		abortCommittedResponse(w, total)
		return total, fmt.Errorf("fetch full object: got %d bytes, want %d", total, obj.Size)
	}
	return total, nil
}

func (p *Proxy) bufferColdFullPage(ctx context.Context, target s3request.Target, obj cache.Object, pageIndex int64, pageWriter *cache.PageWriter, chunk []byte, stats *requestStats) (int64, *cache.PageWriter, error) {
	for len(chunk) > 0 {
		if pageWriter == nil {
			var err error
			pageWriter, err = p.beginCachePageWriter(ctx, target, obj, pageIndex, stats)
			if err != nil {
				return pageIndex, pageWriter, err
			}
		}
		remaining := int(obj.PageSize - pageWriter.Size())
		if remaining > len(chunk) {
			remaining = len(chunk)
		}
		if _, err := pageWriter.Write(chunk[:remaining]); err != nil {
			return pageIndex, pageWriter, err
		}
		chunk = chunk[remaining:]
		if pageWriter.Size() == obj.PageSize {
			p.commitCachePageWriter(ctx, target, pageIndex, pageWriter, stats)
			pageIndex++
			pageWriter = nil
		}
	}
	return pageIndex, pageWriter, nil
}

func (p *Proxy) beginCachePageWriter(ctx context.Context, target s3request.Target, obj cache.Object, pageIndex int64, stats *requestStats) (*cache.PageWriter, error) {
	writer, err := p.cache.BeginPageWrite(ctx, cache.PageWriteOptions{
		ObjectID:      obj.ID,
		Index:         pageIndex,
		ETag:          obj.ETag,
		ExpectedEpoch: obj.Epoch,
	})
	if err != nil {
		p.recordCacheWriteFailure(ctx, target, pageIndex, err, stats)
		return nil, err
	}
	return writer, nil
}

func (p *Proxy) commitCachePageWriter(ctx context.Context, target s3request.Target, pageIndex int64, writer *cache.PageWriter, stats *requestStats) {
	if _, err := writer.Commit(ctx); err != nil {
		p.recordCacheWriteFailure(ctx, target, pageIndex, err, stats)
	}
}

func (p *Proxy) recordCacheWriteFailure(ctx context.Context, target s3request.Target, pageIndex int64, err error, _ *requestStats) {
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
	if err := p.sign(req); err != nil {
		return cache.Object{}, 0, nil, false, err
	}

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

func (p *Proxy) selectReadStrategy(bucket string, objectSize int64) readStrategySelection {
	configured := p.readSharding
	if configured == "" {
		configured = readShardingAuto
	}
	pageSize := p.pageSizeForBucket(bucket)
	pageCount := pageCountForObject(objectSize, pageSize)
	strategy := configured
	if strategy == readShardingAuto {
		minPages := p.pageShardMin
		if minPages <= 0 {
			minPages = 2
		}
		if pageCount >= minPages {
			strategy = readShardingPage
		} else {
			strategy = readShardingObject
		}
	}
	return readStrategySelection{
		configuredStrategy: configured,
		strategy:           strategy,
		effectivePageSize:  pageSize,
		pageCount:          pageCount,
	}
}

func pageCountForObject(objectSize, pageSize int64) int64 {
	if objectSize <= 0 || pageSize <= 0 {
		return 0
	}
	return (objectSize + pageSize - 1) / pageSize
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

func (p *Proxy) serveDistributedPageRead(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, byteRange cacheplan.ByteRange, rangeResponse bool, stats *requestStats) (int, int64, error) {
	pages, err := cacheplan.PagesForRange(byteRange, obj.PageSize)
	if err != nil {
		return 0, 0, err
	}
	stats.pagesRequested = int64(len(pages))

	batches, err := p.openCoordinatorPageBatches(r, target, obj, pages, stats)
	if err != nil {
		closeCoordinatorPageBatches(batches)
		stats.cacheResult = "fallback"
		return p.forward(w, r, stats)
	}
	defer closeCoordinatorPageBatches(batches)

	writeCachedObjectHeaders(w.Header(), obj, rangeResponse)
	status := http.StatusOK
	if rangeResponse {
		status = http.StatusPartialContent
		w.Header().Set("Content-Length", strconv.FormatInt(byteRange.End-byteRange.Start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, obj.Size))
	}
	w.WriteHeader(status)

	start := time.Now()
	bytesWritten, err := p.streamCoordinatorPageSpans(w, r, target, obj, pages, batches, stats)
	stats.cacheServeDuration += time.Since(start)
	stats.finishCachedResult()
	return status, bytesWritten, err
}

func (p *Proxy) openCoordinatorPageBatches(r *http.Request, target s3request.Target, obj cache.Object, pages []cacheplan.PageSpan, stats *requestStats) (map[string]*coordinatorPageBatch, error) {
	batches := p.groupCoordinatorPageBatches(target, pages)
	for _, batch := range batches {
		if batch.owner.ID == p.peerRouter.LocalID() {
			continue
		}
		if err := p.openRemoteCoordinatorPageBatch(r, target, obj, batch, stats); err != nil {
			return batches, err
		}
	}
	return batches, nil
}

func (p *Proxy) groupCoordinatorPageBatches(target s3request.Target, pages []cacheplan.PageSpan) map[string]*coordinatorPageBatch {
	batches := make(map[string]*coordinatorPageBatch)
	for _, page := range pages {
		owner := p.peerRouter.PageOwner(target.Bucket, target.Key, page.Index)
		batch := batches[owner.ID]
		if batch == nil {
			batch = &coordinatorPageBatch{owner: owner}
			batches[owner.ID] = batch
		}
		batch.pages = append(batch.pages, page.Index)
	}
	return batches
}

func (p *Proxy) openRemoteCoordinatorPageBatch(r *http.Request, target s3request.Target, obj cache.Object, batch *coordinatorPageBatch, stats *requestStats) error {
	body := internalPageReadRequest{
		Bucket:     target.Bucket,
		Key:        target.Key,
		ObjectSize: obj.Size,
		PageSize:   obj.PageSize,
		ETag:       obj.ETag,
		Epoch:      obj.Epoch,
		Pages:      batch.pages,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	peerURL := strings.TrimRight(batch.owner.URL, "/") + "/internal/v1/pages/read"
	ctx := r.Context()
	if p.peerTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.peerTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peerURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(peerFromHeader, p.peerRouter.LocalID())
	req.Header.Set(peerRingHeader, p.peerRouter.RingID())

	headerStart := time.Now()
	resp, err := p.peerClient.Do(req)
	stats.peerResponseHeaderDuration += time.Since(headerStart)
	if err != nil {
		p.recordPeerReadFallback(r.Context(), target, batch.owner.ID, "request_failed", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		err := fmt.Errorf("peer %s page read returned status %d", batch.owner.ID, resp.StatusCode)
		p.recordPeerReadFallback(r.Context(), target, batch.owner.ID, "status", err)
		return err
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, peerrouter.PageFrameContentType) {
		defer resp.Body.Close()
		err := fmt.Errorf("peer %s page read content type %q", batch.owner.ID, got)
		p.recordPeerReadFallback(r.Context(), target, batch.owner.ID, "content_type", err)
		return err
	}
	reader, err := peerrouter.NewPageFrameReader(resp.Body, batch.pages, obj.PageSize)
	if err != nil {
		defer resp.Body.Close()
		p.recordPeerReadFallback(r.Context(), target, batch.owner.ID, "frame", err)
		return err
	}
	batch.body = resp.Body
	batch.reader = reader
	return nil
}

func (p *Proxy) recordPeerReadFallback(ctx context.Context, target s3request.Target, peerID, reason string, err error) {
	if p.metrics != nil {
		p.metrics.RecordPeerReadFallback(target.Bucket, peerID, reason)
	}
	p.logger.WarnContext(ctx, "distributed page read falling back to upstream pass-through",
		slog.String("bucket", target.Bucket),
		slog.String("key", target.Key),
		slog.String("peer_id", peerID),
		slog.String("reason", reason),
		slog.String("error", err.Error()),
	)
}

func closeCoordinatorPageBatches(batches map[string]*coordinatorPageBatch) {
	for _, batch := range batches {
		if batch.body != nil {
			_ = batch.body.Close()
		}
	}
}

func (p *Proxy) streamCoordinatorPageSpans(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, pages []cacheplan.PageSpan, batches map[string]*coordinatorPageBatch, stats *requestStats) (int64, error) {
	readers := make(map[string]*coordinatorPageReader)
	var total int64
	for _, page := range pages {
		owner := p.peerRouter.PageOwner(target.Bucket, target.Key, page.Index)
		batch := batches[owner.ID]
		if batch == nil {
			err := fmt.Errorf("missing page batch for owner %s", owner.ID)
			abortCommittedResponse(w, total)
			return total, err
		}
		n, err := p.streamCoordinatorPageSpan(w, r, target, obj, page, batch, readers, stats)
		stats.cacheResponseBytes += n
		total += n
		if err != nil {
			abortCommittedResponse(w, total)
			return total, err
		}
	}
	for _, batch := range batches {
		if batch.reader == nil {
			continue
		}
		readStart := time.Now()
		_, err := batch.reader.NextPage()
		stats.peerResponseBodyReadDuration += time.Since(readStart)
		if !errors.Is(err, io.EOF) {
			abortCommittedResponse(w, total)
			return total, err
		}
	}
	return total, nil
}

func (p *Proxy) streamCoordinatorPageSpan(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, page cacheplan.PageSpan, batch *coordinatorPageBatch, readers map[string]*coordinatorPageReader, stats *requestStats) (int64, error) {
	if batch.owner.ID == p.peerRouter.LocalID() {
		return p.streamPageSpan(w, r, target, obj, page, stats)
	}

	reader := readers[batch.owner.ID]
	if reader == nil {
		reader = &coordinatorPageReader{batch: batch}
		readers[batch.owner.ID] = reader
	}
	if !reader.loaded || reader.frame.Index != page.Index {
		readStart := time.Now()
		frame, err := batch.reader.NextPage()
		stats.peerResponseBodyReadDuration += time.Since(readStart)
		if err != nil {
			return 0, err
		}
		stats.peerResponseBodyReadChunks++
		stats.peerResponseBytes += int64(len(frame.Bytes))
		reader.frame = frame
		reader.loaded = true
	}
	if reader.frame.Index != page.Index {
		return 0, fmt.Errorf("peer %s returned page %d while coordinator wanted page %d", batch.owner.ID, reader.frame.Index, page.Index)
	}

	copyStart := time.Now()
	n, err := copyPageSpan(w, bytes.NewReader(reader.frame.Bytes), page, obj.PageSize)
	stats.cacheResponseCopyDuration += time.Since(copyStart)
	return n, err
}

func (p *Proxy) streamCachedPages(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, pages []cacheplan.PageSpan, firstPage io.ReadCloser, stats *requestStats) (int64, error) {
	var total int64
	for i, page := range pages {
		var n int64
		var err error
		if i == 0 {
			copyStart := time.Now()
			n, err = copyPageSpan(w, firstPage, page, obj.PageSize)
			stats.cacheResponseCopyDuration += time.Since(copyStart)
			if closeErr := firstPage.Close(); err == nil {
				err = closeErr
			}
		} else {
			n, err = p.streamPageSpan(w, r, target, obj, page, stats)
		}
		stats.cacheResponseBytes += int64(n)
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
	body, ok, err := p.openCachedPage(ctx, target, obj, index, stats, true)
	if err != nil {
		return nil, err
	}
	if ok {
		return body, nil
	}

	return p.fillMissingPage(ctx, r, target, obj, index, stats)
}

func (p *Proxy) openCachedPage(ctx context.Context, target s3request.Target, obj cache.Object, index int64, stats *requestStats, recordMiss bool) (io.ReadCloser, bool, error) {
	start := time.Now()
	body, ok, err := p.cache.OpenPage(ctx, obj.ID, index, obj.ETag, obj.Epoch)
	stats.cachePageOpenDuration += time.Since(start)
	if err != nil {
		return nil, false, err
	}
	if ok {
		stats.pagesHit++
		if p.metrics != nil {
			p.metrics.RecordPageHit(target.Bucket)
		}
		return body, true, nil
	}
	if recordMiss {
		stats.pagesMissed++
		if p.metrics != nil {
			p.metrics.RecordPageMiss(target.Bucket)
		}
	}
	return nil, false, nil
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

	release, acquired := p.tryAcquirePeerFill(obj.ID)
	if !acquired {
		err := errPeerFillOverloaded
		call.err = err
		p.pageFillMu.Lock()
		delete(p.pageFills, key)
		p.pageFillMu.Unlock()
		close(call.done)
		return nil, err
	}
	reader, err := p.fetchAndStorePage(ctx, r, target, obj, index, nil, stats)
	release()
	call.err = err

	p.pageFillMu.Lock()
	delete(p.pageFills, key)
	p.pageFillMu.Unlock()
	close(call.done)

	return reader, err
}

func (p *Proxy) fillMissingPageBytes(ctx context.Context, r *http.Request, target s3request.Target, obj cache.Object, index int64, stats *requestStats) ([]byte, error) {
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
				return p.fillMissingPageBytes(ctx, r, target, obj, index, stats)
			}
			return readAndClosePage(body, obj.PageSize)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &pageFillCall{done: make(chan struct{})}
	p.pageFills[key] = call
	p.pageFillMu.Unlock()

	release, acquired := p.tryAcquirePeerFill(obj.ID)
	if !acquired {
		err := errPeerFillOverloaded
		call.err = err
		p.pageFillMu.Lock()
		delete(p.pageFills, key)
		p.pageFillMu.Unlock()
		close(call.done)
		return nil, err
	}
	data, err := p.fetchPageBytesAndStoreBestEffort(ctx, r, target, obj, index, stats)
	release()
	call.err = err

	p.pageFillMu.Lock()
	delete(p.pageFills, key)
	p.pageFillMu.Unlock()
	close(call.done)

	return data, err
}

func (p *Proxy) streamPageSpan(w http.ResponseWriter, r *http.Request, target s3request.Target, obj cache.Object, page cacheplan.PageSpan, stats *requestStats) (int64, error) {
	body, ok, err := p.openCachedPage(r.Context(), target, obj, page.Index, stats, true)
	if err != nil {
		return 0, err
	}
	if ok {
		defer body.Close()
		copyStart := time.Now()
		n, err := copyPageSpan(w, body, page, obj.PageSize)
		stats.cacheResponseCopyDuration += time.Since(copyStart)
		return n, err
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

	release, acquired := p.tryAcquirePeerFill(obj.ID)
	if !acquired {
		err := errPeerFillOverloaded
		call.err = err
		p.pageFillMu.Lock()
		delete(p.pageFills, key)
		p.pageFillMu.Unlock()
		close(call.done)
		return 0, err
	}
	stream := newPageSpanStream(w, page, obj.PageSize)
	reader, err := p.fetchAndStorePage(r.Context(), r, target, obj, page.Index, stream, stats)
	release()
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

func (p *Proxy) tryAcquirePeerFill(objectID string) (func(), bool) {
	if p.peerRouter == nil {
		return func() {}, true
	}
	releaseGlobal, ok := tryAcquireSemaphore(p.peerFillSem)
	if !ok {
		return nil, false
	}
	releaseObject, ok := p.tryAcquireObjectFill(objectID)
	if !ok {
		releaseGlobal()
		return nil, false
	}
	return func() {
		releaseObject()
		releaseGlobal()
	}, true
}

func (p *Proxy) tryAcquireObjectFill(objectID string) (func(), bool) {
	if p.objectFillLimit <= 0 {
		return func() {}, true
	}
	p.objectFillMu.Lock()
	if p.objectFillSems == nil {
		p.objectFillSems = make(map[string]chan struct{})
	}
	sem := p.objectFillSems[objectID]
	if sem == nil {
		sem = make(chan struct{}, p.objectFillLimit)
		p.objectFillSems[objectID] = sem
	}
	p.objectFillMu.Unlock()
	return tryAcquireSemaphore(sem)
}

func tryAcquireSemaphore(sem chan struct{}) (func(), bool) {
	if sem == nil {
		return func() {}, true
	}
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	default:
		return nil, false
	}
}

func (p *Proxy) fetchPageBytesAndStoreBestEffort(ctx context.Context, r *http.Request, target s3request.Target, obj cache.Object, index int64, stats *requestStats) ([]byte, error) {
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
	if err := p.sign(req); err != nil {
		return nil, err
	}

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
		bodySample, readErr := readCappedBody(resp.Body, upstreamErrorBodyLogLimit)
		attrs := []slog.Attr{
			slog.String("method", req.Method),
			slog.String("bucket", target.Bucket),
			slog.String("key", target.Key),
			slog.String("upstream_path", req.URL.EscapedPath()),
			slog.String("upstream_host", req.Host),
			slog.String("upstream_url_host", req.URL.Host),
			slog.String("range", rangeHeader),
			slog.String("if_match", obj.ETag),
			slog.Int64("page_index", index),
			slog.Int("status", resp.StatusCode),
			slog.String("response_body", bodySample),
		}
		if readErr != nil {
			attrs = append(attrs, slog.String("response_body_read_error", readErr.Error()))
		}
		p.logger.LogAttrs(ctx, slog.LevelWarn, "upstream page fill returned non-206", attrs...)
		return nil, fmt.Errorf("fetch page %d: upstream status %d", index, resp.StatusCode)
	}

	expectedSize := bounds.End - bounds.Start + 1
	data, err := io.ReadAll(io.LimitReader(resp.Body, expectedSize+1))
	stats.upstreamDuration += time.Since(start)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != expectedSize {
		return nil, fmt.Errorf("fetch page %d: got %d bytes, want %d", index, len(data), expectedSize)
	}
	stats.bytesFetchedUpstream += int64(len(data))
	if p.metrics != nil {
		p.metrics.RecordUpstreamFillBytes(target.Bucket, int64(len(data)))
		p.metrics.ObserveUpstreamDuration(target.Bucket, "fill", time.Since(start))
	}

	writer, err := p.beginCachePageWriter(ctx, target, obj, index, stats)
	if err != nil {
		return data, nil
	}
	defer writer.Abort()
	if _, err := writer.Write(data); err != nil {
		p.recordCacheWriteFailure(ctx, target, index, err, stats)
		return data, nil
	}
	if _, err := writer.Commit(ctx); err != nil {
		p.recordCacheWriteFailure(ctx, target, index, err, stats)
		return data, nil
	}
	return data, nil
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
	if err := p.sign(req); err != nil {
		return nil, err
	}

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
		bodySample, readErr := readCappedBody(resp.Body, upstreamErrorBodyLogLimit)
		attrs := []slog.Attr{
			slog.String("method", req.Method),
			slog.String("bucket", target.Bucket),
			slog.String("key", target.Key),
			slog.String("upstream_path", req.URL.EscapedPath()),
			slog.String("upstream_host", req.Host),
			slog.String("upstream_url_host", req.URL.Host),
			slog.String("range", rangeHeader),
			slog.String("if_match", obj.ETag),
			slog.Int64("page_index", index),
			slog.Int("status", resp.StatusCode),
			slog.String("response_body", bodySample),
		}
		if readErr != nil {
			attrs = append(attrs, slog.String("response_body_read_error", readErr.Error()))
		}
		p.logger.LogAttrs(ctx, slog.LevelWarn, "upstream page fill returned non-206", attrs...)
		return nil, fmt.Errorf("fetch page %d: upstream status %d", index, resp.StatusCode)
	}
	writer, err := p.beginCachePageWriter(ctx, target, obj, index, stats)
	if err != nil {
		stats.upstreamDuration += time.Since(start)
		return nil, err
	}
	defer writer.Abort()
	size, err := copyPageResponseToWriter(resp.Body, writer, stream)
	if err != nil {
		stats.upstreamDuration += time.Since(start)
		return nil, err
	}
	stats.upstreamDuration += time.Since(start)
	if size != bounds.End-bounds.Start+1 {
		return nil, fmt.Errorf("fetch page %d: got %d bytes, want %d", index, size, bounds.End-bounds.Start+1)
	}
	stats.bytesFetchedUpstream += size
	if p.metrics != nil {
		p.metrics.RecordUpstreamFillBytes(target.Bucket, size)
		p.metrics.ObserveUpstreamDuration(target.Bucket, "fill", time.Since(start))
	}

	if _, err := writer.Commit(ctx); err != nil {
		p.recordCacheWriteFailure(ctx, target, index, err, stats)
		return nil, err
	}

	if stream != nil {
		return nil, nil
	}
	reader, ok, err := p.cache.OpenPage(ctx, obj.ID, index, obj.ETag, obj.Epoch)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("stored page %d was not readable after commit", index)
	}
	return reader, nil
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

func copyPageResponseToWriter(src io.Reader, dst io.Writer, stream *pageSpanStream) (int64, error) {
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			written, writeErr := dst.Write(chunk)
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
			if stream != nil {
				if _, writeErr := stream.Write(chunk); writeErr != nil {
					return total, writeErr
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
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
	p.applyUpstreamHost(req)
	return req, nil
}

func (p *Proxy) applyUpstreamHost(req *http.Request) {
	if p.upstreamHost == "" {
		return
	}
	req.Host = p.upstreamHost
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

func readCappedBody(body io.Reader, limit int64) (string, error) {
	if body == nil || limit <= 0 {
		return "", nil
	}
	data, err := io.ReadAll(io.LimitReader(body, limit))
	if err != nil {
		return "", err
	}
	return string(data), nil
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
		strings.EqualFold(key, peerFromHeader) ||
		strings.EqualFold(key, peerRingHeader)
}

func stripPeerHeaders(header http.Header) {
	for key := range header {
		if isPeerHeader(key) {
			header.Del(key)
		}
	}
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
