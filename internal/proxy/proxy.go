package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/ekkuleivonen/simple-s3-cache/internal/cache"
	"github.com/ekkuleivonen/simple-s3-cache/internal/cacheplan"
	appconfig "github.com/ekkuleivonen/simple-s3-cache/internal/config"
	"github.com/ekkuleivonen/simple-s3-cache/internal/s3request"
)

const unsignedPayload = "UNSIGNED-PAYLOAD"

var (
	errObjectChanged = errors.New("cached object changed upstream")
	errMetadataStore = errors.New("cache metadata store failed")
)

type cacheStore interface {
	PutObject(context.Context, cache.ObjectMetadata) (cache.Object, error)
	GetObject(context.Context, string, string) (cache.Object, bool, error)
	DeleteObject(context.Context, string, string) error
	StorePage(context.Context, cache.PageWrite) (cache.Page, error)
	OpenPage(context.Context, string, int64) (io.ReadCloser, bool, error)
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
}

func New(ctx context.Context, cfg appconfig.Config, logger *slog.Logger) (*Proxy, error) {
	upstreamEndpoint, err := url.Parse(cfg.Upstream.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse upstream endpoint: %w", err)
	}

	cacheStore, err := cache.Open(ctx, cache.Options{
		CachePath: cfg.Cache.CachePath,
		MetaPath:  cfg.Cache.MetaPath,
		MaxSize:   cfg.Cache.MaxSize,
	})
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Upstream.Region))
	if err != nil {
		_ = cacheStore.Close()
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if _, err := awsCfg.Credentials.Retrieve(ctx); err != nil {
		_ = cacheStore.Close()
		return nil, fmt.Errorf("load upstream credentials: %w", err)
	}

	return &Proxy{
		upstreamEndpoint: upstreamEndpoint,
		region:           cfg.Upstream.Region,
		credentials:      awsCfg.Credentials,
		signer:           v4.NewSigner(),
		client:           http.DefaultClient,
		logger:           logger,
		cache:            cacheStore,
		pageSize:         cfg.Cache.PageSize,
	}, nil
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

	start := time.Now()
	status, bytesWritten, err := p.handle(w, r, target, classification)
	attrs := []slog.Attr{
		slog.String("bucket", target.Bucket),
		slog.String("key", target.Key),
		slog.String("classification", string(classification.Disposition)),
		slog.String("classification_reason", classification.Reason),
		slog.Int("upstream_status", status),
		slog.Int64("upstream_duration_ms", time.Since(start).Milliseconds()),
		slog.Int64("upstream_bytes", bytesWritten),
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		p.logger.LogAttrs(r.Context(), slog.LevelError, "proxy request failed", attrs...)
		return
	}

	p.logger.LogAttrs(r.Context(), slog.LevelInfo, "proxy request", attrs...)
}

func (p *Proxy) handle(w http.ResponseWriter, r *http.Request, target s3request.Target, classification s3request.Classification) (int, int64, error) {
	if p.cache == nil {
		return p.forward(w, r)
	}

	switch classification.Disposition {
	case s3request.CacheableHeadObject:
		return p.serveCachedHead(w, r, target)
	case s3request.CacheableRangeObject:
		return p.serveCachedRange(w, r, target)
	case s3request.CacheableFullObject:
		return p.serveCachedFullObject(w, r, target)
	default:
		status, bytesWritten, err := p.forward(w, r)
		if err == nil && isSuccessfulStatus(status) && shouldInvalidateAfterWrite(r, target) {
			if deleteErr := p.cache.DeleteObject(r.Context(), target.Bucket, target.Key); deleteErr != nil {
				p.logger.WarnContext(r.Context(), "cache invalidation failed after successful write",
					slog.String("bucket", target.Bucket),
					slog.String("key", target.Key),
					slog.String("error", deleteErr.Error()),
				)
			}
		}
		return status, bytesWritten, err
	}
}

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	upstreamURL := p.upstreamURL(r)
	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL.String(), r.Body)
	if err != nil {
		http.Error(w, "build upstream request", http.StatusInternalServerError)
		return 0, 0, err
	}
	req.ContentLength = r.ContentLength
	copyRequestHeaders(req.Header, r.Header)
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)

	if err := p.sign(req); err != nil {
		http.Error(w, "sign upstream request", http.StatusBadGateway)
		return 0, 0, err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		http.Error(w, "upstream request failed", http.StatusBadGateway)
		return 0, 0, err
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	if r.Method == http.MethodHead {
		return resp.StatusCode, 0, nil
	}

	bytesWritten, copyErr := io.Copy(w, resp.Body)
	if copyErr != nil {
		return resp.StatusCode, bytesWritten, copyErr
	}

	return resp.StatusCode, bytesWritten, nil
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

func (p *Proxy) serveCachedHead(w http.ResponseWriter, r *http.Request, target s3request.Target) (int, int64, error) {
	obj, ok, err := p.cache.GetObject(r.Context(), target.Bucket, target.Key)
	if err != nil {
		http.Error(w, "read cached metadata", http.StatusInternalServerError)
		return 0, 0, err
	}
	if !ok {
		obj, status, headers, ok, err := p.fetchMetadata(r.Context(), r, target)
		if err != nil {
			if errors.Is(err, errMetadataStore) {
				return p.forward(w, r)
			}
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

	writeCachedObjectHeaders(w.Header(), obj, false)
	w.WriteHeader(http.StatusOK)
	return http.StatusOK, 0, nil
}

func (p *Proxy) serveCachedRange(w http.ResponseWriter, r *http.Request, target s3request.Target) (int, int64, error) {
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target)
	if err != nil {
		if errors.Is(err, errMetadataStore) {
			return p.forward(w, r)
		}
		http.Error(w, "fetch upstream metadata", http.StatusBadGateway)
		return 0, 0, err
	}
	if !ok {
		return p.forward(w, r)
	}

	byteRange, err := cacheplan.ParseRange(r.Header.Get("Range"), obj.Size)
	if err != nil {
		return p.forward(w, r)
	}

	pages, firstPage, err := p.prepareFirstPage(r, target, obj, byteRange)
	if errors.Is(err, errObjectChanged) {
		obj, byteRange, pages, firstPage, err = p.refetchAfterObjectChanged(r, target, byteRange)
	}
	if err != nil {
		http.Error(w, "fetch upstream page", http.StatusBadGateway)
		return 0, 0, err
	}

	writeCachedObjectHeaders(w.Header(), obj, true)
	w.Header().Set("Content-Length", strconv.FormatInt(byteRange.End-byteRange.Start+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, obj.Size))
	w.WriteHeader(http.StatusPartialContent)

	bytesWritten, err := p.streamCachedPages(w, r, target, obj, pages, firstPage)
	return http.StatusPartialContent, bytesWritten, err
}

func (p *Proxy) serveCachedFullObject(w http.ResponseWriter, r *http.Request, target s3request.Target) (int, int64, error) {
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target)
	if err != nil {
		if errors.Is(err, errMetadataStore) {
			return p.forward(w, r)
		}
		http.Error(w, "fetch upstream metadata", http.StatusBadGateway)
		return 0, 0, err
	}
	if !ok {
		return p.forward(w, r)
	}
	if obj.Size == 0 {
		writeCachedObjectHeaders(w.Header(), obj, false)
		w.WriteHeader(http.StatusOK)
		return http.StatusOK, 0, nil
	}

	byteRange := cacheplan.ByteRange{Start: 0, End: obj.Size - 1}
	pages, firstPage, err := p.prepareFirstPage(r, target, obj, byteRange)
	if errors.Is(err, errObjectChanged) {
		obj, byteRange, pages, firstPage, err = p.refetchAfterObjectChanged(r, target, byteRange)
	}
	if err != nil {
		http.Error(w, "fetch upstream page", http.StatusBadGateway)
		return 0, 0, err
	}

	writeCachedObjectHeaders(w.Header(), obj, false)
	w.WriteHeader(http.StatusOK)
	bytesWritten, err := p.streamCachedPages(w, r, target, obj, pages, firstPage)
	return http.StatusOK, bytesWritten, err
}

func (p *Proxy) ensureObjectMetadata(ctx context.Context, r *http.Request, target s3request.Target) (cache.Object, bool, error) {
	obj, ok, err := p.cache.GetObject(ctx, target.Bucket, target.Key)
	if err != nil || ok {
		return obj, ok, err
	}

	obj, _, _, ok, err = p.fetchMetadata(ctx, r, target)
	return obj, ok, err
}

func (p *Proxy) fetchMetadata(ctx context.Context, r *http.Request, target s3request.Target) (cache.Object, int, http.Header, bool, error) {
	req, err := p.newUpstreamRequest(ctx, r, http.MethodHead, nil)
	if err != nil {
		return cache.Object{}, 0, nil, false, err
	}
	req.Header.Del("Range")

	resp, err := p.client.Do(req)
	if err != nil {
		return cache.Object{}, 0, nil, false, err
	}
	defer resp.Body.Close()

	headers := responseHeaders(resp)
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
		PageSize: p.pageSize,
		Headers:  headers,
	})
	if err != nil {
		return cache.Object{}, resp.StatusCode, headers, false, fmt.Errorf("%w: %v", errMetadataStore, err)
	}

	return obj, resp.StatusCode, headers, true, nil
}

func (p *Proxy) prepareFirstPage(r *http.Request, target s3request.Target, obj cache.Object, byteRange cacheplan.ByteRange) ([]cacheplan.PageSpan, []byte, error) {
	pages, err := cacheplan.PagesForRange(byteRange, obj.PageSize)
	if err != nil {
		return nil, nil, err
	}
	if len(pages) == 0 {
		return pages, nil, nil
	}

	firstPage, err := p.pageData(r.Context(), r, target, obj, pages[0].Index)
	if err != nil {
		return nil, nil, err
	}

	return pages, firstPage, nil
}

func (p *Proxy) refetchAfterObjectChanged(r *http.Request, target s3request.Target, requestedRange cacheplan.ByteRange) (cache.Object, cacheplan.ByteRange, []cacheplan.PageSpan, []byte, error) {
	if err := p.cache.DeleteObject(r.Context(), target.Bucket, target.Key); err != nil {
		return cache.Object{}, cacheplan.ByteRange{}, nil, nil, err
	}
	obj, ok, err := p.ensureObjectMetadata(r.Context(), r, target)
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
		return cache.Object{}, cacheplan.ByteRange{}, nil, nil, errors.New("requested range is invalid after refetch")
	}

	pages, firstPage, err := p.prepareFirstPage(r, target, obj, requestedRange)
	return obj, requestedRange, pages, firstPage, err
}

func (p *Proxy) streamCachedPages(w io.Writer, r *http.Request, target s3request.Target, obj cache.Object, pages []cacheplan.PageSpan, firstPage []byte) (int64, error) {
	var total int64
	for i, page := range pages {
		data := firstPage
		if i != 0 {
			var err error
			data, err = p.pageData(r.Context(), r, target, obj, page.Index)
			if err != nil {
				return total, err
			}
		}
		start := page.Start - page.Index*obj.PageSize
		end := page.End - page.Index*obj.PageSize
		if start < 0 || end >= int64(len(data)) || start > end {
			return total, fmt.Errorf("cached page %d too short for requested range", page.Index)
		}
		n, err := w.Write(data[start : end+1])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (p *Proxy) pageData(ctx context.Context, r *http.Request, target s3request.Target, obj cache.Object, index int64) ([]byte, error) {
	body, ok, err := p.cache.OpenPage(ctx, obj.ID, index)
	if err != nil {
		return nil, err
	}
	if ok {
		defer body.Close()
		return io.ReadAll(body)
	}

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

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPreconditionFailed {
		return nil, errObjectChanged
	}
	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("fetch page %d: upstream status %d", index, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != bounds.End-bounds.Start+1 {
		return nil, fmt.Errorf("fetch page %d: got %d bytes, want %d", index, len(data), bounds.End-bounds.Start+1)
	}

	if _, err := p.cache.StorePage(ctx, cache.PageWrite{
		ObjectID:      obj.ID,
		Index:         index,
		ETag:          obj.ETag,
		ExpectedEpoch: obj.Epoch,
		Data:          data,
	}); err != nil {
		p.logger.WarnContext(ctx, "cache page store failed",
			slog.String("bucket", target.Bucket),
			slog.String("key", target.Key),
			slog.Int64("page_index", index),
			slog.String("error", err.Error()),
		)
	}

	return data, nil
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
	return p.signer.SignHTTP(req.Context(), credentials, req, unsignedPayload, "s3", p.region, time.Now())
}

func (p *Proxy) upstreamURL(r *http.Request) url.URL {
	upstreamURL := *p.upstreamEndpoint
	upstreamURL.Path = joinURLPath(p.upstreamEndpoint.Path, r.URL.EscapedPath())
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
	}
}

func copyRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) || isClientSigningHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
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
