package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"

	appconfig "github.com/ekkuleivonen/simple-s3-cache/internal/config"
	"github.com/ekkuleivonen/simple-s3-cache/internal/s3request"
)

const unsignedPayload = "UNSIGNED-PAYLOAD"

type Proxy struct {
	upstreamEndpoint *url.URL
	region           string
	credentials      aws.CredentialsProvider
	signer           *v4.Signer
	client           *http.Client
	logger           *slog.Logger
}

func New(ctx context.Context, cfg appconfig.Config, logger *slog.Logger) (*Proxy, error) {
	upstreamEndpoint, err := url.Parse(cfg.Upstream.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse upstream endpoint: %w", err)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Upstream.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if _, err := awsCfg.Credentials.Retrieve(ctx); err != nil {
		return nil, fmt.Errorf("load upstream credentials: %w", err)
	}

	return &Proxy{
		upstreamEndpoint: upstreamEndpoint,
		region:           cfg.Upstream.Region,
		credentials:      awsCfg.Credentials,
		signer:           v4.NewSigner(),
		client:           http.DefaultClient,
		logger:           logger,
	}, nil
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

	if !isReadOnlySupported(classification.Disposition) {
		http.Error(w, "request is not supported by read-only pass-through proxy", http.StatusNotImplemented)
		return
	}

	start := time.Now()
	status, bytesWritten, err := p.forward(w, r)
	attrs := []slog.Attr{
		slog.String("bucket", target.Bucket),
		slog.String("key", target.Key),
		slog.String("classification", string(classification.Disposition)),
		slog.Int("upstream_status", status),
		slog.Int64("upstream_duration_ms", time.Since(start).Milliseconds()),
		slog.Int64("upstream_bytes", bytesWritten),
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		p.logger.LogAttrs(r.Context(), slog.LevelError, "proxy read failed", attrs...)
		return
	}

	p.logger.LogAttrs(r.Context(), slog.LevelInfo, "proxy read", attrs...)
}

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request) (int, int64, error) {
	upstreamURL := p.upstreamURL(r)
	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL.String(), nil)
	if err != nil {
		http.Error(w, "build upstream request", http.StatusInternalServerError)
		return 0, 0, err
	}
	copyRequestHeaders(req.Header, r.Header)
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)

	credentials, err := p.credentials.Retrieve(r.Context())
	if err != nil {
		http.Error(w, "load upstream credentials", http.StatusBadGateway)
		return 0, 0, err
	}
	if err := p.signer.SignHTTP(r.Context(), credentials, req, unsignedPayload, "s3", p.region, time.Now()); err != nil {
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

func (p *Proxy) upstreamURL(r *http.Request) url.URL {
	upstreamURL := *p.upstreamEndpoint
	upstreamURL.Path = joinURLPath(p.upstreamEndpoint.Path, r.URL.EscapedPath())
	upstreamURL.RawPath = ""
	upstreamURL.RawQuery = r.URL.RawQuery
	return upstreamURL
}

func isReadOnlySupported(disposition s3request.Disposition) bool {
	switch disposition {
	case s3request.CacheableFullObject, s3request.CacheableHeadObject, s3request.CacheableRangeObject:
		return true
	default:
		return false
	}
}

func copyRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) || strings.EqualFold(key, "Authorization") {
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
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			key = strings.ToLower(key)
			dst[key] = append(dst[key], values...)
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
