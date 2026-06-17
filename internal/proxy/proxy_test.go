package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/ekkuleivonen/simple-s3-cache/internal/cache"
	appconfig "github.com/ekkuleivonen/simple-s3-cache/internal/config"
	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"
	peerrouter "github.com/ekkuleivonen/simple-s3-cache/internal/peer"
	"github.com/ekkuleivonen/simple-s3-cache/internal/s3request"
)

func TestProxyForwardsPassThroughRequests(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		rawQuery   string
		headers    http.Header
		body       []byte
		wantStatus int
	}{
		{
			name:       "get subresource",
			method:     http.MethodGet,
			path:       "/bucket/key",
			rawQuery:   "tagging=",
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "escaped path",
			method:     http.MethodGet,
			path:       "/bucket/path%20with%20spaces/plus+and%2525.txt",
			rawQuery:   "tagging=",
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "versioned get",
			method:     http.MethodGet,
			path:       "/bucket/key",
			rawQuery:   "versionId=123",
			wantStatus: http.StatusAccepted,
		},
		{
			name:   "conditional head",
			method: http.MethodHead,
			path:   "/bucket/key",
			headers: http.Header{
				"If-None-Match": []string{`"etag"`},
			},
			wantStatus: http.StatusNotModified,
		},
		{
			name:   "multi range get",
			method: http.MethodGet,
			path:   "/bucket/key",
			headers: http.Header{
				"Range": []string{"bytes=0-2,4-6"},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name:   "sse-c get",
			method: http.MethodGet,
			path:   "/bucket/key",
			headers: http.Header{
				"X-Amz-Server-Side-Encryption-Customer-Key": []string{"secret"},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "bucket operation",
			method:     http.MethodGet,
			path:       "/bucket",
			rawQuery:   "list-type=2&prefix=objects",
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "put object",
			method:     http.MethodPut,
			path:       "/bucket/key",
			body:       []byte("uploaded through proxy"),
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod, gotPath, gotRawQuery string
			var gotHeader http.Header
			var gotBody []byte

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				gotPath = r.URL.EscapedPath()
				gotRawQuery = r.URL.RawQuery
				gotHeader = r.Header.Clone()
				var err error
				gotBody, err = io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read upstream body: %v", err)
				}

				w.Header().Set("X-Upstream", "ok")
				w.WriteHeader(tt.wantStatus)
				if r.Method != http.MethodHead {
					_, _ = w.Write([]byte("upstream response"))
				}
			}))
			defer upstream.Close()

			p := testProxy(t, upstream.URL)
			reqURL := tt.path
			if tt.rawQuery != "" {
				reqURL += "?" + tt.rawQuery
			}
			req := httptest.NewRequest(tt.method, reqURL, bytes.NewReader(tt.body))
			for key, values := range tt.headers {
				req.Header[key] = append([]string(nil), values...)
			}
			rec := httptest.NewRecorder()

			p.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if gotMethod != tt.method {
				t.Fatalf("upstream method = %q, want %q", gotMethod, tt.method)
			}
			if gotPath != tt.path {
				t.Fatalf("upstream path = %q, want %q", gotPath, tt.path)
			}
			if gotRawQuery != tt.rawQuery {
				t.Fatalf("upstream raw query = %q, want %q", gotRawQuery, tt.rawQuery)
			}
			if !bytes.Equal(gotBody, tt.body) {
				t.Fatalf("upstream body = %q, want %q", gotBody, tt.body)
			}
			for key, values := range tt.headers {
				if got := gotHeader.Values(key); !equalStringSlices(got, values) {
					t.Fatalf("upstream header %s = %q, want %q", key, got, values)
				}
			}
		})
	}
}

func TestNewUpstreamHTTPClientUsesProductionTimeouts(t *testing.T) {
	client := newUpstreamHTTPClient(appconfig.UpstreamConfig{
		ResponseHeaderTimeout: 7 * time.Second,
	})

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport = %T, want *http.Transport", client.Transport)
	}
	if transport.ResponseHeaderTimeout != 7*time.Second {
		t.Fatalf("ResponseHeaderTimeout = %s, want 7s", transport.ResponseHeaderTimeout)
	}
	if transport.TLSHandshakeTimeout == 0 {
		t.Fatal("TLSHandshakeTimeout = 0, want non-zero")
	}
	if transport.IdleConnTimeout == 0 {
		t.Fatal("IdleConnTimeout = 0, want non-zero")
	}
	if transport.MaxIdleConnsPerHost == 0 {
		t.Fatal("MaxIdleConnsPerHost = 0, want non-zero")
	}
}

func TestNewSingleModeDoesNotRequirePeerConfig(t *testing.T) {
	var gotHeader http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("single mode upstream"))
	}))
	defer upstream.Close()

	root := t.TempDir()
	cfg := appconfig.Default()
	cfg.Upstream.Endpoint = upstream.URL
	cfg.Upstream.AccessKey = "test-access-key"
	cfg.Upstream.SecretKey = "test-secret-key"
	cfg.Cache.CachePath = filepath.Join(root, "cache-bytes")
	cfg.Cache.MetaPath = filepath.Join(root, "cache-meta")
	cfg.Peer.Mode = "single"
	cfg.Peer.LocalID = ""
	cfg.Peer.Peers = nil

	p, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if p.peerRouter != nil {
		t.Fatal("peerRouter is configured in single mode, want nil")
	}

	req := httptest.NewRequest(http.MethodGet, "/bucket/key?tagging=", nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "other-peer")
	req.Header.Set(peerRingHeader, "mismatched-ring")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "single mode upstream" {
		t.Fatalf("body = %q, want single mode upstream", got)
	}
	if got := gotHeader.Get(peerForwardedHeader); got != "" {
		t.Fatalf("upstream peer forwarded header = %q, want empty", got)
	}
	if got := gotHeader.Get(peerOwnerHeader); got != "" {
		t.Fatalf("upstream peer owner header = %q, want empty", got)
	}
	if got := gotHeader.Get(peerRingHeader); got != "" {
		t.Fatalf("upstream peer ring header = %q, want empty", got)
	}
}

func TestNewPeerModePreservesExistingCacheState(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	root := t.TempDir()
	cachePath := filepath.Join(root, "cache-bytes")
	metaPath := filepath.Join(root, "cache-meta")
	sentinelPath := filepath.Join(cachePath, "objects", "existing-page")
	if err := os.MkdirAll(filepath.Dir(sentinelPath), 0o755); err != nil {
		t.Fatalf("create sentinel dir: %v", err)
	}
	if err := os.WriteFile(sentinelPath, []byte("warm cache state"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	cfg := appconfig.Default()
	cfg.Upstream.Endpoint = upstream.URL
	cfg.Upstream.AccessKey = "test-access-key"
	cfg.Upstream.SecretKey = "test-secret-key"
	cfg.Cache.CachePath = cachePath
	cfg.Cache.MetaPath = metaPath
	cfg.Peer.Mode = "peer"
	cfg.Peer.LocalID = "cache-0"
	cfg.Peer.AuthSecret = "test-peer-secret"
	cfg.Peer.Peers = []appconfig.Peer{
		{ID: "cache-0", URL: "http://cache-0.example.test:8080"},
	}

	p, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	got, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatalf("read sentinel after New: %v", err)
	}
	if string(got) != "warm cache state" {
		t.Fatalf("sentinel = %q, want preserved warm cache state", got)
	}
}

func TestNewPreparesConfiguredUploadSpoolPath(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	root := t.TempDir()
	spoolPath := filepath.Join(root, "upload-spool")
	cfg := appconfig.Default()
	cfg.Upstream.Endpoint = upstream.URL
	cfg.Upstream.AccessKey = "test-access-key"
	cfg.Upstream.SecretKey = "test-secret-key"
	cfg.Cache.CachePath = filepath.Join(root, "cache-bytes")
	cfg.Cache.MetaPath = filepath.Join(root, "cache-meta")
	cfg.Upload.SpoolPath = spoolPath

	p, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	info, err := os.Stat(spoolPath)
	if err != nil {
		t.Fatalf("stat spool path: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("spool path is not a directory")
	}
}

func TestNewRejectsUploadSpoolPathThatIsFile(t *testing.T) {
	root := t.TempDir()
	spoolPath := filepath.Join(root, "upload-spool")
	if err := os.WriteFile(spoolPath, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write spool file: %v", err)
	}

	cfg := appconfig.Default()
	cfg.Upstream.Endpoint = "http://upstream.invalid"
	cfg.Upstream.AccessKey = "test-access-key"
	cfg.Upstream.SecretKey = "test-secret-key"
	cfg.Cache.CachePath = filepath.Join(root, "cache-bytes")
	cfg.Cache.MetaPath = filepath.Join(root, "cache-meta")
	cfg.Upload.SpoolPath = spoolPath

	_, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("New() error = nil, want spool path validation error")
	}
	if !strings.Contains(err.Error(), "prepare upload spool path") {
		t.Fatalf("New() error = %v, want upload spool path error", err)
	}
}

func TestProxyPublicBoundaryStripsSpoofedPeerHeaders(t *testing.T) {
	var gotHeader http.Header
	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		gotHeader = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("public request"))
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyOwnedBy(t, router, "bucket", "cache-0")
	req := httptest.NewRequest(http.MethodPut, "/bucket/"+key, strings.NewReader("body"))
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "cache-1")
	req.Header.Set(peerFromHeader, "cache-1")
	req.Header.Set(peerRingHeader, "spoofed-ring")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstream requests = %d, want 1", upstreamRequests)
	}
	for _, header := range []string{peerForwardedHeader, peerOwnerHeader, peerFromHeader, peerRingHeader} {
		if got := gotHeader.Get(header); got != "" {
			t.Fatalf("upstream %s = %q, want stripped", header, got)
		}
	}
}

func TestProxyReSignsInsteadOfForwardingClientSigV4Headers(t *testing.T) {
	var gotHeader http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	req := httptest.NewRequest(http.MethodGet, "/bucket/key?tagging=", nil)
	req.Header.Set("Authorization", "client signature")
	req.Header.Set("X-Amz-Date", "20000101T000000Z")
	req.Header.Set("X-Amz-Security-Token", "client-token")
	req.Header.Set("X-Amz-Content-Sha256", "client-payload-hash")

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := gotHeader.Get("Authorization"); got == "" || got == "client signature" {
		t.Fatalf("Authorization was not re-signed: %q", got)
	}
	if got := gotHeader.Get("X-Amz-Date"); got == "" || got == "20000101T000000Z" {
		t.Fatalf("X-Amz-Date was not regenerated: %q", got)
	}
	if got := gotHeader.Get("X-Amz-Security-Token"); got != "" {
		t.Fatalf("X-Amz-Security-Token = %q, want empty for static credentials", got)
	}
	if got := gotHeader.Get("X-Amz-Content-Sha256"); got != unsignedPayload {
		t.Fatalf("X-Amz-Content-Sha256 = %q, want %q", got, unsignedPayload)
	}
}

func TestProxyUsesConfiguredUpstreamHostWhileDialingEndpoint(t *testing.T) {
	var gotHost, gotPath string
	var gotAuthorization string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		gotPath = r.URL.EscapedPath()
		gotAuthorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithUpstreamHost(t, upstream.URL, "192.168.30.216:9000")
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("body"))
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if gotHost != "192.168.30.216:9000" {
		t.Fatalf("upstream Host = %q, want configured signing host", gotHost)
	}
	if gotPath != "/bucket/key" {
		t.Fatalf("upstream path = %q, want path-style bucket/key", gotPath)
	}
	if !strings.Contains(gotAuthorization, "SignedHeaders=") || !strings.Contains(gotAuthorization, "host") {
		t.Fatalf("Authorization = %q, want SigV4 signature over host header", gotAuthorization)
	}
}

func TestNewUpstreamRequestKeepsDialEndpointWhenUpstreamHostIsConfigured(t *testing.T) {
	p := testProxyWithUpstreamHost(t, "http://rustfs-api.rustfs.svc.cluster.local:9000", "192.168.30.216:9000")
	clientReq := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)

	req, err := p.newUpstreamRequest(context.Background(), clientReq, http.MethodHead, nil)
	if err != nil {
		t.Fatalf("newUpstreamRequest() error = %v", err)
	}

	if req.URL.Host != "rustfs-api.rustfs.svc.cluster.local:9000" {
		t.Fatalf("URL.Host = %q, want dial endpoint host", req.URL.Host)
	}
	if req.Host != "192.168.30.216:9000" {
		t.Fatalf("Host = %q, want configured signing host", req.Host)
	}
	if req.URL.EscapedPath() != "/bucket/key" {
		t.Fatalf("path = %q, want path-style bucket/key", req.URL.EscapedPath())
	}
}

func TestProxyForwardsStreamingPutBodyWithContentLength(t *testing.T) {
	body := []byte("streamed through chunked transfer encoding")
	var gotBody []byte
	var gotContentLength int64
	var gotTransferEncoding []string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotContentLength = r.ContentLength
		gotTransferEncoding = append([]string(nil), r.TransferEncoding...)
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	req := httptest.NewRequest(http.MethodPut, "/bucket/chunked.bin", newChunkedTestReader(
		[]byte("streamed "),
		[]byte("through "),
		[]byte("chunked transfer encoding"),
	))
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("upstream body = %q, want %q", gotBody, body)
	}
	if gotContentLength != int64(len(body)) {
		t.Fatalf("upstream ContentLength = %d, want %d", gotContentLength, len(body))
	}
	if len(gotTransferEncoding) != 0 {
		t.Fatalf("upstream TransferEncoding = %q, want none after proxy frames body", gotTransferEncoding)
	}
}

func TestProxyDecodesAWSChunkedPutBodyBeforeResigning(t *testing.T) {
	body := []byte("decoded aws chunked payload")
	var gotBody []byte
	var gotContentEncoding string
	var gotDecodedContentLength string
	var gotContentSHA256 string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotContentEncoding = r.Header.Get("Content-Encoding")
		gotDecodedContentLength = r.Header.Get("X-Amz-Decoded-Content-Length")
		gotContentSHA256 = r.Header.Get("X-Amz-Content-Sha256")
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	req := httptest.NewRequest(http.MethodPut, "/bucket/aws-chunked.bin", bytes.NewReader(awsChunkedBody(
		[]byte("decoded aws "),
		[]byte("chunked payload"),
	)))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(body)))
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if !bytes.Equal(gotBody, body) {
		t.Fatalf("upstream body = %q, want %q", gotBody, body)
	}
	if gotContentEncoding != "" {
		t.Fatalf("upstream Content-Encoding = %q, want empty", gotContentEncoding)
	}
	if gotDecodedContentLength != "" {
		t.Fatalf("upstream X-Amz-Decoded-Content-Length = %q, want empty", gotDecodedContentLength)
	}
	if gotContentSHA256 != unsignedPayload {
		t.Fatalf("upstream X-Amz-Content-Sha256 = %q, want %q", gotContentSHA256, unsignedPayload)
	}
}

func TestForwardBodyStreamsAWSChunkedWithDecodedLengthWithoutSpooling(t *testing.T) {
	body := []byte("decoded streaming body")
	req := httptest.NewRequest(http.MethodPut, "/bucket/streamed.bin", bytes.NewReader(awsChunkedBody(body[:8], body[8:])))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(body)))
	req.ContentLength = -1

	spoolPath := filepath.Join(t.TempDir(), "spool-parent-does-not-exist", "spool")
	reader, contentLength, getBody, cleanup, decodedAWSChunked, err := forwardBody(req, uploadOptions{
		spoolPath:    spoolPath,
		maxSpoolSize: 1,
	})
	if err != nil {
		t.Fatalf("forwardBody() error = %v", err)
	}
	if contentLength != int64(len(body)) {
		t.Fatalf("contentLength = %d, want %d", contentLength, len(body))
	}
	if getBody != nil {
		t.Fatal("getBody != nil, want streaming body without replay")
	}
	if cleanup != nil {
		t.Fatal("cleanup != nil, want no spool cleanup")
	}
	if !decodedAWSChunked {
		t.Fatal("decodedAWSChunked = false, want true")
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read decoded body: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("decoded body = %q, want %q", got, body)
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spool path stat error = %v, want not exist", err)
	}
}

func TestForwardBodyRejectsFallbackSpoolOverLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/bucket/unknown-length.bin", bytes.NewReader([]byte("too large")))
	req.ContentLength = -1

	_, _, _, _, _, err := forwardBody(req, uploadOptions{
		spoolPath:    t.TempDir(),
		maxSpoolSize: 3,
	})
	if !errors.Is(err, errSpoolLimit) {
		t.Fatalf("forwardBody() error = %v, want %v", err, errSpoolLimit)
	}
}

func TestProxyCachesHeadMetadata(t *testing.T) {
	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "11")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", `"etag-head"`)
		w.Header().Set("X-Amz-Meta-Test", "metadata")
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodHead, "/bucket/object.txt", nil)
		rec := httptest.NewRecorder()

		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("HEAD %d status = %d, want 200", i+1, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); got != "text/plain" {
			t.Fatalf("HEAD %d Content-Type = %q, want text/plain", i+1, got)
		}
		if got := rec.Header().Get("ETag"); got != `"etag-head"` {
			t.Fatalf("HEAD %d ETag = %q, want etag", i+1, got)
		}
		if got := rec.Header().Get("X-Amz-Meta-Test"); got != "metadata" {
			t.Fatalf("HEAD %d metadata = %q, want metadata", i+1, got)
		}
	}

	if upstreamRequests != 1 {
		t.Fatalf("upstream requests = %d, want 1", upstreamRequests)
	}
}

func TestProxyStoresBucketSpecificPageSizeInMetadata(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag-page-size"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithBucketPageSizes(t, upstream.URL, 4, map[string]int64{
		"analytics": 2,
		"media":     8,
	})

	for _, tt := range []struct {
		bucket       string
		wantPageSize int64
	}{
		{bucket: "analytics", wantPageSize: 2},
		{bucket: "media", wantPageSize: 8},
		{bucket: "other", wantPageSize: 4},
	} {
		req := httptest.NewRequest(http.MethodHead, "/"+tt.bucket+"/object.bin", nil)
		rec := httptest.NewRecorder()

		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s HEAD status = %d, want 200", tt.bucket, rec.Code)
		}
		obj, ok, err := p.cache.GetObject(context.Background(), tt.bucket, "object.bin")
		if err != nil {
			t.Fatalf("%s GetObject() error = %v", tt.bucket, err)
		}
		if !ok {
			t.Fatalf("%s object metadata was not cached", tt.bucket)
		}
		if obj.PageSize != tt.wantPageSize {
			t.Fatalf("%s PageSize = %d, want %d", tt.bucket, obj.PageSize, tt.wantPageSize)
		}
	}
}

func TestProxyUsesBucketSpecificPageSizeForRangeFetch(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.URL.Path+" "+r.Header.Get("Range"))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag-bucket-page-size"`)
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
			return
		}

		switch r.Header.Get("Range") {
		case "bytes=3-5":
			w.Header().Set("Content-Length", "3")
			w.Header().Set("Content-Range", "bytes 3-5/12")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[3:6])
		case "bytes=0-5":
			w.Header().Set("Content-Length", "6")
			w.Header().Set("Content-Range", "bytes 0-5/12")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[0:6])
		case "bytes=4-7":
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Content-Range", "bytes 4-7/12")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[4:8])
		default:
			t.Fatalf("unexpected upstream range %q", r.Header.Get("Range"))
		}
	}))
	defer upstream.Close()

	p := testProxyWithBucketPageSizes(t, upstream.URL, 4, map[string]int64{
		"analytics": 3,
		"media":     6,
	})

	for _, path := range []string{
		"/analytics/object.bin",
		"/media/object.bin",
		"/other/object.bin",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Range", "bytes=4-4")
		rec := httptest.NewRecorder()

		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusPartialContent {
			t.Fatalf("%s status = %d, want 206; body=%q", path, rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); got != "e" {
			t.Fatalf("%s body = %q, want %q", path, got, "e")
		}
	}

	wantRequests := []string{
		"HEAD /analytics/object.bin ",
		"GET /analytics/object.bin bytes=3-5",
		"HEAD /media/object.bin ",
		"GET /media/object.bin bytes=0-5",
		"HEAD /other/object.bin ",
		"GET /other/object.bin bytes=4-7",
	}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxySelectReadStrategyUsesGlobalAndBucketPageSizes(t *testing.T) {
	p := &Proxy{
		pageSize:     4,
		readSharding: readShardingAuto,
		pageShardMin: 3,
		pageSizeByBucket: map[string]int64{
			"small-pages": 2,
			"large-pages": 8,
		},
	}

	tests := []struct {
		name            string
		bucket          string
		objectSize      int64
		readSharding    string
		wantStrategy    string
		wantPageSize    int64
		wantObjectPages int64
	}{
		{
			name:            "global one page auto object",
			bucket:          "default",
			objectSize:      4,
			wantStrategy:    readShardingObject,
			wantPageSize:    4,
			wantObjectPages: 1,
		},
		{
			name:            "global threshold auto page",
			bucket:          "default",
			objectSize:      9,
			wantStrategy:    readShardingPage,
			wantPageSize:    4,
			wantObjectPages: 3,
		},
		{
			name:            "bucket small page size crosses threshold",
			bucket:          "small-pages",
			objectSize:      5,
			wantStrategy:    readShardingPage,
			wantPageSize:    2,
			wantObjectPages: 3,
		},
		{
			name:            "bucket large page size stays object",
			bucket:          "large-pages",
			objectSize:      12,
			wantStrategy:    readShardingObject,
			wantPageSize:    8,
			wantObjectPages: 2,
		},
		{
			name:            "forced object",
			bucket:          "small-pages",
			objectSize:      12,
			readSharding:    readShardingObject,
			wantStrategy:    readShardingObject,
			wantPageSize:    2,
			wantObjectPages: 6,
		},
		{
			name:            "forced page",
			bucket:          "large-pages",
			objectSize:      1,
			readSharding:    readShardingPage,
			wantStrategy:    readShardingPage,
			wantPageSize:    8,
			wantObjectPages: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p.readSharding = readShardingAuto
			if tt.readSharding != "" {
				p.readSharding = tt.readSharding
			}

			got := p.selectReadStrategy(tt.bucket, tt.objectSize)
			if got.strategy != tt.wantStrategy {
				t.Fatalf("strategy = %q, want %q", got.strategy, tt.wantStrategy)
			}
			if got.effectivePageSize != tt.wantPageSize {
				t.Fatalf("effectivePageSize = %d, want %d", got.effectivePageSize, tt.wantPageSize)
			}
			if got.pageCount != tt.wantObjectPages {
				t.Fatalf("pageCount = %d, want %d", got.pageCount, tt.wantObjectPages)
			}
		})
	}
}

func TestProxyServesConditionalNotModifiedFromCachedMetadata(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeObjectResponse(t, w, r, body, `"etag-conditional"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	warmReq := httptest.NewRequest(http.MethodHead, "/bucket/conditional.bin", nil)
	warmRec := httptest.NewRecorder()
	p.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusOK {
		t.Fatalf("warm HEAD status = %d, want 200", warmRec.Code)
	}

	for _, tt := range []struct {
		name   string
		method string
		header string
		value  string
	}{
		{
			name:   "head if-none-match",
			method: http.MethodHead,
			header: "If-None-Match",
			value:  `"etag-conditional"`,
		},
		{
			name:   "get if-none-match",
			method: http.MethodGet,
			header: "If-None-Match",
			value:  `"etag-conditional"`,
		},
		{
			name:   "get if-modified-since",
			method: http.MethodGet,
			header: "If-Modified-Since",
			value:  "Tue, 16 Jun 2026 00:00:00 GMT",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/bucket/conditional.bin", nil)
			req.Header.Set(tt.header, tt.value)
			rec := httptest.NewRecorder()

			p.ServeHTTP(rec, req)

			if rec.Code != http.StatusNotModified {
				t.Fatalf("status = %d, want 304; body=%q", rec.Code, rec.Body.String())
			}
			if rec.Body.Len() != 0 {
				t.Fatalf("body length = %d, want 0", rec.Body.Len())
			}
			if got := rec.Header().Get("ETag"); got != `"etag-conditional"` {
				t.Fatalf("ETag = %q, want cached etag", got)
			}
		})
	}

	if upstreamRequests != 1 {
		t.Fatalf("upstream requests = %d, want 1; conditionals should use cached metadata", upstreamRequests)
	}
}

func TestProxyServesConditionalPreconditionFailedFromCachedMetadata(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeObjectResponse(t, w, r, body, `"etag-precondition"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	warmReq := httptest.NewRequest(http.MethodHead, "/bucket/precondition.bin", nil)
	warmRec := httptest.NewRecorder()
	p.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusOK {
		t.Fatalf("warm HEAD status = %d, want 200", warmRec.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/bucket/precondition.bin", nil)
	req.Header.Set("If-Match", `"different-etag"`)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412; body=%q", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", rec.Body.Len())
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstream requests = %d, want 1; precondition should use cached metadata", upstreamRequests)
	}
}

func TestProxyUsesStrongETagComparisonForIfMatch(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		writeObjectResponse(t, w, r, body, `"etag-strong"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	warmReq := httptest.NewRequest(http.MethodHead, "/bucket/strong-etag.bin", nil)
	warmRec := httptest.NewRecorder()
	p.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusOK {
		t.Fatalf("warm HEAD status = %d, want 200", warmRec.Code)
	}

	req := httptest.NewRequest(http.MethodHead, "/bucket/strong-etag.bin", nil)
	req.Header.Set("If-Match", `W/"etag-strong"`)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412; body=%q", rec.Code, rec.Body.String())
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstream requests = %d, want 1; weak If-Match should be answered from cached metadata", upstreamRequests)
	}
}

func TestProxyPassesThroughDateConditionalWhenCachedMetadataIsInsufficient(t *testing.T) {
	ctx := context.Background()
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("If-Modified-Since"))
		w.Header().Set("ETag", `"etag-no-date"`)
		w.WriteHeader(http.StatusNotModified)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	_, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      "no-last-modified.bin",
		ETag:     `"etag-no-date"`,
		Size:     12,
		PageSize: 4,
		Headers: http.Header{
			"Content-Length": []string{"12"},
			"Content-Type":   []string{"application/octet-stream"},
			"ETag":           []string{`"etag-no-date"`},
		},
	})
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/bucket/no-last-modified.bin", nil)
	req.Header.Set("If-Modified-Since", "Tue, 16 Jun 2026 00:00:00 GMT")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304 from upstream; body=%q", rec.Code, rec.Body.String())
	}
	wantRequests := []string{"GET Tue, 16 Jun 2026 00:00:00 GMT"}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxySignsPageFillWithFinalRangeHeaders(t *testing.T) {
	body := []byte("abcdefghijkl")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag-signed-page"`)
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			assertRequestSignatureMatchesFinalHeaders(t, r)
			w.WriteHeader(http.StatusOK)
			return
		}

		if got := r.Header.Get("Range"); got != "bytes=4-7" {
			t.Fatalf("upstream Range = %q, want page fill range", got)
		}
		if got := r.Header.Get("If-Match"); got != `"etag-signed-page"` {
			t.Fatalf("upstream If-Match = %q, want cached ETag", got)
		}
		assertRequestSignatureMatchesFinalHeaders(t, r)
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 4-7/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[4:8])
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	req := httptest.NewRequest(http.MethodGet, "/bucket/object.bin", nil)
	req.Header.Set("Range", "bytes=5-5")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "f" {
		t.Fatalf("body = %q, want %q", got, "f")
	}
}

func TestProxyCachesSinglePageRangeRead(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		w.Header().Set("X-Amz-Checksum-Crc32", "full-object-checksum")
		writeObjectResponse(t, w, r, body, `"etag-range"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	var storePageCalls atomic.Int32
	p.cache = &storePageCountingCache{
		cacheStore: p.cache,
		calls:      &storePageCalls,
	}
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/bucket/object.bin", nil)
		req.Header.Set("Range", "bytes=1-3")
		rec := httptest.NewRecorder()

		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusPartialContent {
			t.Fatalf("GET range %d status = %d, want 206; body=%q", i+1, rec.Code, rec.Body.String())
		}
		if got := rec.Body.Bytes(); !bytes.Equal(got, body[1:4]) {
			t.Fatalf("GET range %d body = %q, want %q", i+1, got, body[1:4])
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 1-3/12" {
			t.Fatalf("GET range %d Content-Range = %q", i+1, got)
		}
		if got := rec.Header().Get("X-Amz-Checksum-Crc32"); got != "" {
			t.Fatalf("GET range %d X-Amz-Checksum-Crc32 = %q, want empty", i+1, got)
		}
	}

	wantRequests := []string{"HEAD ", "GET bytes=0-3"}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
	if got := storePageCalls.Load(); got != 0 {
		t.Fatalf("StorePage() calls = %d, want 0 with direct page writer", got)
	}
}

func TestProxyCoalescesConcurrentFetchesForSameMissingPage(t *testing.T) {
	body := []byte("abcdefghijkl")
	firstFillStarted := make(chan struct{})
	releaseFill := make(chan struct{})
	secondMissSeen := make(chan struct{})
	var fillRequests atomic.Int32
	var firstFill atomic.Bool
	var misses atomic.Int32
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseFill)
		})
	}
	defer release()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			fillRequests.Add(1)
			if firstFill.CompareAndSwap(false, true) {
				close(firstFillStarted)
			}
			<-releaseFill
		}
		writeObjectResponse(t, w, r, body, `"etag-coalesce"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.cache = &missingPageSignalCache{
		cacheStore:     p.cache,
		misses:         &misses,
		secondMissSeen: secondMissSeen,
	}
	warmReq := httptest.NewRequest(http.MethodHead, "/bucket/coalesce.bin", nil)
	warmRec := httptest.NewRecorder()
	p.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusOK {
		t.Fatalf("warm HEAD status = %d, want 200; body=%q", warmRec.Code, warmRec.Body.String())
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- serveRangeRequest(p, "/bucket/coalesce.bin", "bytes=1-3", body[1:4])
	}()

	select {
	case <-firstFillStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first upstream fill")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- serveRangeRequest(p, "/bucket/coalesce.bin", "bytes=1-3", body[1:4])
	}()

	select {
	case <-secondMissSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second request to observe the missing page")
	}
	if got := fillRequests.Load(); got != 1 {
		t.Fatalf("upstream fill requests while first fill is blocked = %d, want 1", got)
	}
	release()

	for i, done := range []chan error{firstDone, secondDone} {
		if err := <-done; err != nil {
			t.Fatalf("range request %d failed: %v", i+1, err)
		}
	}
	if got := fillRequests.Load(); got != 1 {
		t.Fatalf("upstream fill requests = %d, want 1", got)
	}
}

func TestProxyCachesMultiPageRangeRead(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		writeObjectResponse(t, w, r, body, `"etag-multi-range"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/bucket/object.bin", nil)
		req.Header.Set("Range", "bytes=2-7")
		rec := httptest.NewRecorder()

		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusPartialContent {
			t.Fatalf("GET multi-page range %d status = %d, want 206; body=%q", i+1, rec.Code, rec.Body.String())
		}
		if got := rec.Body.Bytes(); !bytes.Equal(got, body[2:8]) {
			t.Fatalf("GET multi-page range %d body = %q, want %q", i+1, got, body[2:8])
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 2-7/12" {
			t.Fatalf("GET multi-page range %d Content-Range = %q", i+1, got)
		}
	}

	wantRequests := []string{"HEAD ", "GET bytes=0-3", "GET bytes=4-7"}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyRecordsRangeMetricsAndStructuredLogFields(t *testing.T) {
	body := []byte("abcdefghijkl")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeObjectResponse(t, w, r, body, `"etag-observe"`)
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.peerRouter = testPeerRouter(t)
	p.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	p.metrics = metrics.NewRecorder(1 << 20)

	req := httptest.NewRequest(http.MethodGet, "/photos/object.bin", nil)
	req.Header.Set("Range", "bytes=2-7")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206; body=%q", rec.Code, rec.Body.String())
	}

	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatalf("decode log entry: %v\nlog: %s", err, logs.String())
	}
	for key, want := range map[string]any{
		"method":                   http.MethodGet,
		"bucket":                   "photos",
		"key":                      "object.bin",
		"cache_result":             "miss",
		"requested_range":          "bytes=2-7",
		"bytes_requested":          float64(6),
		"pages_requested":          float64(2),
		"pages_hit":                float64(0),
		"pages_missed":             float64(2),
		"status":                   float64(http.StatusPartialContent),
		"bytes_sent":               float64(6),
		"bytes_fetched_upstream":   float64(8),
		"read_strategy":            "page",
		"configured_read_sharding": "auto",
		"effective_page_size":      float64(4),
		"object_page_count":        float64(3),
		"coordinator_id":           "cache-0",
		"etag":                     `"etag-observe"`,
		"epoch":                    float64(0),
		"internal_peer_requests":   float64(0),
	} {
		if entry[key] != want {
			t.Fatalf("log field %s = %#v, want %#v\nentry=%#v", key, entry[key], want, entry)
		}
	}
	if got, ok := entry["page_indexes"].([]any); !ok || len(got) != 2 || got[0] != float64(0) || got[1] != float64(1) {
		t.Fatalf("page_indexes = %#v, want [0 1]", entry["page_indexes"])
	}
	if got, ok := entry["page_owner_ids"].([]any); !ok || len(got) != 2 || got[0] != "cache-0" || got[1] != "cache-0" {
		t.Fatalf("page_owner_ids = %#v, want [cache-0 cache-0]", entry["page_owner_ids"])
	}
	if got, ok := entry["read_amplification"].(float64); !ok || got < 1.33 || got > 1.34 {
		t.Fatalf("read_amplification = %#v, want about 1.333", entry["read_amplification"])
	}

	metricsBody := renderProxyMetrics(t, p.metrics)
	for _, want := range []string{
		`simple_s3_cache_page_misses_total{bucket="photos"} 2`,
		`simple_s3_cache_upstream_fill_bytes_total{bucket="photos"} 8`,
		`simple_s3_cache_requested_bytes_sum{bucket="photos"} 6`,
		`simple_s3_cache_pages_touched_sum{bucket="photos"} 2`,
		`simple_s3_cache_read_amplification_sum{bucket="photos"} 1.333`,
		`simple_s3_cache_read_strategy_selected_total{bucket="photos",strategy="page"} 1`,
		`simple_s3_cache_coordinator_requests_total{bucket="photos",method="GET",strategy="page",status_class="2xx"} 1`,
		`simple_s3_cache_internal_peer_requests_per_client_request_sum{bucket="photos",strategy="page"} 0`,
		`simple_s3_cache_page_batch_size_sum{bucket="photos",owner_id="cache-0"} 2`,
	} {
		if !bytes.Contains([]byte(metricsBody), []byte(want)) {
			t.Fatalf("metrics body missing %q:\n%s", want, metricsBody)
		}
	}
}

func TestProxyRecordsUpstreamFailureWhenPageFillReturnsUnexpectedStatus(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag-fail-fill"`)
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.metrics = metrics.NewRecorder(1 << 20)
	req := httptest.NewRequest(http.MethodGet, "/photos/object.bin", nil)
	req.Header.Set("Range", "bytes=0-3")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !bytes.Contains([]byte(metricsBody), []byte(`simple_s3_cache_upstream_request_failures_total{bucket="photos",operation="fill"} 1`)) {
		t.Fatalf("metrics body missing upstream fill failure:\n%s", metricsBody)
	}
}

func TestProxyStreamsMultiPageFullGetThroughCache(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		writeObjectResponse(t, w, r, body, `"etag-full"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 5)
	var listPagesCalls atomic.Int32
	var storePageCalls atomic.Int32
	p.cache = &storePageCountingCache{
		cacheStore: &listPagesCountingCache{
			cacheStore: p.cache,
			calls:      &listPagesCalls,
		},
		calls: &storePageCalls,
	}
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/bucket/full.bin", nil)
		rec := httptest.NewRecorder()

		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("full GET %d status = %d, want 200; body=%q", i+1, rec.Code, rec.Body.String())
		}
		if got := rec.Body.Bytes(); !bytes.Equal(got, body) {
			t.Fatalf("full GET %d body = %q, want %q", i+1, got, body)
		}
		if got := rec.Header().Get("Content-Length"); got != "12" {
			t.Fatalf("full GET %d Content-Length = %q, want 12", i+1, got)
		}
	}

	wantRequests := []string{"HEAD ", "GET "}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
	if got := listPagesCalls.Load(); got != 0 {
		t.Fatalf("ListPages() calls = %d, want 0 on full cached path", got)
	}
	if got := storePageCalls.Load(); got != 0 {
		t.Fatalf("StorePage() calls = %d, want 0 with direct page writer", got)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_hit_duration_seconds_count{bucket="bucket"} 2`) {
		t.Fatalf("metrics missing cache serve duration:\n%s", metricsBody)
	}
}

func TestProxyKeepsPartialFullGetOnPagedPath(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		writeObjectResponse(t, w, r, body, `"etag-partial-full"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	rangeReq := httptest.NewRequest(http.MethodGet, "/bucket/partial-full.bin", nil)
	rangeReq.Header.Set("Range", "bytes=0-3")
	rangeRec := httptest.NewRecorder()
	p.ServeHTTP(rangeRec, rangeReq)
	if rangeRec.Code != http.StatusPartialContent {
		t.Fatalf("range status = %d, want 206; body=%q", rangeRec.Code, rangeRec.Body.String())
	}

	fullReq := httptest.NewRequest(http.MethodGet, "/bucket/partial-full.bin", nil)
	fullRec := httptest.NewRecorder()
	p.ServeHTTP(fullRec, fullReq)
	if fullRec.Code != http.StatusOK {
		t.Fatalf("full status = %d, want 200; body=%q", fullRec.Code, fullRec.Body.String())
	}
	if got := fullRec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("full body = %q, want %q", got, body)
	}

	wantRequests := []string{"HEAD ", "GET bytes=0-3", "GET bytes=4-7", "GET bytes=8-11"}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyDoesNotCommitHeadersWhenFirstPageFetchFails(t *testing.T) {
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag-fail"`)
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream unavailable"))
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	req := httptest.NewRequest(http.MethodGet, "/bucket/fail.bin", nil)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() == 12 {
		t.Fatalf("body length = %d, expected error response rather than promised object body", rec.Body.Len())
	}
	wantRequests := []string{"HEAD ", "GET "}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyAbortsCommittedResponseWhenLaterPageFetchFails(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		w.Header().Set("Content-Length", "12")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"etag-midstream"`)
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		switch r.Header.Get("Range") {
		case "":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("upstream unavailable"))
		case "bytes=0-3":
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Content-Range", "bytes 0-3/12")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[:4])
		case "bytes=4-7":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("upstream unavailable"))
		default:
			t.Fatalf("unexpected upstream range %q", r.Header.Get("Range"))
		}
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	warmReq := httptest.NewRequest(http.MethodGet, "/bucket/midstream.bin", nil)
	warmReq.Header.Set("Range", "bytes=0-3")
	warmRec := httptest.NewRecorder()
	p.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusPartialContent {
		t.Fatalf("warm status = %d, want 206; body=%q", warmRec.Code, warmRec.Body.String())
	}
	upstreamRequests = nil

	req := httptest.NewRequest(http.MethodGet, "/bucket/midstream.bin", nil)
	rec := &abortTrackingResponseWriter{ResponseRecorder: httptest.NewRecorder()}

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want committed 200 before mid-stream failure; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "abcd" {
		t.Fatalf("body = %q, want only first page before abort", got)
	}
	if !rec.aborted {
		t.Fatal("response was not aborted after later page fetch failed")
	}
	wantRequests := []string{"GET bytes=4-7"}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyInvalidatesAndRefetchesMetadataOnPreconditionFailure(t *testing.T) {
	ctx := context.Background()
	body := []byte("new-version")
	var upstreamRequests []string

	p := testProxyWithPageSize(t, "", 4)
	_, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      "changed.bin",
		ETag:     `"old-etag"`,
		Size:     int64(len(body)),
		PageSize: 4,
		Headers: http.Header{
			"Content-Length": []string{"11"},
			"Content-Type":   []string{"application/octet-stream"},
			"ETag":           []string{`"old-etag"`},
		},
	})
	if err != nil {
		t.Fatalf("PutObject(stale) error = %v", err)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range")+" "+r.Header.Get("If-Match"))
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "11")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("ETag", `"new-etag"`)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Header.Get("If-Match") == `"old-etag"` {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		writeObjectResponse(t, w, r, body, `"new-etag"`)
	}))
	defer upstream.Close()

	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	p.upstreamEndpoint = parsed

	req := httptest.NewRequest(http.MethodGet, "/bucket/changed.bin", nil)
	req.Header.Set("Range", "bytes=4-7")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, body[4:8]) {
		t.Fatalf("body = %q, want %q", got, body[4:8])
	}
	wantRequests := []string{
		"GET bytes=4-7 \"old-etag\"",
		"HEAD  ",
		"GET bytes=4-7 \"new-etag\"",
	}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyFallsBackWhenRefetchedObjectNoLongerSatisfiesRange(t *testing.T) {
	ctx := context.Background()
	body := []byte("short")
	var upstreamRequests []string

	p := testProxyWithPageSize(t, "", 4)
	_, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      "shrunk.bin",
		ETag:     `"old-etag"`,
		Size:     12,
		PageSize: 4,
		Headers: http.Header{
			"Content-Length": []string{"12"},
			"Content-Type":   []string{"application/octet-stream"},
			"ETag":           []string{`"old-etag"`},
		},
	})
	if err != nil {
		t.Fatalf("PutObject(stale) error = %v", err)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range")+" "+r.Header.Get("If-Match"))
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("ETag", `"new-etag"`)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Header.Get("If-Match") == `"old-etag"` {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if r.Header.Get("Range") == "bytes=8-10" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(body)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		t.Fatalf("unexpected upstream GET range=%q if-match=%q", r.Header.Get("Range"), r.Header.Get("If-Match"))
	}))
	defer upstream.Close()

	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	p.upstreamEndpoint = parsed

	req := httptest.NewRequest(http.MethodGet, "/bucket/shrunk.bin", nil)
	req.Header.Set("Range", "bytes=8-10")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes */5" {
		t.Fatalf("Content-Range = %q, want bytes */5", got)
	}
	wantRequests := []string{
		"GET bytes=8-11 \"old-etag\"",
		"HEAD  ",
		"GET bytes=8-10 ",
	}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyDoesNotServeStalePageAfterMetadataRefresh(t *testing.T) {
	ctx := context.Background()
	oldBody := []byte("old-data")
	newBody := []byte("new-data")
	var upstreamRequests []string

	p := testProxyWithPageSize(t, "", 4)
	oldObj, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      "refreshed.bin",
		ETag:     `"old-etag"`,
		Size:     int64(len(oldBody)),
		PageSize: 4,
		Headers: http.Header{
			"Content-Length": []string{strconv.Itoa(len(oldBody))},
			"Content-Type":   []string{"application/octet-stream"},
			"ETag":           []string{`"old-etag"`},
		},
	})
	if err != nil {
		t.Fatalf("PutObject(old) error = %v", err)
	}
	if _, err := p.cache.StorePage(ctx, cache.PageWrite{
		ObjectID:      oldObj.ID,
		Index:         0,
		ETag:          oldObj.ETag,
		ExpectedEpoch: oldObj.Epoch,
		Size:          4,
		Source:        bytes.NewReader(oldBody[:4]),
	}); err != nil {
		t.Fatalf("StorePage(old) error = %v", err)
	}
	if _, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      "refreshed.bin",
		ETag:     `"new-etag"`,
		Size:     int64(len(newBody)),
		PageSize: 4,
		Headers: http.Header{
			"Content-Length": []string{strconv.Itoa(len(newBody))},
			"Content-Type":   []string{"application/octet-stream"},
			"ETag":           []string{`"new-etag"`},
		},
	}); err != nil {
		t.Fatalf("PutObject(new) error = %v", err)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range")+" "+r.Header.Get("If-Match"))
		if r.Header.Get("If-Match") != `"new-etag"` {
			t.Fatalf("If-Match = %q, want new etag", r.Header.Get("If-Match"))
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/8")
		w.Header().Set("ETag", `"new-etag"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(newBody[:4])
	}))
	defer upstream.Close()
	parsed, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	p.upstreamEndpoint = parsed

	req := httptest.NewRequest(http.MethodGet, "/bucket/refreshed.bin", nil)
	req.Header.Set("Range", "bytes=0-3")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, newBody[:4]) {
		t.Fatalf("body = %q, want %q", got, newBody[:4])
	}
	wantRequests := []string{`GET bytes=0-3 "new-etag"`}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyInvalidatesCachedObjectAfterSuccessfulWrites(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		path    string
		query   string
		headers http.Header
		status  int
	}{
		{
			name:   "put object",
			method: http.MethodPut,
			path:   "/bucket/write.bin",
			status: http.StatusOK,
		},
		{
			name:   "delete object",
			method: http.MethodDelete,
			path:   "/bucket/write.bin",
			status: http.StatusNoContent,
		},
		{
			name:   "copy object destination",
			method: http.MethodPut,
			path:   "/bucket/write.bin",
			headers: http.Header{
				"X-Amz-Copy-Source": []string{"/bucket/source.bin"},
			},
			status: http.StatusOK,
		},
		{
			name:   "multipart complete",
			method: http.MethodPost,
			path:   "/bucket/write.bin",
			query:  "uploadId=upload-123",
			status: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte("abcdefghijkl")
			var upstreamRequests []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range")+" "+r.URL.RawQuery)
				if r.Method == tt.method {
					w.WriteHeader(tt.status)
					return
				}
				writeObjectResponse(t, w, r, body, `"etag-write"`)
			}))
			defer upstream.Close()

			p := testProxyWithPageSize(t, upstream.URL, 4)
			cachedReq := httptest.NewRequest(http.MethodGet, "/bucket/write.bin", nil)
			cachedReq.Header.Set("Range", "bytes=0-3")
			cachedRec := httptest.NewRecorder()
			p.ServeHTTP(cachedRec, cachedReq)
			if cachedRec.Code != http.StatusPartialContent {
				t.Fatalf("warm cache status = %d, want 206; body=%q", cachedRec.Code, cachedRec.Body.String())
			}
			if _, ok, err := p.cache.GetObject(context.Background(), "bucket", "write.bin"); err != nil {
				t.Fatalf("GetObject(warmed) error = %v", err)
			} else if !ok {
				t.Fatal("GetObject(warmed) ok = false, want true")
			}

			writeURL := tt.path
			if tt.query != "" {
				writeURL += "?" + tt.query
			}
			writeReq := httptest.NewRequest(tt.method, writeURL, bytes.NewReader([]byte("new body")))
			for key, values := range tt.headers {
				writeReq.Header[key] = append([]string(nil), values...)
			}
			writeRec := httptest.NewRecorder()
			p.ServeHTTP(writeRec, writeReq)

			if writeRec.Code != tt.status {
				t.Fatalf("write status = %d, want %d; body=%q", writeRec.Code, tt.status, writeRec.Body.String())
			}
			if _, ok, err := p.cache.GetObject(context.Background(), "bucket", "write.bin"); err != nil {
				t.Fatalf("GetObject(after write) error = %v", err)
			} else if ok {
				t.Fatal("GetObject(after write) ok = true, want false")
			}
			if len(upstreamRequests) == 0 || upstreamRequests[len(upstreamRequests)-1] != tt.method+"  "+tt.query {
				t.Fatalf("last upstream request = %q, want %q", upstreamRequests, tt.method+"  "+tt.query)
			}
		})
	}
}

func TestWriteInvalidationClassification(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		rawURL  string
		headers http.Header
		want    bool
	}{
		{
			name:   "plain put invalidates",
			method: http.MethodPut,
			rawURL: "/bucket/object.bin",
			want:   true,
		},
		{
			name:   "upload part does not invalidate before completion",
			method: http.MethodPut,
			rawURL: "/bucket/object.bin?partNumber=1&uploadId=upload-123",
			want:   false,
		},
		{
			name:   "upload part copy does not invalidate destination before completion",
			method: http.MethodPut,
			rawURL: "/bucket/object.bin?partNumber=1&uploadId=upload-123",
			headers: http.Header{
				"X-Amz-Copy-Source": []string{"/bucket/source.bin"},
			},
			want: false,
		},
		{
			name:   "copy object invalidates destination",
			method: http.MethodPut,
			rawURL: "/bucket/object.bin",
			headers: http.Header{
				"X-Amz-Copy-Source": []string{"/bucket/source.bin"},
			},
			want: true,
		},
		{
			name:   "multipart complete invalidates destination",
			method: http.MethodPost,
			rawURL: "/bucket/object.bin?uploadId=upload-123",
			want:   true,
		},
		{
			name:   "bucket operation does not invalidate",
			method: http.MethodPut,
			rawURL: "/bucket",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.rawURL, nil)
			for key, values := range tt.headers {
				req.Header[key] = append([]string(nil), values...)
			}
			target, ok := s3request.ParsePathStyle(req.URL.EscapedPath())
			if !ok {
				t.Fatalf("ParsePathStyle(%q) ok = false", req.URL.EscapedPath())
			}

			if got := shouldInvalidateAfterWrite(req, target); got != tt.want {
				t.Fatalf("shouldInvalidateAfterWrite() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProxyCopyInvalidatesDestinationOnly(t *testing.T) {
	body := []byte("abcdefghijkl")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "" {
			w.WriteHeader(http.StatusOK)
			return
		}
		writeObjectResponse(t, w, r, body, `"etag-copy"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	for _, path := range []string{"/bucket/source.bin", "/bucket/destination.bin"} {
		req := httptest.NewRequest(http.MethodHead, path, nil)
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("warm %s status = %d, want 200", path, rec.Code)
		}
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/bucket/destination.bin", nil)
	copyReq.Header.Set("X-Amz-Copy-Source", "/bucket/source.bin")
	copyRec := httptest.NewRecorder()
	p.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want 200", copyRec.Code)
	}

	if _, ok, err := p.cache.GetObject(context.Background(), "bucket", "source.bin"); err != nil {
		t.Fatalf("GetObject(source) error = %v", err)
	} else if !ok {
		t.Fatal("source cache entry was invalidated, want it preserved")
	}
	if _, ok, err := p.cache.GetObject(context.Background(), "bucket", "destination.bin"); err != nil {
		t.Fatalf("GetObject(destination) error = %v", err)
	} else if ok {
		t.Fatal("destination cache entry was preserved, want invalidated")
	}
}

func TestProxyDoesNotFailSuccessfulWriteWhenInvalidationFails(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("write ok"))
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	p.cache = invalidationFailingCache{cacheStore: p.cache}
	enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
	})
	req := httptest.NewRequest(http.MethodPut, "/bucket/object.bin", bytes.NewReader([]byte("new body")))
	rec := httptest.NewRecorder()

	target := s3request.Target{Bucket: "bucket", Key: "object.bin"}
	status, bytesWritten, err := p.handle(rec, req, target, s3request.Classification{
		Disposition: s3request.PassThrough,
	}, newRequestStats(req, target))
	if err != nil {
		t.Fatalf("handle() error = %v, want nil because upstream write already succeeded", err)
	}
	if status != http.StatusOK || rec.Code != http.StatusOK {
		t.Fatalf("status = %d recorder = %d, want 200", status, rec.Code)
	}
	if bytesWritten == 0 {
		t.Fatal("bytesWritten = 0, want upstream response bytes copied")
	}
	if ready, reason := p.Readiness(); ready || reason == "" {
		t.Fatalf("Readiness() = (%v, %q), want degraded reason", ready, reason)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_degraded{reason_code="write_invalidation_failed"} 1`) {
		t.Fatalf("metrics missing degraded reason:\n%s", metricsBody)
	}
	if !strings.Contains(metricsBody, `simple_s3_cache_invalidation_broadcasts_total{bucket="bucket",peer_id="cache-0",status="failure"} 1`) {
		t.Fatalf("metrics missing local invalidation failure:\n%s", metricsBody)
	}
}

func TestProxyBroadcastsInvalidationAfterSuccessfulWrite(t *testing.T) {
	ctx := context.Background()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("upstream method = %q, want PUT", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("write ok"))
	}))
	defer upstream.Close()

	owner := testProxyWithPageSize(t, upstream.URL, 4)
	ownerServer := httptest.NewServer(owner)
	defer ownerServer.Close()

	peers := []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: ownerServer.URL},
	}
	ownerRouter, err := peerrouter.NewRouter("cache-1", peers)
	if err != nil {
		t.Fatalf("NewRouter(owner) error = %v", err)
	}
	owner.peerRouter = ownerRouter
	owner.peerAuthSecret = "test-peer-secret"

	key := "write-broadcast.bin"
	oldObj, err := owner.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      key,
		ETag:     `"old"`,
		Size:     4,
		PageSize: 4,
		Headers:  http.Header{"Content-Type": []string{"application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("owner PutObject() error = %v", err)
	}
	if _, err := owner.cache.StorePage(ctx, cache.PageWrite{
		ObjectID:      oldObj.ID,
		Index:         0,
		ETag:          oldObj.ETag,
		ExpectedEpoch: oldObj.Epoch,
		Size:          int64(len("old!")),
		Source:        strings.NewReader("old!"),
	}); err != nil {
		t.Fatalf("owner StorePage() error = %v", err)
	}

	coordinator := testProxyWithPageSize(t, upstream.URL, 4)
	enablePeerMode(t, coordinator, "cache-0", peers)
	req := httptest.NewRequest(http.MethodPut, "/bucket/"+key, strings.NewReader("new!"))
	rec := httptest.NewRecorder()

	coordinator.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if _, ok, err := owner.cache.GetObject(ctx, "bucket", key); err != nil {
		t.Fatalf("owner GetObject() error = %v", err)
	} else if ok {
		t.Fatal("owner object still cached after invalidation")
	}
	if body, ok, err := owner.cache.OpenPage(ctx, oldObj.ID, 0, oldObj.ETag, oldObj.Epoch); err != nil {
		t.Fatalf("owner OpenPage() error = %v", err)
	} else if ok {
		_ = body.Close()
		t.Fatal("owner stale page still opens after invalidation")
	}
	refreshed, err := owner.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      key,
		ETag:     `"new"`,
		Size:     4,
		PageSize: 4,
		Headers:  http.Header{"Content-Type": []string{"application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("owner PutObject(new) error = %v", err)
	}
	if refreshed.Epoch <= oldObj.Epoch {
		t.Fatalf("refreshed epoch = %d, want greater than old epoch %d", refreshed.Epoch, oldObj.Epoch)
	}
	metricsBody := renderProxyMetrics(t, owner.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_invalidation_broadcasts_total{bucket="bucket",peer_id="cache-1",status="success"} 1`) {
		t.Fatalf("metrics missing owner local invalidation success:\n%s", metricsBody)
	}
}

func TestProxyMarksNotReadyWhenPeerInvalidationFails(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("write ok"))
	}))
	defer upstream.Close()

	failingPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/invalidate" {
			t.Fatalf("peer path = %q, want /internal/v1/invalidate", r.URL.Path)
		}
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer failingPeer.Close()

	p := testProxy(t, upstream.URL)
	enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: failingPeer.URL},
	})
	req := httptest.NewRequest(http.MethodPut, "/bucket/object.bin", strings.NewReader("new body"))
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want upstream 200; body=%q", rec.Code, rec.Body.String())
	}
	if ready, reason := p.Readiness(); ready || !strings.Contains(reason, "peer invalidation") {
		t.Fatalf("Readiness() = (%v, %q), want peer invalidation degradation", ready, reason)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_invalidation_broadcasts_total{bucket="bucket",peer_id="cache-1",status="failure"} 1`) {
		t.Fatalf("metrics missing failed invalidation broadcast:\n%s", metricsBody)
	}
}

func TestProxyInternalRoutesRequirePeerSignature(t *testing.T) {
	p := testProxy(t, "http://upstream.invalid")
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	body := []byte(`{"bucket":"bucket","key":"object.bin","epoch":1}`)

	unsigned := httptest.NewRequest(http.MethodPost, "/internal/v1/invalidate", bytes.NewReader(body))
	unsigned.Header.Set(peerFromHeader, "cache-1")
	unsigned.Header.Set(peerRingHeader, router.RingID())
	unsigned.Header.Set("Content-Type", "application/json")
	unsignedRec := httptest.NewRecorder()
	p.ServeHTTP(unsignedRec, unsigned)
	if unsignedRec.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned status = %d, want 401; body=%q", unsignedRec.Code, unsignedRec.Body.String())
	}

	badSignature := httptest.NewRequest(http.MethodPost, "/internal/v1/invalidate", bytes.NewReader(body))
	badSignature.Header.Set(peerFromHeader, "cache-1")
	badSignature.Header.Set(peerRingHeader, router.RingID())
	badSignature.Header.Set(peerTimestampHeader, "1700000000")
	badSignature.Header.Set(peerSignatureHeader, "not-a-valid-signature")
	badSignature.Header.Set("Content-Type", "application/json")
	badRec := httptest.NewRecorder()
	p.ServeHTTP(badRec, badSignature)
	if badRec.Code != http.StatusUnauthorized {
		t.Fatalf("bad signature status = %d, want 401; body=%q", badRec.Code, badRec.Body.String())
	}

	signed := httptest.NewRequest(http.MethodPost, "/internal/v1/invalidate", bytes.NewReader(body))
	signed.Header.Set(peerFromHeader, "cache-1")
	signed.Header.Set(peerRingHeader, router.RingID())
	signed.Header.Set("Content-Type", "application/json")
	signTestInternalPeerRequest(t, signed, body, p.peerAuthSecret)
	signedRec := httptest.NewRecorder()
	p.ServeHTTP(signedRec, signed)
	if signedRec.Code != http.StatusOK {
		t.Fatalf("signed status = %d, want 200; body=%q", signedRec.Code, signedRec.Body.String())
	}
}

func TestProxyBroadcastsInvalidationToAllPeersAfterFailure(t *testing.T) {
	var successfulInvalidations int
	successfulPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/invalidate" {
			t.Fatalf("successful peer path = %q, want /internal/v1/invalidate", r.URL.Path)
		}
		successfulInvalidations++
		w.WriteHeader(http.StatusOK)
	}))
	defer successfulPeer.Close()

	failingPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/invalidate" {
			t.Fatalf("failing peer path = %q, want /internal/v1/invalidate", r.URL.Path)
		}
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer failingPeer.Close()

	p := testProxy(t, "http://upstream.invalid")
	enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: failingPeer.URL},
		{ID: "cache-2", URL: successfulPeer.URL},
	})

	_, err := p.invalidateObject(context.Background(), s3request.Target{Bucket: "bucket", Key: "object.bin"})

	if err == nil {
		t.Fatal("invalidateObject() error = nil, want failed peer error")
	}
	if successfulInvalidations != 1 {
		t.Fatalf("successful peer invalidations = %d, want 1", successfulInvalidations)
	}
	if ready, reason := p.Readiness(); ready || !strings.Contains(reason, "peer invalidation") {
		t.Fatalf("Readiness() = (%v, %q), want peer invalidation degradation", ready, reason)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	for _, want := range []string{
		`simple_s3_cache_invalidation_broadcasts_total{bucket="bucket",peer_id="cache-0",status="success"} 1`,
		`simple_s3_cache_invalidation_broadcasts_total{bucket="bucket",peer_id="cache-1",status="failure"} 1`,
		`simple_s3_cache_invalidation_broadcasts_total{bucket="bucket",peer_id="cache-2",status="success"} 1`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metricsBody)
		}
	}
}

func TestProxyDoesNotInvalidateCachedObjectAfterFailedWrite(t *testing.T) {
	body := []byte("abcdefghijkl")
	var headRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Method == http.MethodHead {
			headRequests++
		}
		writeObjectResponse(t, w, r, body, `"etag-failed-write"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	warmReq := httptest.NewRequest(http.MethodHead, "/bucket/failed-write.bin", nil)
	warmRec := httptest.NewRecorder()
	p.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusOK {
		t.Fatalf("warm HEAD status = %d, want 200", warmRec.Code)
	}

	writeReq := httptest.NewRequest(http.MethodPut, "/bucket/failed-write.bin", bytes.NewReader([]byte("new body")))
	writeRec := httptest.NewRecorder()
	p.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusInternalServerError {
		t.Fatalf("write status = %d, want 500", writeRec.Code)
	}

	cachedReq := httptest.NewRequest(http.MethodHead, "/bucket/failed-write.bin", nil)
	cachedRec := httptest.NewRecorder()
	p.ServeHTTP(cachedRec, cachedReq)
	if cachedRec.Code != http.StatusOK {
		t.Fatalf("cached HEAD status = %d, want 200", cachedRec.Code)
	}
	if headRequests != 1 {
		t.Fatalf("upstream HEAD requests = %d, want 1; failed write should not invalidate metadata", headRequests)
	}
}

func TestProxyFallsBackToPassThroughWhenMetadataStoreFails(t *testing.T) {
	body := []byte("metadata store failure still reads")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", `"etag-pass-through"`)
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.cache = metadataStoreFailingCache{cacheStore: p.cache}
	req := httptest.NewRequest(http.MethodGet, "/bucket/object.txt", nil)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
	wantRequests := []string{"HEAD ", "GET "}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyPeerModeForwardsRemoteOwnerRequest(t *testing.T) {
	var peerRequests int
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerRequests++
		if got := r.URL.EscapedPath(); !strings.HasPrefix(got, internalObjectRoutePrefix+"/bucket/") {
			t.Fatalf("peer path = %q, want internal object route", got)
		}
		if got := r.Header.Get(peerForwardedHeader); got != "1" {
			t.Fatalf("peer forwarded header = %q, want 1", got)
		}
		if got := r.Header.Get(peerOwnerHeader); got != "cache-1" {
			t.Fatalf("peer owner header = %q, want cache-1", got)
		}
		if got := r.Header.Get(peerFromHeader); got != "cache-0" {
			t.Fatalf("peer from header = %q, want cache-0", got)
		}
		if got := r.Header.Get(peerRingHeader); got == "" {
			t.Fatal("peer ring header is empty")
		}
		if got := r.Header.Get("Authorization"); got != "client-signature" {
			t.Fatalf("Authorization = %q, want client-signature", got)
		}
		w.Header().Set("ETag", `"peer-etag"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("from-peer"))
	}))
	defer peer.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD metadata request only", r.Method)
		}
		w.Header().Set("Content-Length", "9")
		w.Header().Set("ETag", `"peer-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer.URL},
	})
	key := keyOwnedBy(t, router, "bucket", "cache-1")
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	req.Header.Set("Authorization", "client-signature")
	req.Header.Set(peerForwardedHeader, "spoofed")
	req.Header.Set(peerOwnerHeader, "cache-0")
	req.Header.Set(peerFromHeader, "client")
	req.Header.Set(peerRingHeader, "spoofed-ring")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "from-peer" {
		t.Fatalf("body = %q, want from-peer", got)
	}
	if peerRequests != 1 {
		t.Fatalf("peer requests = %d, want 1", peerRequests)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if strings.Contains(metricsBody, "simple_s3_cache_peer_forward") {
		t.Fatalf("metrics include pruned peer forwarding series:\n%s", metricsBody)
	}
}

func TestProxyPeerModeHandlesLocalOwnerRequestLocally(t *testing.T) {
	var invalidations int
	peer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		invalidations++
	}))
	defer peer.Close()

	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		if r.Method != http.MethodPut {
			t.Fatalf("upstream method = %q, want PUT", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer.URL},
	})
	key := keyOwnedBy(t, router, "bucket", "cache-0")
	req := httptest.NewRequest(http.MethodPut, "/bucket/"+key, strings.NewReader("body"))
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if upstreamRequests != 1 {
		t.Fatalf("upstream requests = %d, want 1", upstreamRequests)
	}
	if invalidations != 1 {
		t.Fatalf("peer invalidations = %d, want 1", invalidations)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_peer_ring_info{mode="peer",local_id="cache-0",ring_id="`) {
		t.Fatalf("metrics missing peer ring info:\n%s", metricsBody)
	}
}

func TestProxyPeerModeForwardedWriteInvalidatesOnOwner(t *testing.T) {
	var upstreamDeletes int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("upstream method = %q, want DELETE", r.Method)
		}
		if got := r.Header.Get(peerForwardedHeader); got != "" {
			t.Fatalf("upstream peer forwarded header = %q, want empty", got)
		}
		if got := r.Header.Get(peerOwnerHeader); got != "" {
			t.Fatalf("upstream peer owner header = %q, want empty", got)
		}
		if got := r.Header.Get(peerFromHeader); got != "" {
			t.Fatalf("upstream peer from header = %q, want empty", got)
		}
		upstreamDeletes++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	owner := testProxy(t, upstream.URL)
	ownerServer := httptest.NewServer(owner)
	defer ownerServer.Close()

	router, err := peerrouter.NewRouter("cache-1", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: ownerServer.URL},
	})
	if err != nil {
		t.Fatalf("NewRouter(owner) error = %v", err)
	}
	key := keyOwnedBy(t, router, "bucket", "cache-1")
	if _, err := owner.cache.PutObject(context.Background(), cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      key,
		ETag:     `"etag"`,
		Size:     12,
		PageSize: 4,
		Headers:  http.Header{"ETag": []string{`"etag"`}},
	}); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	owner.peerRouter = router
	owner.peerAuthSecret = "test-peer-secret"
	if owner.metrics != nil {
		owner.metrics.SetPeerRingInfo("peer", router.LocalID(), router.RingID())
	}

	receiver := testProxy(t, upstream.URL)
	enablePeerMode(t, receiver, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: ownerServer.URL},
	})
	req := httptest.NewRequest(http.MethodDelete, "/bucket/"+key, nil)
	rec := httptest.NewRecorder()

	receiver.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%q", rec.Code, rec.Body.String())
	}
	if upstreamDeletes != 1 {
		t.Fatalf("upstream deletes = %d, want 1", upstreamDeletes)
	}
	if _, ok, err := owner.cache.GetObject(context.Background(), "bucket", key); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if ok {
		t.Fatal("object metadata still cached on owner after forwarded DELETE")
	}
}

func TestInternalObjectRequestPreservesEscapedS3Path(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, internalObjectRoutePrefix+"/bucket/path%20with%20spaces/plus+and%2525.txt?versionId=123", nil)

	got := internalObjectRequest(req)

	if got.URL.EscapedPath() != "/bucket/path%20with%20spaces/plus+and%2525.txt" {
		t.Fatalf("EscapedPath() = %q, want original escaped S3 path", got.URL.EscapedPath())
	}
	target, ok := s3request.ParsePathStyle(got.URL.EscapedPath())
	if !ok {
		t.Fatal("ParsePathStyle() ok = false")
	}
	if target.Bucket != "bucket" || target.Key != "path with spaces/plus+and%25.txt" {
		t.Fatalf("target = %+v, want decoded bucket/key", target)
	}
}

func TestProxyInternalRoutesRequirePeerHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive invalid internal request")
	}))
	defer upstream.Close()

	tests := []struct {
		name       string
		headers    http.Header
		wantStatus int
	}{
		{
			name: "missing peer identity",
			headers: http.Header{
				peerForwardedHeader: []string{"1"},
				peerOwnerHeader:     []string{"cache-0"},
				peerRingHeader:      []string{"valid"},
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "unknown peer identity",
			headers: http.Header{
				peerForwardedHeader: []string{"1"},
				peerOwnerHeader:     []string{"cache-0"},
				peerFromHeader:      []string{"cache-9"},
				peerRingHeader:      []string{"valid"},
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "missing ring",
			headers: http.Header{
				peerForwardedHeader: []string{"1"},
				peerOwnerHeader:     []string{"cache-0"},
				peerFromHeader:      []string{"cache-1"},
			},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "mismatched ring",
			headers: http.Header{
				peerForwardedHeader: []string{"1"},
				peerOwnerHeader:     []string{"cache-0"},
				peerFromHeader:      []string{"cache-1"},
				peerRingHeader:      []string{"different-ring"},
			},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "missing forwarded header",
			headers: http.Header{
				peerOwnerHeader: []string{"cache-0"},
				peerFromHeader:  []string{"cache-1"},
				peerRingHeader:  []string{"valid"},
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing owner header",
			headers: http.Header{
				peerForwardedHeader: []string{"1"},
				peerFromHeader:      []string{"cache-1"},
				peerRingHeader:      []string{"valid"},
			},
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := testProxy(t, upstream.URL)
			router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
				{ID: "cache-0", URL: "http://cache-0.invalid"},
				{ID: "cache-1", URL: "http://cache-1.invalid"},
			})
			var cacheTouches atomic.Int32
			p.cache = &cacheTouchCountingCache{cacheStore: p.cache, calls: &cacheTouches}
			key := keyOwnedBy(t, router, "bucket", "cache-0")
			req := httptest.NewRequest(http.MethodGet, internalObjectRoutePrefix+"/bucket/"+key, nil)
			for header, values := range tt.headers {
				for _, value := range values {
					if value == "valid" {
						value = router.RingID()
					}
					req.Header.Add(header, value)
				}
			}
			rec := httptest.NewRecorder()

			p.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if got := cacheTouches.Load(); got != 0 {
				t.Fatalf("cache touches = %d, want 0 before internal request validation succeeds", got)
			}
		})
	}
}

func TestProxyInternalPageReadServesCachedPages(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive cached internal page read")
	}))
	defer upstream.Close()

	ctx := context.Background()
	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-0", "cache-0"})
	obj, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      key,
		ETag:     `"etag-cached"`,
		Size:     8,
		PageSize: 4,
		Headers:  http.Header{"ETag": []string{`"etag-cached"`}},
	})
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	for index, data := range [][]byte{[]byte("abcd"), []byte("efgh")} {
		if _, err := p.cache.StorePage(ctx, cache.PageWrite{
			ObjectID:      obj.ID,
			Index:         int64(index),
			ETag:          obj.ETag,
			ExpectedEpoch: obj.Epoch,
			Size:          int64(len(data)),
			Source:        bytes.NewReader(data),
		}); err != nil {
			t.Fatalf("StorePage(%d) error = %v", index, err)
		}
	}

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 8,
		PageSize:   4,
		ETag:       `"etag-cached"`,
		Epoch:      obj.Epoch,
		Pages:      []int64{0, 1},
	}))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != peerrouter.PageFrameContentType {
		t.Fatalf("Content-Type = %q, want %q", got, peerrouter.PageFrameContentType)
	}
	assertPageFrames(t, rec.Body, []int64{0, 1}, [][]byte{[]byte("abcd"), []byte("efgh")}, 4)
	metricsBody := renderProxyMetrics(t, p.metrics)
	for _, want := range []string{
		`simple_s3_cache_page_owner_requests_total{bucket="bucket",owner_id="cache-0",status_class="2xx"} 1`,
		`simple_s3_cache_page_owner_bytes_served_total{bucket="bucket",owner_id="cache-0"} 8`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metricsBody)
		}
	}
}

func TestProxyInternalPageReadRejectsWrongPageOwner(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive wrong-owner internal page read")
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1"})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 4,
		PageSize:   4,
		ETag:       `"etag-wrong-owner"`,
		Epoch:      1,
		Pages:      []int64{0},
	}))

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%q", rec.Code, rec.Body.String())
	}
}

func TestProxyInternalPageReadFillsMissingPage(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	key := ""
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.URL.EscapedPath()+" "+r.Header.Get("Range")+" "+r.Header.Get("If-Match"))
		if r.Method != http.MethodGet {
			t.Fatalf("upstream method = %q, want GET", r.Method)
		}
		if got := r.Header.Get(peerFromHeader); got != "" {
			t.Fatalf("upstream peer header = %q, want empty", got)
		}
		if got := r.Header.Get("If-Match"); got != `"etag-fill"` {
			t.Fatalf("If-Match = %q, want request ETag", got)
		}
		if got := r.Header.Get("Range"); got != "bytes=4-7" {
			t.Fatalf("Range = %q, want page-aligned range", got)
		}
		assertRequestSignatureMatchesFinalHeaders(t, r)
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 4-7/12")
		w.Header().Set("ETag", `"etag-fill"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[4:8])
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key = keyWithPageOwnerAt(t, router, "bucket", 1, "cache-0")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 12,
		PageSize:   4,
		ETag:       `"etag-fill"`,
		Epoch:      0,
		Pages:      []int64{1},
	}))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	assertPageFrames(t, rec.Body, []int64{1}, [][]byte{body[4:8]}, 4)
	wantRequests := []string{`GET /bucket/` + escapeS3KeyPath(key) + ` bytes=4-7 "etag-fill"`}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	for _, want := range []string{
		`simple_s3_cache_page_owner_requests_total{bucket="bucket",owner_id="cache-0",status_class="2xx"} 1`,
		`simple_s3_cache_page_owner_bytes_served_total{bucket="bucket",owner_id="cache-0"} 4`,
		`simple_s3_cache_page_owner_upstream_fill_bytes_total{bucket="bucket",owner_id="cache-0"} 4`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metricsBody)
		}
	}
}

func TestProxyInternalPageReadStreamsFirstPageBeforeLoadingFullBatch(t *testing.T) {
	body := []byte("abcdefgh")
	pageOneStarted := make(chan struct{})
	releasePageOne := make(chan struct{})
	var signalOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Range") {
		case "bytes=0-3":
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Content-Range", "bytes 0-3/8")
			w.Header().Set("ETag", `"etag-stream-owner"`)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[:4])
		case "bytes=4-7":
			signalOnce.Do(func() { close(pageOneStarted) })
			<-releasePageOne
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Content-Range", "bytes 4-7/8")
			w.Header().Set("ETag", `"etag-stream-owner"`)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[4:])
		default:
			t.Fatalf("Range = %q", r.Header.Get("Range"))
		}
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-0", "cache-0"})
	rec := newThreadSafeRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
			Bucket:     "bucket",
			Key:        key,
			ObjectSize: int64(len(body)),
			PageSize:   4,
			ETag:       `"etag-stream-owner"`,
			Epoch:      0,
			Pages:      []int64{0, 1},
		}))
	}()

	select {
	case <-pageOneStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second page fill to start")
	}
	if !bytes.Contains(rec.bodyBytes(), body[:4]) {
		close(releasePageOne)
		t.Fatal("first page was not streamed before second page finished loading")
	}
	close(releasePageOne)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for internal page read to finish")
	}
	assertPageFrames(t, bytes.NewReader(rec.bodyBytes()), []int64{0, 1}, [][]byte{body[:4], body[4:]}, 4)
}

func TestProxyDistributedFallbackClearsLocalPlanningState(t *testing.T) {
	body := []byte("abcdefghijkl")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("upstream method = %q, want GET fallback", r.Method)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"etag-fallback"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://127.0.0.1:1"},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1", "cache-1"})
	if _, err := p.cache.PutObject(context.Background(), cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      key,
		ETag:     `"etag-fallback"`,
		Size:     int64(len(body)),
		PageSize: 4,
		Headers:  http.Header{"ETag": []string{`"etag-fallback"`}, "Content-Length": []string{strconv.Itoa(len(body))}},
	}); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 fallback; body=%q", rec.Code, rec.Body.String())
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Fatalf("body = %q, want %q", rec.Body.Bytes(), body)
	}
	if _, ok, err := p.cache.GetObject(context.Background(), "bucket", key); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if ok {
		t.Fatal("local planning metadata still cached after distributed fallback")
	}
}

func TestNewPeerModePreservesLocalCacheOnStartup(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cachePath := filepath.Join(root, "cache-bytes")
	metaPath := filepath.Join(root, "cache-meta")
	existing, err := cache.Open(ctx, cache.Options{
		CachePath: cachePath,
		MetaPath:  metaPath,
	})
	if err != nil {
		t.Fatalf("open existing cache: %v", err)
	}
	obj, err := existing.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      "stale-after-downtime.bin",
		ETag:     `"old"`,
		Size:     4,
		PageSize: 4,
		Headers:  http.Header{"ETag": []string{`"old"`}},
	})
	if err != nil {
		t.Fatalf("PutObject(existing) error = %v", err)
	}
	if _, err := existing.StorePage(ctx, cache.PageWrite{
		ObjectID:      obj.ID,
		Index:         0,
		ETag:          obj.ETag,
		ExpectedEpoch: obj.Epoch,
		Size:          4,
		Source:        strings.NewReader("old!"),
	}); err != nil {
		t.Fatalf("StorePage(existing) error = %v", err)
	}
	if err := existing.Close(); err != nil {
		t.Fatalf("close existing cache: %v", err)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	cfg := appconfig.Default()
	cfg.Upstream.Endpoint = upstream.URL
	cfg.Upstream.AccessKey = "test-access-key"
	cfg.Upstream.SecretKey = "test-secret-key"
	cfg.Cache.CachePath = cachePath
	cfg.Cache.MetaPath = metaPath
	cfg.Peer.Mode = "peer"
	cfg.Peer.LocalID = "cache-0"
	cfg.Peer.AuthSecret = "test-peer-secret"
	cfg.Peer.Peers = []appconfig.Peer{{ID: "cache-0", URL: "http://cache-0.invalid"}}

	p, err := New(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer p.Close()

	if _, ok, err := p.cache.GetObject(ctx, "bucket", "stale-after-downtime.bin"); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if !ok {
		t.Fatal("peer-mode startup lost existing local cache metadata")
	}
}

func TestProxyInternalPageReadEstablishesMissingMetadataForNonZeroEpoch(t *testing.T) {
	ctx := context.Background()
	body := []byte("abcdefghijkl")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("If-Match"); got != `"etag-epoch"` {
			t.Fatalf("If-Match = %q, want request ETag", got)
		}
		if got := r.Header.Get("Range"); got != "bytes=8-11" {
			t.Fatalf("Range = %q, want page-aligned range", got)
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 8-11/12")
		w.Header().Set("ETag", `"etag-epoch"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[8:12])
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwnerAt(t, router, "bucket", 2, "cache-0")

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 12,
		PageSize:   4,
		ETag:       `"etag-epoch"`,
		Epoch:      7,
		Pages:      []int64{2},
	}))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	assertPageFrames(t, rec.Body, []int64{2}, [][]byte{body[8:12]}, 4)
	obj, ok, err := p.cache.GetObject(ctx, "bucket", key)
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	if !ok {
		t.Fatal("GetObject() ok = false, want metadata established")
	}
	if obj.Epoch != 7 || obj.ETag != `"etag-epoch"` || obj.Size != 12 || obj.PageSize != 4 {
		t.Fatalf("object metadata = %#v, want coordinator identity", obj)
	}
	page, ok, err := p.cache.OpenPage(ctx, obj.ID, 2, obj.ETag, obj.Epoch)
	if err != nil {
		t.Fatalf("OpenPage() error = %v", err)
	}
	if !ok {
		t.Fatal("OpenPage() ok = false, want filled page")
	}
	_ = page.Close()
}

func TestProxyClientHeadRefreshesInternalPageReadIdentityMetadata(t *testing.T) {
	body := []byte("abcdefghijkl")
	var headRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Content-Range", "bytes 0-3/12")
			w.Header().Set("ETag", `"etag-internal-identity"`)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[:4])
		case http.MethodHead:
			headRequests++
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Cache-Control", "max-age=60")
			w.Header().Set("X-Amz-Meta-Test", "metadata")
			w.Header().Set("ETag", `"etag-internal-identity"`)
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("upstream method = %q", r.Method)
		}
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwnerAt(t, router, "bucket", 0, "cache-0")
	internalRec := httptest.NewRecorder()
	p.ServeHTTP(internalRec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: int64(len(body)),
		PageSize:   4,
		ETag:       `"etag-internal-identity"`,
		Epoch:      3,
		Pages:      []int64{0},
	}))
	if internalRec.Code != http.StatusOK {
		t.Fatalf("internal status = %d, want 200; body=%q", internalRec.Code, internalRec.Body.String())
	}

	headReq := httptest.NewRequest(http.MethodHead, "/bucket/"+key, nil)
	headRec := httptest.NewRecorder()
	p.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d, want 200; body=%q", headRec.Code, headRec.Body.String())
	}
	if headRequests != 1 {
		t.Fatalf("upstream HEAD requests = %d, want 1", headRequests)
	}
	if got := headRec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want upstream metadata", got)
	}
	if got := headRec.Header().Get("X-Amz-Meta-Test"); got != "metadata" {
		t.Fatalf("X-Amz-Meta-Test = %q, want upstream metadata", got)
	}
}

func TestProxyInternalPageReadServesBytesWhenCacheWriteFails(t *testing.T) {
	body := []byte("abcdefghijkl")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/12")
		w.Header().Set("ETag", `"etag-write-fail"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[:4])
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.cache = beginPageWriteFailingCache{cacheStore: p.cache}
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwnerAt(t, router, "bucket", 0, "cache-0")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 12,
		PageSize:   4,
		ETag:       `"etag-write-fail"`,
		Epoch:      0,
		Pages:      []int64{0},
	}))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	assertPageFrames(t, rec.Body, []int64{0}, [][]byte{body[:4]}, 4)
}

func TestProxyInternalPageReadInvalidatesOnUpstreamPreconditionFailure(t *testing.T) {
	ctx := context.Background()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwnerAt(t, router, "bucket", 0, "cache-0")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 12,
		PageSize:   4,
		ETag:       `"stale-etag"`,
		Epoch:      0,
		Pages:      []int64{0},
	}))

	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412; body=%q", rec.Code, rec.Body.String())
	}
	if _, ok, err := p.cache.GetObject(ctx, "bucket", key); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if ok {
		t.Fatal("object metadata remains cached after upstream precondition failure")
	}
}

func TestProxyInternalPageReadTreatsStaleLocalPageAsMiss(t *testing.T) {
	ctx := context.Background()
	oldBody := []byte("old-data")
	newBody := []byte("new-data")
	var upstreamRequests []string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range")+" "+r.Header.Get("If-Match"))
		if r.Header.Get("If-Match") != `"new-etag"` {
			t.Fatalf("If-Match = %q, want new etag", r.Header.Get("If-Match"))
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/8")
		w.Header().Set("ETag", `"new-etag"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(newBody[:4])
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwnerAt(t, router, "bucket", 0, "cache-0")
	oldObj, err := p.cache.PutObject(ctx, cache.ObjectMetadata{
		Bucket:   "bucket",
		Key:      key,
		ETag:     `"old-etag"`,
		Size:     int64(len(oldBody)),
		PageSize: 4,
		Headers:  http.Header{"ETag": []string{`"old-etag"`}},
	})
	if err != nil {
		t.Fatalf("PutObject(old) error = %v", err)
	}
	if _, err := p.cache.StorePage(ctx, cache.PageWrite{
		ObjectID:      oldObj.ID,
		Index:         0,
		ETag:          oldObj.ETag,
		ExpectedEpoch: oldObj.Epoch,
		Size:          4,
		Source:        bytes.NewReader(oldBody[:4]),
	}); err != nil {
		t.Fatalf("StorePage(old) error = %v", err)
	}

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: int64(len(newBody)),
		PageSize:   4,
		ETag:       `"new-etag"`,
		Epoch:      oldObj.Epoch,
		Pages:      []int64{0},
	}))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	assertPageFrames(t, rec.Body, []int64{0}, [][]byte{newBody[:4]}, 4)
	wantRequests := []string{`GET bytes=0-3 "new-etag"`}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
	}
}

func TestProxyInternalPageReadCoalescesConcurrentMisses(t *testing.T) {
	body := []byte("abcdefghijkl")
	firstFillStarted := make(chan struct{})
	releaseFill := make(chan struct{})
	var fillRequests atomic.Int32
	var firstFill atomic.Bool
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseFill)
		})
	}
	defer release()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fillRequests.Add(1)
		if firstFill.CompareAndSwap(false, true) {
			close(firstFillStarted)
		}
		<-releaseFill
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/12")
		w.Header().Set("ETag", `"etag-coalesce-peer"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[:4])
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwnerAt(t, router, "bucket", 0, "cache-0")
	requestBody := internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 12,
		PageSize:   4,
		ETag:       `"etag-coalesce-peer"`,
		Epoch:      0,
		Pages:      []int64{0},
	}

	firstDone := make(chan error, 1)
	go func() {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, requestBody))
		firstDone <- assertInternalPageReadResponse(rec, []int64{0}, [][]byte{body[:4]}, 4)
	}()

	select {
	case <-firstFillStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first upstream fill")
	}

	secondDone := make(chan error, 1)
	go func() {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, requestBody))
		secondDone <- assertInternalPageReadResponse(rec, []int64{0}, [][]byte{body[:4]}, 4)
	}()

	time.Sleep(50 * time.Millisecond)
	if got := fillRequests.Load(); got != 1 {
		t.Fatalf("upstream fill requests while first fill is blocked = %d, want 1", got)
	}
	release()
	for i, done := range []chan error{firstDone, secondDone} {
		if err := <-done; err != nil {
			t.Fatalf("internal page read %d failed: %v", i+1, err)
		}
	}
	if got := fillRequests.Load(); got != 1 {
		t.Fatalf("upstream fill requests = %d, want 1", got)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_fill_coalesced_total{bucket="bucket",result="hit"} 1`) {
		t.Fatalf("metrics missing coalesced fill hit:\n%s", metricsBody)
	}
}

func TestProxyInternalPageReadReturnsBadGatewayOnUpstreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("upstream failed"))
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyWithPageOwnerAt(t, router, "bucket", 0, "cache-0")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, internalPageReadHTTPRequest(t, router, internalPageReadRequest{
		Bucket:     "bucket",
		Key:        key,
		ObjectSize: 12,
		PageSize:   4,
		ETag:       `"etag-failure"`,
		Epoch:      0,
		Pages:      []int64{0},
	}))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
}

func TestProxyCoordinatorPageReadServesSingleRemotePage(t *testing.T) {
	body := []byte("abcd")
	var peerRequests []internalPageReadRequest
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(peerFromHeader); got != "cache-0" {
			t.Fatalf("peer from header = %q, want cache-0", got)
		}
		if got := r.Header.Get(peerRingHeader); got == "" || got == "spoofed-ring" {
			t.Fatalf("peer ring header = %q, want coordinator ring", got)
		}
		if got := r.Header.Get(peerForwardedHeader); got != "" {
			t.Fatalf("peer forwarded header = %q, want empty", got)
		}
		if got := r.Header.Get(peerOwnerHeader); got != "" {
			t.Fatalf("peer owner header = %q, want empty", got)
		}
		peerRequests = append(peerRequests, decodeInternalPageReadRequest(t, r))
		w.Header().Set("Content-Type", peerrouter.PageFrameContentType)
		w.WriteHeader(http.StatusOK)
		writer, err := peerrouter.NewPageFrameWriter(w)
		if err != nil {
			t.Fatalf("NewPageFrameWriter() error = %v", err)
		}
		if err := writer.WritePage(0, body); err != nil {
			t.Fatalf("WritePage() error = %v", err)
		}
		if err := writer.WriteEnd(); err != nil {
			t.Fatalf("WriteEnd() error = %v", err)
		}
	}))
	defer peer.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD only", r.Method)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"etag-single-page"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.readSharding = readShardingPage
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer.URL},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1"})
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	req.Header.Set(peerFromHeader, "client")
	req.Header.Set(peerRingHeader, "spoofed-ring")
	req.Header.Set(peerForwardedHeader, "spoofed")
	req.Header.Set(peerOwnerHeader, "cache-9")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
	if len(peerRequests) != 1 {
		t.Fatalf("peer requests = %d, want 1", len(peerRequests))
	}
	if got := peerRequests[0].Pages; !equalInt64Slices(got, []int64{0}) {
		t.Fatalf("peer request pages = %v, want [0]", got)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	for _, want := range []string{
		`simple_s3_cache_coordinator_requests_total{bucket="bucket",method="GET",strategy="page",status_class="2xx"} 1`,
		`simple_s3_cache_internal_peer_requests_per_client_request_sum{bucket="bucket",strategy="page"} 1`,
		`simple_s3_cache_page_batch_size_sum{bucket="bucket",owner_id="cache-1"} 1`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metricsBody)
		}
	}
}

func TestProxyCoordinatorPageReadKeepsRemoteStreamContextAlive(t *testing.T) {
	body := []byte("abcd")
	headerReady := make(chan struct{})
	releasePage := make(chan struct{})
	peerErr := make(chan error, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = decodeInternalPageReadRequest(t, r)
		w.Header().Set("Content-Type", peerrouter.PageFrameContentType)
		w.WriteHeader(http.StatusOK)
		writer, err := peerrouter.NewPageFrameWriter(w)
		if err != nil {
			peerErr <- fmt.Errorf("NewPageFrameWriter: %w", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(headerReady)
		select {
		case <-releasePage:
		case <-r.Context().Done():
			peerErr <- r.Context().Err()
			return
		}
		if err := writer.WritePage(0, body); err != nil {
			peerErr <- fmt.Errorf("WritePage: %w", err)
			return
		}
		if err := writer.WriteEnd(); err != nil {
			peerErr <- fmt.Errorf("WriteEnd: %w", err)
			return
		}
		peerErr <- nil
	}))
	defer peer.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD only", r.Method)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"etag-delayed-page"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.readSharding = readShardingPage
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer.URL},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1"})
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.ServeHTTP(rec, req)
	}()

	select {
	case <-headerReady:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer frame header")
	}
	time.Sleep(50 * time.Millisecond)
	close(releasePage)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for coordinator response")
	}
	if err := <-peerErr; err != nil {
		t.Fatalf("peer stream error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestProxyCoordinatorPageReadBatchesByOwnerAndStreamsInOrder(t *testing.T) {
	body := []byte("abcdefghijkl")
	peerBodies := map[string]map[int64][]byte{
		"cache-1": {0: body[0:4], 2: body[8:12]},
		"cache-2": {1: body[4:8]},
	}
	peerRequests := map[string][]internalPageReadRequest{}
	peerServer := func(peerID string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req := decodeInternalPageReadRequest(t, r)
			peerRequests[peerID] = append(peerRequests[peerID], req)
			w.Header().Set("Content-Type", peerrouter.PageFrameContentType)
			w.WriteHeader(http.StatusOK)
			writer, err := peerrouter.NewPageFrameWriter(w)
			if err != nil {
				t.Fatalf("NewPageFrameWriter(%s) error = %v", peerID, err)
			}
			for _, pageIndex := range req.Pages {
				if err := writer.WritePage(pageIndex, peerBodies[peerID][pageIndex]); err != nil {
					t.Fatalf("WritePage(%s, %d) error = %v", peerID, pageIndex, err)
				}
			}
			if err := writer.WriteEnd(); err != nil {
				t.Fatalf("WriteEnd(%s) error = %v", peerID, err)
			}
		}))
	}
	peer1 := peerServer("cache-1")
	defer peer1.Close()
	peer2 := peerServer("cache-2")
	defer peer2.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD only", r.Method)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"etag-multi-owner"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.readSharding = readShardingPage
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer1.URL},
		{ID: "cache-2", URL: peer2.URL},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1", "cache-2", "cache-1"})
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	req.Header.Set("Range", "bytes=2-9")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206; body=%q", rec.Code, rec.Body.String())
	}
	if got, want := rec.Body.Bytes(), body[2:10]; !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got := peerRequests["cache-1"]; len(got) != 1 || !equalInt64Slices(got[0].Pages, []int64{0, 2}) {
		t.Fatalf("cache-1 requests = %+v, want one batch pages [0 2]", got)
	}
	if got := peerRequests["cache-2"]; len(got) != 1 || !equalInt64Slices(got[0].Pages, []int64{1}) {
		t.Fatalf("cache-2 requests = %+v, want one batch pages [1]", got)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	for _, want := range []string{
		`simple_s3_cache_internal_peer_requests_per_client_request_sum{bucket="bucket",strategy="page"} 2`,
		`simple_s3_cache_page_batch_size_sum{bucket="bucket",owner_id="cache-1"} 2`,
		`simple_s3_cache_page_batch_size_sum{bucket="bucket",owner_id="cache-2"} 1`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metricsBody)
		}
	}
}

func TestProxyCoordinatorPageReadFallsBackBeforeCommitWithoutStoringPages(t *testing.T) {
	body := []byte("abcdefghijkl")
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "peer unavailable", http.StatusServiceUnavailable)
	}))
	defer peer.Close()

	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("ETag", `"etag-fallback"`)
			w.WriteHeader(http.StatusOK)
			return
		}
		writeObjectResponse(t, w, r, body, `"etag-fallback"`)
	}))
	defer upstream.Close()

	ctx := context.Background()
	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.readSharding = readShardingPage
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer.URL},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1", "cache-1", "cache-1"})
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 fallback; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("fallback body = %q, want %q", got, body)
	}
	if got, want := upstreamRequests, []string{"HEAD ", "GET "}; !equalStringSlices(got, want) {
		t.Fatalf("upstream requests = %q, want %q", got, want)
	}
	obj, ok, err := p.cache.GetObject(ctx, "bucket", key)
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	if ok {
		t.Fatal("metadata still cached after peer-read fallback planning-state discard")
	}
	_ = obj
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_peer_read_fallbacks_total{bucket="bucket",peer_id="cache-1",reason="status"} 1`) {
		t.Fatalf("metrics missing peer read fallback:\n%s", metricsBody)
	}
}

func TestProxyCoordinatorPageReadClosesDownstreamOnPostCommitPeerFailure(t *testing.T) {
	body := []byte("abcd")
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", peerrouter.PageFrameContentType)
		w.WriteHeader(http.StatusOK)
		writer, err := peerrouter.NewPageFrameWriter(w)
		if err != nil {
			t.Fatalf("NewPageFrameWriter() error = %v", err)
		}
		if err := writer.WritePage(0, body); err != nil {
			t.Fatalf("WritePage() error = %v", err)
		}
	}))
	defer peer.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD only", r.Method)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"etag-post-commit"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.readSharding = readShardingPage
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer.URL},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1"})
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	rec := newHijackableRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 before downstream close; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, body) {
		t.Fatalf("body before close = %q, want %q", got, body)
	}
	if !rec.hijacked.Load() {
		t.Fatal("downstream was not closed after truncated peer frame stream")
	}
}

func TestProxyCoordinatorPageReadRejectsTruncatedFrameAfterRequestedSpan(t *testing.T) {
	body := []byte("ab")
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", peerrouter.PageFrameContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{'S', '3', 'P', 'F'})
		_ = binary.Write(w, binary.BigEndian, peerrouter.PageFrameVersion)
		w.Write([]byte{1})
		_ = binary.Write(w, binary.BigEndian, uint64(0))
		_ = binary.Write(w, binary.BigEndian, uint64(4))
		_, _ = w.Write(body)
	}))
	defer peer.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("upstream method = %q, want HEAD only", r.Method)
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("ETag", `"etag-truncated-after-span"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
	p.readSharding = readShardingPage
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: peer.URL},
	})
	key := keyWithPageOwners(t, router, "bucket", []string{"cache-1"})
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	req.Header.Set("Range", "bytes=0-0")
	rec := newHijackableRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206 before downstream close; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "a" {
		t.Fatalf("body before close = %q, want a", got)
	}
	if !rec.hijacked.Load() {
		t.Fatal("downstream was not closed after remote frame truncated after requested span")
	}
}

func TestProxyPeerModeRejectsForwardingLoop(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive looped peer request")
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyOwnedBy(t, router, "bucket", "cache-1")
	req := httptest.NewRequest(http.MethodGet, internalObjectRoutePrefix+"/bucket/"+key, nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "cache-1")
	req.Header.Set(peerFromHeader, "cache-1")
	req.Header.Set(peerRingHeader, router.RingID())
	signTestInternalPeerRequest(t, req, nil, p.peerAuthSecret)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
}

func TestProxyPeerModeRejectsForwardedRingMismatch(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive request with peer ring mismatch")
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyOwnedBy(t, router, "bucket", "cache-0")
	req := httptest.NewRequest(http.MethodGet, internalObjectRoutePrefix+"/bucket/"+key, nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "cache-0")
	req.Header.Set(peerFromHeader, "cache-1")
	req.Header.Set(peerRingHeader, "different-ring")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
	if ready, reason := p.Readiness(); ready || !strings.Contains(reason, "peer ring mismatch") {
		t.Fatalf("Readiness() = (%v, %q), want peer ring mismatch degradation", ready, reason)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_degraded{reason_code="peer_ring_mismatch"} 1`) {
		t.Fatalf("metrics missing degraded ring mismatch:\n%s", metricsBody)
	}
}

func TestProxyPeerStateReportsRingAndDegradedSnapshot(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	p.markDegradedWithContext("peer ring mismatch", map[string]string{"peer_ring_id": "different-ring"})

	state := p.PeerState()
	if state.Mode != "peer" {
		t.Fatalf("Mode = %q, want peer", state.Mode)
	}
	if state.LocalID != "cache-0" {
		t.Fatalf("LocalID = %q, want cache-0", state.LocalID)
	}
	if state.RingID != router.RingID() {
		t.Fatalf("RingID = %q, want %q", state.RingID, router.RingID())
	}
	if state.Ready {
		t.Fatal("Ready = true, want degraded not-ready")
	}
	if state.Degraded == nil || state.Degraded.Code != "peer_ring_mismatch" {
		t.Fatalf("Degraded = %#v, want peer_ring_mismatch", state.Degraded)
	}
	if len(state.Peers) != 2 {
		t.Fatalf("len(Peers) = %d, want 2", len(state.Peers))
	}
	if !state.AuthConfigured {
		t.Fatal("AuthConfigured = false, want true")
	}
}

func TestProxyPeerModeRejectsForwardedMissingRing(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive request with missing peer ring")
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyOwnedBy(t, router, "bucket", "cache-0")
	req := httptest.NewRequest(http.MethodGet, internalObjectRoutePrefix+"/bucket/"+key, nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "cache-0")
	req.Header.Set(peerFromHeader, "cache-1")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
	if ready, reason := p.Readiness(); !ready || reason != "" {
		t.Fatalf("Readiness() = (%v, %q), want ready after missing ring header rejection", ready, reason)
	}
}

func TestProxyMarksNotReadyWhenLocalCacheCorruptionCannotBeRepaired(t *testing.T) {
	p := testProxy(t, "http://upstream.invalid")
	p.cache = openPageCorruptFailingCache{cacheStore: p.cache}
	target := s3request.Target{Bucket: "bucket", Key: "corrupt.bin"}
	obj := cache.Object{
		ID:    cache.ObjectKey(target.Bucket, target.Key),
		ETag:  `"etag"`,
		Epoch: 1,
	}

	body, ok, err := p.openCachedPage(context.Background(), target, obj, 0, newRequestStats(
		httptest.NewRequest(http.MethodGet, "/bucket/corrupt.bin", nil),
		target,
	), true)
	if err == nil {
		t.Fatal("openCachedPage() error = nil, want corruption error")
	}
	if ok {
		if body != nil {
			_ = body.Close()
		}
		t.Fatal("openCachedPage() ok = true, want false")
	}
	if ready, reason := p.Readiness(); ready || !strings.Contains(reason, "local cache corruption") {
		t.Fatalf("Readiness() = (%v, %q), want local cache corruption degradation", ready, reason)
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_degraded{reason_code="local_cache_corruption"} 1`) {
		t.Fatalf("metrics missing degraded local cache corruption:\n%s", metricsBody)
	}
}

func TestProxyPeerModeRejectsForwardedOwnerMismatch(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive request with peer owner mismatch")
	}))
	defer upstream.Close()

	p := testProxy(t, upstream.URL)
	router := enablePeerMode(t, p, "cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0.invalid"},
		{ID: "cache-1", URL: "http://cache-1.invalid"},
	})
	key := keyOwnedBy(t, router, "bucket", "cache-0")
	req := httptest.NewRequest(http.MethodGet, internalObjectRoutePrefix+"/bucket/"+key, nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "cache-1")
	req.Header.Set(peerFromHeader, "cache-1")
	req.Header.Set(peerRingHeader, router.RingID())
	signTestInternalPeerRequest(t, req, nil, p.peerAuthSecret)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
}

func enablePeerMode(t *testing.T, p *Proxy, localID string, peers []peerrouter.Peer) *peerrouter.Router {
	t.Helper()

	router, err := peerrouter.NewRouter(localID, peers)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	p.peerRouter = router
	p.peerClient = http.DefaultClient
	p.peerTimeout = time.Minute
	p.peerAuthSecret = "test-peer-secret"
	if p.metrics != nil {
		p.metrics.SetPeerRingInfo("peer", router.LocalID(), router.RingID())
	}
	return router
}

func keyOwnedBy(t *testing.T, router *peerrouter.Router, bucket, ownerID string) string {
	t.Helper()

	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("object-%d.bin", i)
		if router.Owner(bucket, key).ID == ownerID {
			return key
		}
	}
	t.Fatalf("could not find key owned by %s", ownerID)
	return ""
}

func keyWithPageOwners(t *testing.T, router *peerrouter.Router, bucket string, ownerIDs []string) string {
	t.Helper()

	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("paged-object-%d.bin", i)
		match := true
		for pageIndex, ownerID := range ownerIDs {
			if router.PageOwner(bucket, key, int64(pageIndex)).ID != ownerID {
				match = false
				break
			}
		}
		if match {
			return key
		}
	}
	t.Fatalf("could not find key with page owners %v", ownerIDs)
	return ""
}

func keyWithPageOwnerAt(t *testing.T, router *peerrouter.Router, bucket string, pageIndex int64, ownerID string) string {
	t.Helper()

	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("paged-object-%d.bin", i)
		if router.PageOwner(bucket, key, pageIndex).ID == ownerID {
			return key
		}
	}
	t.Fatalf("could not find key with page %d owned by %s", pageIndex, ownerID)
	return ""
}

func decodeInternalPageReadRequest(t *testing.T, r *http.Request) internalPageReadRequest {
	t.Helper()

	if r.Method != http.MethodPost {
		t.Fatalf("peer method = %q, want POST", r.Method)
	}
	if got := r.URL.EscapedPath(); got != "/internal/v1/pages/read" {
		t.Fatalf("peer path = %q, want /internal/v1/pages/read", got)
	}
	if got := r.Header.Get(peerFromHeader); got == "" {
		t.Fatal("peer from header is empty")
	}
	if got := r.Header.Get(peerRingHeader); got == "" {
		t.Fatal("peer ring header is empty")
	}
	var req internalPageReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Fatalf("decode internal page read request: %v", err)
	}
	return req
}

type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked atomic.Bool
}

func newHijackableRecorder() *hijackableRecorder {
	return &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
}

func (r *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, client := net.Pipe()
	r.hijacked.Store(true)
	_ = client.Close()
	return server, bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server)), nil
}

type threadSafeRecorder struct {
	mu     sync.Mutex
	header http.Header
	code   int
	body   bytes.Buffer
}

func newThreadSafeRecorder() *threadSafeRecorder {
	return &threadSafeRecorder{header: http.Header{}}
}

func (r *threadSafeRecorder) Header() http.Header {
	return r.header
}

func (r *threadSafeRecorder) WriteHeader(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.code == 0 {
		r.code = status
	}
}

func (r *threadSafeRecorder) Write(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.code == 0 {
		r.code = http.StatusOK
	}
	return r.body.Write(data)
}

func (r *threadSafeRecorder) bodyBytes() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.body.Bytes()...)
}

func assertRequestSignatureMatchesFinalHeaders(t *testing.T, r *http.Request) {
	t.Helper()

	signingTime, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
	if err != nil {
		t.Fatalf("parse X-Amz-Date: %v", err)
	}
	replayURL := &url.URL{
		Scheme:   "http",
		Host:     r.Host,
		Path:     r.URL.Path,
		RawPath:  r.URL.RawPath,
		RawQuery: r.URL.RawQuery,
	}
	replay, err := http.NewRequestWithContext(context.Background(), r.Method, replayURL.String(), nil)
	if err != nil {
		t.Fatalf("build replay request: %v", err)
	}
	replay.Host = r.Host
	for key, values := range r.Header {
		if strings.EqualFold(key, "Authorization") {
			continue
		}
		for _, value := range values {
			replay.Header.Add(key, value)
		}
	}
	credentials := aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Source:          "test",
	}
	if err := v4.NewSigner().SignHTTP(context.Background(), credentials, replay, unsignedPayload, "s3", "us-east-1", signingTime, func(options *v4.SignerOptions) {
		options.DisableURIPathEscaping = true
	}); err != nil {
		t.Fatalf("sign replay request: %v", err)
	}
	if got, want := r.Header.Get("Authorization"), replay.Header.Get("Authorization"); got != want {
		t.Fatalf("Authorization does not match final headers\ngot:  %s\nwant: %s", got, want)
	}
}

func internalPageReadHTTPRequest(t *testing.T, router *peerrouter.Router, body internalPageReadRequest) *http.Request {
	t.Helper()

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal internal page read request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/pages/read", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(peerFromHeader, "cache-1")
	req.Header.Set(peerRingHeader, router.RingID())
	signTestInternalPeerRequest(t, req, data, "test-peer-secret")
	return req
}

func signTestInternalPeerRequest(t *testing.T, req *http.Request, body []byte, secret string) {
	t.Helper()
	req.Header.Set(peerTimestampHeader, strconv.FormatInt(time.Now().Unix(), 10))
	bodyHash := sha256.Sum256(body)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		req.URL.RequestURI(),
		req.Header.Get(peerFromHeader),
		req.Header.Get(peerRingHeader),
		req.Header.Get(peerTimestampHeader),
		hex.EncodeToString(bodyHash[:]),
	)
	req.Header.Set(peerSignatureHeader, hex.EncodeToString(mac.Sum(nil)))
}

func assertInternalPageReadResponse(rec *httptest.ResponseRecorder, pages []int64, bodies [][]byte, maxPageBytes int64) error {
	if rec.Code != http.StatusOK {
		return fmt.Errorf("status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != peerrouter.PageFrameContentType {
		return fmt.Errorf("Content-Type = %q, want %q", got, peerrouter.PageFrameContentType)
	}
	return checkPageFrames(rec.Body, pages, bodies, maxPageBytes)
}

func assertPageFrames(t *testing.T, body io.Reader, pages []int64, bodies [][]byte, maxPageBytes int64) {
	t.Helper()
	if err := checkPageFrames(body, pages, bodies, maxPageBytes); err != nil {
		t.Fatal(err)
	}
}

func checkPageFrames(body io.Reader, pages []int64, bodies [][]byte, maxPageBytes int64) error {
	reader, err := peerrouter.NewPageFrameReader(body, pages, maxPageBytes)
	if err != nil {
		return fmt.Errorf("NewPageFrameReader() error = %w", err)
	}
	for i, pageIndex := range pages {
		frame, err := reader.NextPage()
		if err != nil {
			return fmt.Errorf("NextPage(%d) error = %w", i, err)
		}
		if frame.Index != pageIndex {
			return fmt.Errorf("frame index = %d, want %d", frame.Index, pageIndex)
		}
		if !bytes.Equal(frame.Bytes, bodies[i]) {
			return fmt.Errorf("frame %d bytes = %q, want %q", pageIndex, frame.Bytes, bodies[i])
		}
	}
	if _, err := reader.NextPage(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("final NextPage() error = %v, want EOF", err)
	}
	return nil
}

func testProxy(t *testing.T, endpoint string) *Proxy {
	t.Helper()
	return testProxyWithPageSize(t, endpoint, 4<<20)
}

func testProxyWithPageSize(t *testing.T, endpoint string, pageSize int64) *Proxy {
	t.Helper()

	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	root := t.TempDir()
	recorder := metrics.NewRecorder(1 << 30)
	cacheStore, err := cache.Open(context.Background(), cache.Options{
		CachePath: filepath.Join(root, "cache-bytes"),
		MetaPath:  filepath.Join(root, "cache-meta"),
		Metrics:   recorder,
	})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() {
		if err := cacheStore.Close(); err != nil {
			t.Fatalf("close cache: %v", err)
		}
	})

	return &Proxy{
		upstreamEndpoint: parsed,
		region:           "us-east-1",
		credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				Source:          "test",
			}, nil
		}),
		signer:   v4.NewSigner(),
		client:   upstreamClient(endpoint),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		cache:    cacheStore,
		pageSize: pageSize,
		upload: uploadOptions{
			maxSpoolSize: 10 << 30,
		},
		metrics:      recorder,
		peerClient:   http.DefaultClient,
		readSharding: readShardingAuto,
		pageShardMin: 2,
	}
}

func testPeerRouter(t *testing.T) *peerrouter.Router {
	t.Helper()

	router, err := peerrouter.NewRouter("cache-0", []peerrouter.Peer{
		{ID: "cache-0", URL: "http://cache-0:8080"},
	})
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	return router
}

func testProxyWithBucketPageSizes(t *testing.T, endpoint string, pageSize int64, pageSizeByBucket map[string]int64) *Proxy {
	t.Helper()

	p := testProxyWithPageSize(t, endpoint, pageSize)
	p.pageSizeByBucket = pageSizeByBucket
	return p
}

func testProxyWithUpstreamHost(t *testing.T, endpoint, upstreamHost string) *Proxy {
	t.Helper()

	p := testProxy(t, endpoint)
	p.upstreamHost = upstreamHost
	return p
}

func upstreamClient(_ string) *http.Client {
	return http.DefaultClient
}

func renderProxyMetrics(t *testing.T, recorder *metrics.Recorder) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func serveRangeRequest(p *Proxy, path, rangeHeader string, wantBody []byte) error {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Range", rangeHeader)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		return fmt.Errorf("status = %d, want 206; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.Bytes(); !bytes.Equal(got, wantBody) {
		return fmt.Errorf("body = %q, want %q", got, wantBody)
	}
	return nil
}

type metadataStoreFailingCache struct {
	cacheStore
}

func (c metadataStoreFailingCache) PutObject(context.Context, cache.ObjectMetadata) (cache.Object, error) {
	return cache.Object{}, errors.New("metadata store failed")
}

type invalidationFailingCache struct {
	cacheStore
}

func (c invalidationFailingCache) DeleteObject(context.Context, string, string) error {
	return errors.New("invalidation failed")
}

func (c invalidationFailingCache) InvalidateObject(context.Context, string, string, int64) (int64, error) {
	return 0, errors.New("invalidation failed")
}

type openPageCorruptFailingCache struct {
	cacheStore
}

func (c openPageCorruptFailingCache) OpenPage(context.Context, string, int64, string, int64) (io.ReadCloser, bool, error) {
	return nil, false, errors.New("delete corrupt page file: permission denied")
}

type beginPageWriteFailingCache struct {
	cacheStore
}

func (c beginPageWriteFailingCache) BeginPageWrite(context.Context, cache.PageWriteOptions) (*cache.PageWriter, error) {
	return nil, errors.New("cache write failed")
}

type cacheTouchCountingCache struct {
	cacheStore
	calls *atomic.Int32
}

func (c *cacheTouchCountingCache) PutObject(ctx context.Context, metadata cache.ObjectMetadata) (cache.Object, error) {
	c.calls.Add(1)
	return c.cacheStore.PutObject(ctx, metadata)
}

func (c *cacheTouchCountingCache) GetObject(ctx context.Context, bucket, key string) (cache.Object, bool, error) {
	c.calls.Add(1)
	return c.cacheStore.GetObject(ctx, bucket, key)
}

func (c *cacheTouchCountingCache) DeleteObject(ctx context.Context, bucket, key string) error {
	c.calls.Add(1)
	return c.cacheStore.DeleteObject(ctx, bucket, key)
}

func (c *cacheTouchCountingCache) InvalidateObject(ctx context.Context, bucket, key string, minEpoch int64) (int64, error) {
	c.calls.Add(1)
	return c.cacheStore.InvalidateObject(ctx, bucket, key, minEpoch)
}

func (c *cacheTouchCountingCache) StorePage(ctx context.Context, write cache.PageWrite) (cache.Page, error) {
	c.calls.Add(1)
	return c.cacheStore.StorePage(ctx, write)
}

func (c *cacheTouchCountingCache) BeginPageWrite(ctx context.Context, opts cache.PageWriteOptions) (*cache.PageWriter, error) {
	c.calls.Add(1)
	return c.cacheStore.BeginPageWrite(ctx, opts)
}

func (c *cacheTouchCountingCache) ListPages(ctx context.Context, objectID string) ([]cache.Page, error) {
	c.calls.Add(1)
	return c.cacheStore.ListPages(ctx, objectID)
}

func (c *cacheTouchCountingCache) OpenPage(ctx context.Context, objectID string, index int64, expectedETag string, expectedEpoch int64) (io.ReadCloser, bool, error) {
	c.calls.Add(1)
	return c.cacheStore.OpenPage(ctx, objectID, index, expectedETag, expectedEpoch)
}

type missingPageSignalCache struct {
	cacheStore
	misses         *atomic.Int32
	secondMissSeen chan struct{}
	secondSignaled atomic.Bool
}

func (c *missingPageSignalCache) OpenPage(ctx context.Context, objectID string, index int64, expectedETag string, expectedEpoch int64) (io.ReadCloser, bool, error) {
	body, ok, err := c.cacheStore.OpenPage(ctx, objectID, index, expectedETag, expectedEpoch)
	if err == nil && !ok && c.misses.Add(1) == 2 && c.secondSignaled.CompareAndSwap(false, true) {
		close(c.secondMissSeen)
	}
	return body, ok, err
}

type listPagesCountingCache struct {
	cacheStore
	calls *atomic.Int32
}

func (c *listPagesCountingCache) ListPages(ctx context.Context, objectID string) ([]cache.Page, error) {
	c.calls.Add(1)
	return c.cacheStore.ListPages(ctx, objectID)
}

type storePageCountingCache struct {
	cacheStore
	calls *atomic.Int32
}

func (c *storePageCountingCache) StorePage(ctx context.Context, write cache.PageWrite) (cache.Page, error) {
	c.calls.Add(1)
	return c.cacheStore.StorePage(ctx, write)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalInt64Slices(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type abortTrackingResponseWriter struct {
	*httptest.ResponseRecorder
	aborted bool
}

func (w *abortTrackingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.aborted = true
	return closeOnlyConn{}, bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(io.Discard)), nil
}

type closeOnlyConn struct{}

func (closeOnlyConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (closeOnlyConn) Write(p []byte) (int, error)     { return len(p), nil }
func (closeOnlyConn) Close() error                    { return nil }
func (closeOnlyConn) LocalAddr() net.Addr             { return dummyAddr("local") }
func (closeOnlyConn) RemoteAddr() net.Addr            { return dummyAddr("remote") }
func (closeOnlyConn) SetDeadline(time.Time) error     { return nil }
func (closeOnlyConn) SetReadDeadline(time.Time) error { return nil }
func (closeOnlyConn) SetWriteDeadline(time.Time) error {
	return nil
}

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

func writeObjectResponse(t *testing.T, w http.ResponseWriter, r *http.Request, body []byte, etag string) {
	t.Helper()

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", "Tue, 16 Jun 2026 00:00:00 GMT")
	w.Header().Set("X-Amz-Meta-Test", "cache")

	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && ifMatch != etag {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", "12")
		w.WriteHeader(http.StatusOK)
		return
	}

	switch r.Header.Get("Range") {
	case "":
		w.Header().Set("Content-Length", "12")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	case "bytes=0-3":
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[0:4])
	case "bytes=0-4":
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Content-Range", "bytes 0-4/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[0:5])
	case "bytes=4-7":
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 4-7/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[4:8])
	case "bytes=5-9":
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Content-Range", "bytes 5-9/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[5:10])
	case "bytes=8-11":
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 8-11/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[8:12])
	case "bytes=10-11":
		w.Header().Set("Content-Length", "2")
		w.Header().Set("Content-Range", "bytes 10-11/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[10:12])
	default:
		t.Fatalf("unexpected upstream range %q", r.Header.Get("Range"))
	}
}

type chunkedTestReader struct {
	chunks [][]byte
}

func newChunkedTestReader(chunks ...[]byte) *chunkedTestReader {
	return &chunkedTestReader{chunks: chunks}
}

func (r *chunkedTestReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[0])
	r.chunks[0] = r.chunks[0][n:]
	if len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

func awsChunkedBody(chunks ...[]byte) []byte {
	var body bytes.Buffer
	for _, chunk := range chunks {
		_, _ = fmt.Fprintf(&body, "%x;chunk-signature=%064x\r\n", len(chunk), 0)
		_, _ = body.Write(chunk)
		_, _ = body.WriteString("\r\n")
	}
	_, _ = body.WriteString("0;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n\r\n")
	return body.Bytes()
}
