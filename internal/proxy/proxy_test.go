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
		"method":                 http.MethodGet,
		"bucket":                 "photos",
		"key":                    "object.bin",
		"cache_result":           "miss",
		"requested_range":        "bytes=2-7",
		"bytes_requested":        float64(6),
		"pages_requested":        float64(2),
		"pages_hit":              float64(0),
		"pages_missed":           float64(2),
		"status":                 float64(http.StatusPartialContent),
		"bytes_sent":             float64(6),
		"bytes_fetched_upstream": float64(8),
	} {
		if entry[key] != want {
			t.Fatalf("log field %s = %#v, want %#v\nentry=%#v", key, entry[key], want, entry)
		}
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
	for _, want := range []string{
		`simple_s3_cache_cache_metadata_duration_seconds_count{bucket="bucket",cache_result="hit"} 1`,
		`simple_s3_cache_cache_page_open_duration_seconds_count{bucket="bucket",cache_result="hit"} 1`,
		`simple_s3_cache_cache_response_copy_duration_seconds_count{bucket="bucket",cache_result="hit"} 1`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metricsBody)
		}
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

	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not receive remotely owned request")
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
	for _, want := range []string{
		`simple_s3_cache_peer_owner_decisions_total{bucket="bucket",decision="remote",owner_id="cache-1"} 1`,
		`simple_s3_cache_peer_forwarded_requests_total{bucket="bucket",peer_id="cache-1",method="GET",status_class="2xx"} 1`,
		`simple_s3_cache_peer_forward_response_bytes_total{bucket="bucket",peer_id="cache-1"} 9`,
		`simple_s3_cache_peer_forward_duration_seconds_count{bucket="bucket",peer_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_peer_response_header_duration_seconds_count{bucket="bucket",peer_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_peer_response_copy_duration_seconds_count{bucket="bucket",peer_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_peer_response_body_read_duration_seconds_count{bucket="bucket",peer_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_peer_downstream_write_duration_seconds_count{bucket="bucket",peer_id="cache-1",status_class="2xx"} 1`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metricsBody)
		}
	}
}

func TestProxyPeerModeHandlesLocalOwnerRequestLocally(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("peer should not receive locally owned request")
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
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_peer_owner_decisions_total{bucket="bucket",decision="local",owner_id="cache-0"} 1`) {
		t.Fatalf("metrics missing local owner decision:\n%s", metricsBody)
	}
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
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerFromHeader, "cache-1")
	req.Header.Set(peerRingHeader, router.RingID())
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_peer_forward_failures_total{bucket="bucket",peer_id="cache-1",reason="routing_mismatch"} 1`) {
		t.Fatalf("metrics missing routing mismatch failure:\n%s", metricsBody)
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
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "cache-0")
	req.Header.Set(peerFromHeader, "gateway")
	req.Header.Set(peerRingHeader, "different-ring")
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_peer_forward_failures_total{bucket="bucket",peer_id="cache-0",reason="ring_mismatch"} 1`) {
		t.Fatalf("metrics missing ring mismatch failure:\n%s", metricsBody)
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
	req := httptest.NewRequest(http.MethodGet, "/bucket/"+key, nil)
	req.Header.Set(peerForwardedHeader, "1")
	req.Header.Set(peerOwnerHeader, "cache-1")
	req.Header.Set(peerFromHeader, "gateway")
	req.Header.Set(peerRingHeader, router.RingID())
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%q", rec.Code, rec.Body.String())
	}
	metricsBody := renderProxyMetrics(t, p.metrics)
	if !strings.Contains(metricsBody, `simple_s3_cache_peer_forward_failures_total{bucket="bucket",peer_id="cache-0",reason="owner_mismatch"} 1`) {
		t.Fatalf("metrics missing owner mismatch failure:\n%s", metricsBody)
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
		metrics: recorder,
	}
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
