package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/ekkuleivonen/simple-s3-cache/internal/cache"
	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"
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

func TestProxyCachesSinglePageRangeRead(t *testing.T) {
	body := []byte("abcdefghijkl")
	var upstreamRequests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests = append(upstreamRequests, r.Method+" "+r.Header.Get("Range"))
		writeObjectResponse(t, w, r, body, `"etag-range"`)
	}))
	defer upstream.Close()

	p := testProxyWithPageSize(t, upstream.URL, 4)
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
	}

	wantRequests := []string{"HEAD ", "GET bytes=0-3"}
	if !equalStringSlices(upstreamRequests, wantRequests) {
		t.Fatalf("upstream requests = %q, want %q", upstreamRequests, wantRequests)
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

	wantRequests := []string{"HEAD ", "GET bytes=0-4", "GET bytes=5-9", "GET bytes=10-11"}
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
	wantRequests := []string{"HEAD ", "GET bytes=0-3"}
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
		metrics:  recorder,
	}
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

func writeObjectResponse(t *testing.T, w http.ResponseWriter, r *http.Request, body []byte, etag string) {
	t.Helper()

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", etag)
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
	case "bytes=10-11":
		w.Header().Set("Content-Length", "2")
		w.Header().Set("Content-Range", "bytes 10-11/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[10:12])
	default:
		t.Fatalf("unexpected upstream range %q", r.Header.Get("Range"))
	}
}
