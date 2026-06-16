package proxy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
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
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
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

func testProxy(t *testing.T, endpoint string) *Proxy {
	t.Helper()

	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}

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
		signer: v4.NewSigner(),
		client: upstreamClient(endpoint),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func upstreamClient(_ string) *http.Client {
	return http.DefaultClient
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
