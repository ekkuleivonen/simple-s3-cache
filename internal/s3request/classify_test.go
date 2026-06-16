package s3request

import (
	"net/http"
	"testing"
)

func TestClassifyCacheableObjectReads(t *testing.T) {
	tests := []struct {
		name string
		req  Request
		want Disposition
	}{
		{
			name: "plain get object",
			req:  Request{Method: http.MethodGet, Target: Target{Bucket: "bucket", Key: "key"}},
			want: CacheableFullObject,
		},
		{
			name: "plain head object",
			req:  Request{Method: http.MethodHead, Target: Target{Bucket: "bucket", Key: "key"}},
			want: CacheableHeadObject,
		},
		{
			name: "single range get object",
			req: Request{
				Method: http.MethodGet,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"Range": []string{"bytes=0-99"}},
			},
			want: CacheableRangeObject,
		},
		{
			name: "conditional get object",
			req: Request{
				Method: http.MethodGet,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"If-None-Match": []string{`"etag"`}},
			},
			want: CacheableFullObject,
		},
		{
			name: "conditional head object",
			req: Request{
				Method: http.MethodHead,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"If-Modified-Since": []string{"Tue, 16 Jun 2026 00:00:00 GMT"}},
			},
			want: CacheableHeadObject,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.req)
			if got.Disposition != tt.want {
				t.Fatalf("Disposition = %v, want %v; reason=%q", got.Disposition, tt.want, got.Reason)
			}
		})
	}
}

func TestClassifyPassThroughRequests(t *testing.T) {
	tests := []struct {
		name       string
		req        Request
		wantReason string
	}{
		{
			name:       "bucket operation",
			req:        Request{Method: http.MethodGet, Target: Target{Bucket: "bucket"}},
			wantReason: "not_object",
		},
		{
			name:       "write operation",
			req:        Request{Method: http.MethodPut, Target: Target{Bucket: "bucket", Key: "key"}},
			wantReason: "method",
		},
		{
			name:       "versioned read",
			req:        Request{Method: http.MethodGet, Target: Target{Bucket: "bucket", Key: "key"}, RawQuery: "versionId=123"},
			wantReason: "query",
		},
		{
			name:       "subresource read",
			req:        Request{Method: http.MethodGet, Target: Target{Bucket: "bucket", Key: "key"}, RawQuery: "tagging="},
			wantReason: "query",
		},
		{
			name: "response override",
			req: Request{
				Method:   http.MethodGet,
				Target:   Target{Bucket: "bucket", Key: "key"},
				RawQuery: "response-content-type=text/plain",
			},
			wantReason: "query",
		},
		{
			name: "presigned url",
			req: Request{
				Method:   http.MethodGet,
				Target:   Target{Bucket: "bucket", Key: "key"},
				RawQuery: "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc",
			},
			wantReason: "query",
		},
		{
			name: "sse-c request",
			req: Request{
				Method: http.MethodGet,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{
					"X-Amz-Server-Side-Encryption-Customer-Key": []string{"secret"},
				},
			},
			wantReason: "sse_c",
		},
		{
			name: "multi range get",
			req: Request{
				Method: http.MethodGet,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"Range": []string{"bytes=0-99,200-299"}},
			},
			wantReason: "range",
		},
		{
			name: "malformed range get",
			req: Request{
				Method: http.MethodGet,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"Range": []string{"not-bytes"}},
			},
			wantReason: "range",
		},
		{
			name: "nonnumeric range get",
			req: Request{
				Method: http.MethodGet,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"Range": []string{"bytes=abc-99"}},
			},
			wantReason: "range",
		},
		{
			name: "overflowing range get",
			req: Request{
				Method: http.MethodGet,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"Range": []string{"bytes=999999999999999999999999999999-"}},
			},
			wantReason: "range",
		},
		{
			name: "head with range",
			req: Request{
				Method: http.MethodHead,
				Target: Target{Bucket: "bucket", Key: "key"},
				Header: http.Header{"Range": []string{"bytes=0-99"}},
			},
			wantReason: "range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.req)
			if got.Disposition != PassThrough {
				t.Fatalf("Disposition = %v, want PassThrough", got.Disposition)
			}
			if got.Reason != tt.wantReason {
				t.Fatalf("Reason = %q, want %q", got.Reason, tt.wantReason)
			}
		})
	}
}
