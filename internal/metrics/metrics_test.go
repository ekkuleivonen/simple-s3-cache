package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecorderRendersGlobalAndBucketMetrics(t *testing.T) {
	recorder := NewRecorder(1024)

	recorder.RecordPageHit("photos")
	recorder.RecordPageMiss("photos")
	recorder.RecordPassThrough("photos", http.MethodGet)
	recorder.RecordInvalidation("photos")
	recorder.RecordCacheWriteFailure("photos")
	recorder.RecordEviction("photos")
	recorder.RecordUpstreamFailure("photos", "fill")
	recorder.RecordPeerDecision("photos", "remote", "cache-1")
	recorder.RecordPeerForward("photos", "cache-1", http.MethodGet, "2xx")
	recorder.RecordPeerForwardFailure("photos", "cache-1", "request_failed")
	recorder.RecordPeerForwardResponseBytes("photos", "cache-1", 13)
	recorder.RecordBytesServedFromCache("photos", 3)
	recorder.RecordBytesServedFromUpstream("photos", 5)
	recorder.RecordUpstreamFillBytes("photos", 8)
	recorder.ObserveRequestedBytes("photos", 3)
	recorder.ObservePagesTouched("photos", 2)
	recorder.ObserveReadAmplification("photos", float64(8)/3)
	recorder.ObserveUpstreamDuration("photos", "fill", 25*time.Millisecond)
	recorder.ObserveCacheServeDuration("photos", 2*time.Millisecond)
	recorder.ObserveCacheMetadataDuration("photos", "hit", time.Millisecond)
	recorder.ObserveCachePageOpenDuration("photos", "hit", 2*time.Millisecond)
	recorder.ObserveCacheResponseCopyDuration("photos", "hit", 3*time.Millisecond)
	recorder.ObservePeerForwardDuration("photos", "cache-1", "2xx", time.Millisecond)
	recorder.ObservePeerResponseHeaderDuration("photos", "cache-1", "2xx", 2*time.Millisecond)
	recorder.ObservePeerResponseCopyDuration("photos", "cache-1", "2xx", 3*time.Millisecond)
	recorder.SetCachedBytes(64, map[string]int64{"photos": 64})

	body := renderMetrics(t, recorder)
	for _, want := range []string{
		`simple_s3_cache_page_hits_total 1`,
		`simple_s3_cache_page_hits_total{bucket="photos"} 1`,
		`simple_s3_cache_page_misses_total{bucket="photos"} 1`,
		`simple_s3_cache_pass_through_requests_total{bucket="photos",method="GET"} 1`,
		`simple_s3_cache_invalidations_total{bucket="photos"} 1`,
		`simple_s3_cache_cache_write_failures_total{bucket="photos"} 1`,
		`simple_s3_cache_evictions_total{bucket="photos"} 1`,
		`simple_s3_cache_upstream_request_failures_total{bucket="photos",operation="fill"} 1`,
		`simple_s3_cache_peer_owner_decisions_total{bucket="photos",decision="remote",owner_id="cache-1"} 1`,
		`simple_s3_cache_peer_forwarded_requests_total{bucket="photos",peer_id="cache-1",method="GET",status_class="2xx"} 1`,
		`simple_s3_cache_peer_forward_failures_total{bucket="photos",peer_id="cache-1",reason="request_failed"} 1`,
		`simple_s3_cache_peer_forward_response_bytes_total{bucket="photos",peer_id="cache-1"} 13`,
		`simple_s3_cache_bytes_served_from_cache_total{bucket="photos"} 3`,
		`simple_s3_cache_bytes_served_from_upstream_total{bucket="photos"} 5`,
		`simple_s3_cache_upstream_fill_bytes_total{bucket="photos"} 8`,
		`simple_s3_cache_cached_bytes 64`,
		`simple_s3_cache_cached_bytes{bucket="photos"} 64`,
		`simple_s3_cache_cache_max_bytes 1024`,
		`simple_s3_cache_requested_bytes_sum{bucket="photos"} 3`,
		`simple_s3_cache_pages_touched_sum{bucket="photos"} 2`,
		`simple_s3_cache_read_amplification_sum{bucket="photos"} 2.666`,
		`simple_s3_cache_cache_metadata_duration_seconds_count{bucket="photos",cache_result="hit"} 1`,
		`simple_s3_cache_cache_page_open_duration_seconds_count{bucket="photos",cache_result="hit"} 1`,
		`simple_s3_cache_cache_response_copy_duration_seconds_count{bucket="photos",cache_result="hit"} 1`,
		`simple_s3_cache_peer_forward_duration_seconds_count{bucket="photos",peer_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_peer_response_header_duration_seconds_count{bucket="photos",peer_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_peer_response_copy_duration_seconds_count{bucket="photos",peer_id="cache-1",status_class="2xx"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func renderMetrics(t *testing.T, recorder *Recorder) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	return rec.Body.String()
}
