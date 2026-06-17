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
	recorder.RecordReadStrategy("photos", "page")
	recorder.RecordCacheResult("photos", "hit", "2xx", 3)
	recorder.RecordPeerReadFallback("photos", "cache-1", "status")
	recorder.RecordCoordinatorRequest("photos", http.MethodGet, "page", "2xx")
	recorder.RecordPageOwnerRequest("photos", "cache-1", "2xx")
	recorder.RecordPageOwnerBytesServed("photos", "cache-1", 34)
	recorder.RecordPageOwnerUpstreamFillBytes("photos", "cache-1", 13)
	recorder.RecordInvalidationBroadcast("photos", "cache-1", "success")
	recorder.RecordInternalPeerRequestFailure("photos", "cache-1", "status")
	recorder.RecordFillCoalesced("photos", "hit")
	recorder.RecordBytesServedFromCache("photos", 3)
	recorder.RecordBytesServedFromUpstream("photos", 5)
	recorder.RecordUpstreamFillBytes("photos", 8)
	recorder.ObserveRequestedBytes("photos", 3)
	recorder.ObservePagesTouched("photos", 2)
	recorder.ObserveReadAmplification("photos", float64(8)/3)
	recorder.ObserveUpstreamDuration("photos", "fill", 25*time.Millisecond)
	recorder.ObserveCacheServeDuration("photos", 2*time.Millisecond)
	recorder.ObserveInternalPeerRequestsPerClientRequest("photos", "page", 2)
	recorder.ObservePageBatchSize("photos", "cache-1", 3)
	recorder.ObserveInternalPeerRequestDuration("photos", "cache-1", "2xx", 20*time.Millisecond)
	recorder.ObserveInvalidationBroadcastDuration("photos", "cache-1", "success", 30*time.Millisecond)
	recorder.SetCachedBytes(64, map[string]int64{"photos": 64})
	recorder.SetPeerRingInfo("peer", "cache-0", "ring-123", 4)
	recorder.SetDegraded("peer_ring_mismatch")

	body := renderMetrics(t, recorder)
	for _, want := range []string{
		`simple_s3_cache_page_hits_total{bucket="photos"} 1`,
		`simple_s3_cache_page_misses_total{bucket="photos"} 1`,
		`simple_s3_cache_pass_through_requests_total{bucket="photos",method="GET"} 1`,
		`simple_s3_cache_invalidations_total{bucket="photos"} 1`,
		`simple_s3_cache_cache_write_failures_total{bucket="photos"} 1`,
		`simple_s3_cache_evictions_total{bucket="photos"} 1`,
		`simple_s3_cache_upstream_request_failures_total{bucket="photos",operation="fill"} 1`,
		`simple_s3_cache_read_strategy_selected_total{bucket="photos",strategy="page"} 1`,
		`simple_s3_cache_cache_requests_total{bucket="photos",cache_status="hit",status_class="2xx"} 1`,
		`simple_s3_cache_cache_bytes_total{bucket="photos",cache_status="hit",status_class="2xx"} 3`,
		`simple_s3_cache_peer_read_fallbacks_total{bucket="photos",peer_id="cache-1",reason="status"} 1`,
		`simple_s3_cache_coordinator_requests_total{bucket="photos",method="GET",strategy="page",status_class="2xx"} 1`,
		`simple_s3_cache_page_owner_requests_total{bucket="photos",owner_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_page_owner_bytes_served_total{bucket="photos",owner_id="cache-1"} 34`,
		`simple_s3_cache_page_owner_upstream_fill_bytes_total{bucket="photos",owner_id="cache-1"} 13`,
		`simple_s3_cache_invalidation_broadcasts_total{bucket="photos",peer_id="cache-1",status="success"} 1`,
		`simple_s3_cache_internal_peer_request_failures_total{bucket="photos",peer_id="cache-1",reason="status"} 1`,
		`simple_s3_cache_fill_coalesced_total{bucket="photos",result="hit"} 1`,
		`simple_s3_cache_bytes_served_from_cache_total{bucket="photos"} 3`,
		`simple_s3_cache_bytes_served_from_upstream_total{bucket="photos"} 5`,
		`simple_s3_cache_upstream_fill_bytes_total{bucket="photos"} 8`,
		`simple_s3_cache_cached_bytes 64`,
		`simple_s3_cache_cached_bytes{bucket="photos"} 64`,
		`simple_s3_cache_cache_max_bytes 1024`,
		`simple_s3_cache_peer_ring_info{mode="peer",local_id="cache-0",ring_id="ring-123",peer_count="4"} 1`,
		`simple_s3_cache_degraded{reason_code="peer_ring_mismatch"} 1`,
		`simple_s3_cache_requested_bytes_sum{bucket="photos"} 3`,
		`simple_s3_cache_pages_touched_sum{bucket="photos"} 2`,
		`simple_s3_cache_read_amplification_sum{bucket="photos"} 2.666`,
		`simple_s3_cache_internal_peer_requests_per_client_request_count{bucket="photos",strategy="page"} 1`,
		`simple_s3_cache_page_batch_size_count{bucket="photos",owner_id="cache-1"} 1`,
		`simple_s3_cache_internal_peer_request_duration_seconds_count{bucket="photos",owner_id="cache-1",status_class="2xx"} 1`,
		`simple_s3_cache_invalidation_broadcast_duration_seconds_count{bucket="photos",peer_id="cache-1",status="success"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "simple_s3_cache_page_hits_total 1\n") {
		t.Fatalf("metrics body includes duplicate unlabeled counter series:\n%s", body)
	}
}

func TestRecorderRendersHealthyDegradedGauge(t *testing.T) {
	recorder := NewRecorder(1024)

	body := renderMetrics(t, recorder)

	if !strings.Contains(body, `simple_s3_cache_degraded 0`) {
		t.Fatalf("metrics body missing healthy degraded gauge:\n%s", body)
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
