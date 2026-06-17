package metrics

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const contentType = "text/plain; version=0.0.4; charset=utf-8"

type Recorder struct {
	mu sync.Mutex

	cacheMaxBytes  int64
	cachedBytes    int64
	cachedByBucket map[string]int64
	peerRingInfo   string
	degradedReason string

	counters   map[string]map[string]float64
	histograms map[string]*histogram
}

type histogram struct {
	buckets []float64
	series  map[string]*histogramSeries
}

type histogramSeries struct {
	counts []uint64
	sum    float64
	count  uint64
}

func NewRecorder(cacheMaxBytes int64) *Recorder {
	r := &Recorder{
		cacheMaxBytes:  cacheMaxBytes,
		cachedByBucket: map[string]int64{},
		counters:       map[string]map[string]float64{},
		histograms:     map[string]*histogram{},
	}
	r.registerHistogram("simple_s3_cache_requested_bytes", []float64{0, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864})
	r.registerHistogram("simple_s3_cache_pages_touched", []float64{0, 1, 2, 4, 8, 16, 32, 64, 128})
	r.registerHistogram("simple_s3_cache_read_amplification", []float64{0, 1, 1.25, 1.5, 2, 4, 8, 16, 32})
	r.registerHistogram("simple_s3_cache_upstream_duration_seconds", []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10})
	r.registerHistogram("simple_s3_cache_hit_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1})
	r.registerHistogram("simple_s3_cache_internal_peer_requests_per_client_request", []float64{0, 1, 2, 4, 8, 16, 32, 64})
	r.registerHistogram("simple_s3_cache_page_batch_size", []float64{0, 1, 2, 4, 8, 16, 32, 64, 128})
	r.registerHistogram("simple_s3_cache_internal_peer_request_duration_seconds", []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10})
	r.registerHistogram("simple_s3_cache_invalidation_broadcast_duration_seconds", []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10})
	return r
}

func (r *Recorder) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write([]byte(r.Render()))
	})
}

func (r *Recorder) RecordPageHit(bucket string) {
	r.inc("simple_s3_cache_page_hits_total", bucketLabels(bucket), 1)
}

func (r *Recorder) RecordPageMiss(bucket string) {
	r.inc("simple_s3_cache_page_misses_total", bucketLabels(bucket), 1)
}

func (r *Recorder) RecordPassThrough(bucket, method string) {
	r.inc("simple_s3_cache_pass_through_requests_total", labels(label{"bucket", bucket}, label{"method", method}), 1)
}

func (r *Recorder) RecordInvalidation(bucket string) {
	r.inc("simple_s3_cache_invalidations_total", bucketLabels(bucket), 1)
}

func (r *Recorder) RecordCacheWriteFailure(bucket string) {
	r.inc("simple_s3_cache_cache_write_failures_total", bucketLabels(bucket), 1)
}

func (r *Recorder) RecordEviction(bucket string) {
	r.inc("simple_s3_cache_evictions_total", bucketLabels(bucket), 1)
}

func (r *Recorder) RecordUpstreamFailure(bucket, operation string) {
	r.inc("simple_s3_cache_upstream_request_failures_total", labels(label{"bucket", bucket}, label{"operation", operation}), 1)
}

func (r *Recorder) RecordReadStrategy(bucket, strategy string) {
	r.inc("simple_s3_cache_read_strategy_selected_total", labels(label{"bucket", bucket}, label{"strategy", strategy}), 1)
}

func (r *Recorder) RecordCacheResult(bucket, cacheStatus, statusClass string, bytes int64) {
	labelKey := labels(label{"bucket", bucket}, label{"cache_status", cacheStatus}, label{"status_class", statusClass})
	r.inc("simple_s3_cache_cache_requests_total", labelKey, 1)
	if bytes > 0 {
		r.inc("simple_s3_cache_cache_bytes_total", labelKey, float64(bytes))
	}
}

func (r *Recorder) RecordPeerReadFallback(bucket, peerID, reason string) {
	r.inc("simple_s3_cache_peer_read_fallbacks_total", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"reason", reason}), 1)
}

func (r *Recorder) RecordCoordinatorRequest(bucket, method, strategy, statusClass string) {
	r.inc("simple_s3_cache_coordinator_requests_total", labels(label{"bucket", bucket}, label{"method", method}, label{"strategy", strategy}, label{"status_class", statusClass}), 1)
}

func (r *Recorder) RecordPageOwnerRequest(bucket, ownerID, statusClass string) {
	r.inc("simple_s3_cache_page_owner_requests_total", pageOwnerStatusLabels(bucket, ownerID, statusClass), 1)
}

func (r *Recorder) RecordPageOwnerBytesServed(bucket, ownerID string, bytes int64) {
	r.inc("simple_s3_cache_page_owner_bytes_served_total", pageOwnerLabels(bucket, ownerID), float64(bytes))
}

func (r *Recorder) RecordPageOwnerUpstreamFillBytes(bucket, ownerID string, bytes int64) {
	r.inc("simple_s3_cache_page_owner_upstream_fill_bytes_total", pageOwnerLabels(bucket, ownerID), float64(bytes))
}

func (r *Recorder) RecordInvalidationBroadcast(bucket, peerID, status string) {
	r.inc("simple_s3_cache_invalidation_broadcasts_total", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"status", status}), 1)
}

func (r *Recorder) RecordInternalPeerRequestFailure(bucket, peerID, reason string) {
	r.inc("simple_s3_cache_internal_peer_request_failures_total", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"reason", reason}), 1)
}

func (r *Recorder) RecordFillCoalesced(bucket, result string) {
	r.inc("simple_s3_cache_fill_coalesced_total", labels(label{"bucket", bucket}, label{"result", result}), 1)
}

func (r *Recorder) SetPeerRingInfo(mode, localID, ringID string, peerCount ...int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	values := []label{{"mode", mode}, {"local_id", localID}, {"ring_id", ringID}}
	if len(peerCount) > 0 {
		values = append(values, label{"peer_count", strconv.Itoa(peerCount[0])})
	}
	r.peerRingInfo = labels(values...)
}

func (r *Recorder) SetDegraded(reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.degradedReason = strings.TrimSpace(reason)
}

func (r *Recorder) RecordBytesServedFromCache(bucket string, bytes int64) {
	r.inc("simple_s3_cache_bytes_served_from_cache_total", bucketLabels(bucket), float64(bytes))
}

func (r *Recorder) RecordBytesServedFromUpstream(bucket string, bytes int64) {
	r.inc("simple_s3_cache_bytes_served_from_upstream_total", bucketLabels(bucket), float64(bytes))
}

func (r *Recorder) RecordUpstreamFillBytes(bucket string, bytes int64) {
	r.inc("simple_s3_cache_upstream_fill_bytes_total", bucketLabels(bucket), float64(bytes))
}

func (r *Recorder) ObserveRequestedBytes(bucket string, bytes int64) {
	r.observe("simple_s3_cache_requested_bytes", bucketLabels(bucket), float64(bytes))
}

func (r *Recorder) ObservePagesTouched(bucket string, pages int64) {
	r.observe("simple_s3_cache_pages_touched", bucketLabels(bucket), float64(pages))
}

func (r *Recorder) ObserveReadAmplification(bucket string, value float64) {
	r.observe("simple_s3_cache_read_amplification", bucketLabels(bucket), value)
}

func (r *Recorder) ObserveUpstreamDuration(bucket, operation string, d time.Duration) {
	r.observe("simple_s3_cache_upstream_duration_seconds", labels(label{"bucket", bucket}, label{"operation", operation}), d.Seconds())
}

func (r *Recorder) ObserveCacheServeDuration(bucket string, d time.Duration) {
	r.observe("simple_s3_cache_hit_duration_seconds", bucketLabels(bucket), d.Seconds())
}

func (r *Recorder) ObserveInternalPeerRequestsPerClientRequest(bucket, strategy string, count int64) {
	r.observe("simple_s3_cache_internal_peer_requests_per_client_request", labels(label{"bucket", bucket}, label{"strategy", strategy}), float64(count))
}

func (r *Recorder) ObservePageBatchSize(bucket, ownerID string, pages int64) {
	r.observe("simple_s3_cache_page_batch_size", pageOwnerLabels(bucket, ownerID), float64(pages))
}

func (r *Recorder) ObserveInternalPeerRequestDuration(bucket, ownerID, statusClass string, d time.Duration) {
	r.observe("simple_s3_cache_internal_peer_request_duration_seconds", pageOwnerStatusLabels(bucket, ownerID, statusClass), d.Seconds())
}

func (r *Recorder) ObserveInvalidationBroadcastDuration(bucket, peerID, status string, d time.Duration) {
	r.observe("simple_s3_cache_invalidation_broadcast_duration_seconds", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"status", status}), d.Seconds())
}

func (r *Recorder) SetCachedBytes(total int64, byBucket map[string]int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cachedBytes = total
	r.cachedByBucket = make(map[string]int64, len(byBucket))
	for bucket, bytes := range byBucket {
		r.cachedByBucket[bucket] = bytes
	}
}

func (r *Recorder) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder
	r.renderCounters(&b)
	r.renderGauges(&b)
	r.renderHistograms(&b)
	return b.String()
}

func (r *Recorder) registerHistogram(name string, buckets []float64) {
	r.histograms[name] = &histogram{
		buckets: buckets,
		series:  map[string]*histogramSeries{},
	}
}

func (r *Recorder) inc(name, labelKey string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.counters[name]; !ok {
		r.counters[name] = map[string]float64{}
	}
	r.counters[name][labelKey] += value
}

func (r *Recorder) observe(name, labelKey string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	h, ok := r.histograms[name]
	if !ok {
		return
	}
	h.observe(labelKey, value)
}

func (h *histogram) observe(labelKey string, value float64) {
	series, ok := h.series[labelKey]
	if !ok {
		series = &histogramSeries{counts: make([]uint64, len(h.buckets)+1)}
		h.series[labelKey] = series
	}
	for i, bucket := range h.buckets {
		if value <= bucket {
			series.counts[i]++
		}
	}
	series.counts[len(series.counts)-1]++
	series.sum += value
	series.count++
}

func (r *Recorder) renderCounters(b *strings.Builder) {
	names := sortedKeys(r.counters)
	for _, name := range names {
		b.WriteString("# TYPE ")
		b.WriteString(name)
		b.WriteString(" counter\n")
		for _, labelKey := range sortedSeries(r.counters[name]) {
			writeMetricLine(b, name, labelKey, r.counters[name][labelKey])
		}
	}
}

func (r *Recorder) renderGauges(b *strings.Builder) {
	b.WriteString("# TYPE simple_s3_cache_cached_bytes gauge\n")
	writeMetricLine(b, "simple_s3_cache_cached_bytes", "", float64(r.cachedBytes))
	for _, bucket := range sortedStringKeys(r.cachedByBucket) {
		writeMetricLine(b, "simple_s3_cache_cached_bytes", bucketLabels(bucket), float64(r.cachedByBucket[bucket]))
	}
	b.WriteString("# TYPE simple_s3_cache_cache_max_bytes gauge\n")
	writeMetricLine(b, "simple_s3_cache_cache_max_bytes", "", float64(r.cacheMaxBytes))
	if r.peerRingInfo != "" {
		b.WriteString("# TYPE simple_s3_cache_peer_ring_info gauge\n")
		writeMetricLine(b, "simple_s3_cache_peer_ring_info", r.peerRingInfo, 1)
	}
	b.WriteString("# TYPE simple_s3_cache_degraded gauge\n")
	if r.degradedReason == "" {
		writeMetricLine(b, "simple_s3_cache_degraded", "", 0)
	} else {
		writeMetricLine(b, "simple_s3_cache_degraded", labels(label{"reason_code", r.degradedReason}), 1)
	}
}

func (r *Recorder) renderHistograms(b *strings.Builder) {
	names := sortedKeys(r.histograms)
	for _, name := range names {
		h := r.histograms[name]
		b.WriteString("# TYPE ")
		b.WriteString(name)
		b.WriteString(" histogram\n")
		for _, labelKey := range sortedHistogramSeries(h.series) {
			series := h.series[labelKey]
			for i, bucket := range h.buckets {
				writeMetricLine(b, name+"_bucket", addLabel(labelKey, "le", formatFloat(bucket)), float64(series.counts[i]))
			}
			writeMetricLine(b, name+"_bucket", addLabel(labelKey, "le", "+Inf"), float64(series.counts[len(series.counts)-1]))
			writeMetricLine(b, name+"_sum", labelKey, series.sum)
			writeMetricLine(b, name+"_count", labelKey, float64(series.count))
		}
	}
}

type label struct {
	name  string
	value string
}

func bucketLabels(bucket string) string {
	return labels(label{"bucket", bucket})
}

func pageOwnerLabels(bucket, ownerID string) string {
	return labels(label{"bucket", bucket}, label{"owner_id", ownerID})
}

func pageOwnerStatusLabels(bucket, ownerID, statusClass string) string {
	return labels(label{"bucket", bucket}, label{"owner_id", ownerID}, label{"status_class", statusClass})
}

func labels(values ...label) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		if value.value == "" {
			continue
		}
		parts = append(parts, value.name+"="+strconv.Quote(value.value))
	}
	return strings.Join(parts, ",")
}

func addLabel(labelKey, name, value string) string {
	extra := name + "=" + strconv.Quote(value)
	if labelKey == "" {
		return extra
	}
	return labelKey + "," + extra
}

func writeMetricLine(b *strings.Builder, name, labelKey string, value float64) {
	b.WriteString(name)
	if labelKey != "" {
		b.WriteString("{")
		b.WriteString(labelKey)
		b.WriteString("}")
	}
	b.WriteString(" ")
	b.WriteString(formatFloat(value))
	b.WriteString("\n")
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStringKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedSeries(m map[string]float64) []string {
	return sortedKeys(m)
}

func sortedHistogramSeries(m map[string]*histogramSeries) []string {
	return sortedKeys(m)
}
