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
	r.registerHistogram("simple_s3_cache_cache_metadata_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1})
	r.registerHistogram("simple_s3_cache_cache_page_open_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1})
	r.registerHistogram("simple_s3_cache_cache_response_copy_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1})
	r.registerHistogram("simple_s3_cache_peer_forward_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	r.registerHistogram("simple_s3_cache_peer_response_header_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	r.registerHistogram("simple_s3_cache_peer_response_copy_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	r.registerHistogram("simple_s3_cache_peer_response_body_read_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	r.registerHistogram("simple_s3_cache_peer_downstream_write_duration_seconds", []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
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

func (r *Recorder) RecordPeerDecision(bucket, decision, ownerID string) {
	r.inc("simple_s3_cache_peer_owner_decisions_total", labels(label{"bucket", bucket}, label{"decision", decision}, label{"owner_id", ownerID}), 1)
}

func (r *Recorder) RecordPeerForward(bucket, peerID, method, statusClass string) {
	r.inc("simple_s3_cache_peer_forwarded_requests_total", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"method", method}, label{"status_class", statusClass}), 1)
}

func (r *Recorder) RecordPeerForwardFailure(bucket, peerID, reason string) {
	r.inc("simple_s3_cache_peer_forward_failures_total", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"reason", reason}), 1)
}

func (r *Recorder) RecordPeerForwardResponseBytes(bucket, peerID string, bytes int64) {
	r.inc("simple_s3_cache_peer_forward_response_bytes_total", labels(label{"bucket", bucket}, label{"peer_id", peerID}), float64(bytes))
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

func (r *Recorder) ObserveCacheMetadataDuration(bucket, cacheResult string, d time.Duration) {
	r.observe("simple_s3_cache_cache_metadata_duration_seconds", cacheResultLabels(bucket, cacheResult), d.Seconds())
}

func (r *Recorder) ObserveCachePageOpenDuration(bucket, cacheResult string, d time.Duration) {
	r.observe("simple_s3_cache_cache_page_open_duration_seconds", cacheResultLabels(bucket, cacheResult), d.Seconds())
}

func (r *Recorder) ObserveCacheResponseCopyDuration(bucket, cacheResult string, d time.Duration) {
	r.observe("simple_s3_cache_cache_response_copy_duration_seconds", cacheResultLabels(bucket, cacheResult), d.Seconds())
}

func (r *Recorder) ObservePeerForwardDuration(bucket, peerID, statusClass string, d time.Duration) {
	r.observe("simple_s3_cache_peer_forward_duration_seconds", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"status_class", statusClass}), d.Seconds())
}

func (r *Recorder) ObservePeerResponseHeaderDuration(bucket, peerID, statusClass string, d time.Duration) {
	r.observe("simple_s3_cache_peer_response_header_duration_seconds", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"status_class", statusClass}), d.Seconds())
}

func (r *Recorder) ObservePeerResponseCopyDuration(bucket, peerID, statusClass string, d time.Duration) {
	r.observe("simple_s3_cache_peer_response_copy_duration_seconds", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"status_class", statusClass}), d.Seconds())
}

func (r *Recorder) ObservePeerResponseBodyReadDuration(bucket, peerID, statusClass string, d time.Duration) {
	r.observe("simple_s3_cache_peer_response_body_read_duration_seconds", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"status_class", statusClass}), d.Seconds())
}

func (r *Recorder) ObservePeerDownstreamWriteDuration(bucket, peerID, statusClass string, d time.Duration) {
	r.observe("simple_s3_cache_peer_downstream_write_duration_seconds", labels(label{"bucket", bucket}, label{"peer_id", peerID}, label{"status_class", statusClass}), d.Seconds())
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
	r.counters[name][""] += value
	if labelKey != "" {
		r.counters[name][labelKey] += value
	}
}

func (r *Recorder) observe(name, labelKey string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	h, ok := r.histograms[name]
	if !ok {
		return
	}
	h.observe("", value)
	if labelKey != "" {
		h.observe(labelKey, value)
	}
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

func cacheResultLabels(bucket, cacheResult string) string {
	return labels(label{"bucket", bucket}, label{"cache_result", cacheResult})
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
