package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsAndParsesSizes(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Listen != ":8080" {
		t.Fatalf("Listen = %q, want :8080", cfg.Listen)
	}
	if cfg.Cache.CachePath != "/cache/objects" {
		t.Fatalf("Cache.CachePath = %q, want /cache/objects", cfg.Cache.CachePath)
	}
	if cfg.Cache.MetaPath != "/cache/meta" {
		t.Fatalf("Cache.MetaPath = %q, want /cache/meta", cfg.Cache.MetaPath)
	}
	if cfg.Upstream.Region != "us-east-1" {
		t.Fatalf("Upstream.Region = %q, want us-east-1", cfg.Upstream.Region)
	}
	if cfg.Cache.PageSize != 4<<20 {
		t.Fatalf("Cache.PageSize = %d, want %d", cfg.Cache.PageSize, 4<<20)
	}
	if cfg.Cache.MaxSize != 1<<40 {
		t.Fatalf("Cache.MaxSize = %d, want %d", cfg.Cache.MaxSize, int64(1<<40))
	}
	if cfg.Cache.MetadataGCInterval != time.Hour {
		t.Fatalf("Cache.MetadataGCInterval = %s, want 1h", cfg.Cache.MetadataGCInterval)
	}
	if cfg.Cache.MetadataMaxAge != 24*time.Hour {
		t.Fatalf("Cache.MetadataMaxAge = %s, want 24h", cfg.Cache.MetadataMaxAge)
	}
	if cfg.Cache.MetadataGCBatchSize != 512 {
		t.Fatalf("Cache.MetadataGCBatchSize = %d, want 512", cfg.Cache.MetadataGCBatchSize)
	}
	if cfg.Cache.SQLiteCheckpointInterval != 6*time.Hour {
		t.Fatalf("Cache.SQLiteCheckpointInterval = %s, want 6h", cfg.Cache.SQLiteCheckpointInterval)
	}
	if cfg.Upstream.ResponseHeaderTimeout != 30*time.Second {
		t.Fatalf("Upstream.ResponseHeaderTimeout = %s, want 30s", cfg.Upstream.ResponseHeaderTimeout)
	}
	if cfg.HTTP.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("HTTP.ReadHeaderTimeout = %s, want 5s", cfg.HTTP.ReadHeaderTimeout)
	}
	if cfg.HTTP.ReadTimeout != 10*time.Minute {
		t.Fatalf("HTTP.ReadTimeout = %s, want 10m", cfg.HTTP.ReadTimeout)
	}
	if cfg.HTTP.WriteTimeout != 10*time.Minute {
		t.Fatalf("HTTP.WriteTimeout = %s, want 10m", cfg.HTTP.WriteTimeout)
	}
	if cfg.HTTP.IdleTimeout != 2*time.Minute {
		t.Fatalf("HTTP.IdleTimeout = %s, want 2m", cfg.HTTP.IdleTimeout)
	}
	if cfg.Upload.SpoolPath != "" {
		t.Fatalf("Upload.SpoolPath = %q, want empty default", cfg.Upload.SpoolPath)
	}
	if cfg.Upload.MaxSpoolSize != 10<<30 {
		t.Fatalf("Upload.MaxSpoolSize = %d, want %d", cfg.Upload.MaxSpoolSize, int64(10<<30))
	}
	if cfg.Peer.Mode != "single" {
		t.Fatalf("Peer.Mode = %q, want single", cfg.Peer.Mode)
	}
	if cfg.Peer.ForwardTimeout != 10*time.Minute {
		t.Fatalf("Peer.ForwardTimeout = %s, want 10m", cfg.Peer.ForwardTimeout)
	}
	if cfg.Peer.ReadSharding != "auto" {
		t.Fatalf("Peer.ReadSharding = %q, want auto", cfg.Peer.ReadSharding)
	}
	if cfg.Peer.PageShardingMinPages != 2 {
		t.Fatalf("Peer.PageShardingMinPages = %d, want 2", cfg.Peer.PageShardingMinPages)
	}
	if cfg.Peer.MaxFillConcurrency != 32 {
		t.Fatalf("Peer.MaxFillConcurrency = %d, want 32", cfg.Peer.MaxFillConcurrency)
	}
	if cfg.Peer.MaxObjectFillConcurrency != 4 {
		t.Fatalf("Peer.MaxObjectFillConcurrency = %d, want 4", cfg.Peer.MaxObjectFillConcurrency)
	}
	if !cfg.Logging.AccessLog {
		t.Fatal("Logging.AccessLog = false, want true")
	}
	if cfg.Logging.InternalPeerAccessLog {
		t.Fatal("Logging.InternalPeerAccessLog = true, want false")
	}
	if cfg.Logging.InternalPeerSuccessLog {
		t.Fatal("Logging.InternalPeerSuccessLog = true, want false")
	}
	if cfg.Operator.Enabled {
		t.Fatal("Operator.Enabled = true, want false")
	}
	if cfg.Operator.Path != "/debug/peer" {
		t.Fatalf("Operator.Path = %q, want /debug/peer", cfg.Operator.Path)
	}
}

func TestLoadSingleModeDoesNotRequirePeerConfig(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: single
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Peer.Mode != "single" {
		t.Fatalf("Peer.Mode = %q, want single", cfg.Peer.Mode)
	}
	if cfg.Peer.LocalID != "" {
		t.Fatalf("Peer.LocalID = %q, want empty", cfg.Peer.LocalID)
	}
	if len(cfg.Peer.Peers) != 0 {
		t.Fatalf("len(Peer.Peers) = %d, want 0", len(cfg.Peer.Peers))
	}
}

func TestLoadParsesConfiguredValues(t *testing.T) {
	path := writeConfig(t, `
listen: "127.0.0.1:8081"
upstream:
  endpoint: https://s3.example.test
  host: 192.168.30.216:9000
  region: eu-north-1
  access_key: configured-access
  secret_key: configured-secret
  session_token: configured-token
  response_header_timeout: 45s
cache:
  cache_path: /mnt/cache-bytes
  meta_path: /mnt/cache-meta
  max_size: 2GB
  page_size: 512KB
  metadata_gc_interval: 30m
  metadata_max_age: 12h
  metadata_gc_batch_size: 128
  sqlite_checkpoint_interval: 2h
http:
  read_header_timeout: 3s
  read_timeout: 2m
  write_timeout: 4m
  idle_timeout: 90s
upload:
  spool_path: /mnt/cache-spool
  max_spool_size: 5GB
peer:
  mode: peer
  local_id: cache-0
  auth_secret: configured-peer-secret
  read_sharding: page
  page_sharding_min_pages: 4
  max_peer_fill_concurrency: 16
  max_peer_object_fill_concurrency: 3
  forward_timeout: 2m
  peers:
    - id: cache-0
      url: http://cache-0.cache-peers:8080
    - id: cache-1
      url: http://cache-1.cache-peers:8080
logging:
  access_log: false
  internal_peer_access_log: true
  internal_peer_success_log: true
operator:
  enabled: true
  path: /debug/cache-peer
  bearer_token: configured-token
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Listen != "127.0.0.1:8081" {
		t.Fatalf("Listen = %q", cfg.Listen)
	}
	if cfg.Upstream.Endpoint != "https://s3.example.test" {
		t.Fatalf("Upstream.Endpoint = %q", cfg.Upstream.Endpoint)
	}
	if cfg.Upstream.Host != "192.168.30.216:9000" {
		t.Fatalf("Upstream.Host = %q", cfg.Upstream.Host)
	}
	if cfg.Upstream.Region != "eu-north-1" {
		t.Fatalf("Upstream.Region = %q", cfg.Upstream.Region)
	}
	if cfg.Upstream.AccessKey != "configured-access" {
		t.Fatalf("Upstream.AccessKey = %q", cfg.Upstream.AccessKey)
	}
	if cfg.Upstream.SecretKey != "configured-secret" {
		t.Fatalf("Upstream.SecretKey = %q", cfg.Upstream.SecretKey)
	}
	if cfg.Upstream.SessionToken != "configured-token" {
		t.Fatalf("Upstream.SessionToken = %q", cfg.Upstream.SessionToken)
	}
	if cfg.Cache.CachePath != "/mnt/cache-bytes" {
		t.Fatalf("Cache.CachePath = %q", cfg.Cache.CachePath)
	}
	if cfg.Cache.MetaPath != "/mnt/cache-meta" {
		t.Fatalf("Cache.MetaPath = %q", cfg.Cache.MetaPath)
	}
	if cfg.Cache.MaxSize != 2<<30 {
		t.Fatalf("Cache.MaxSize = %d, want %d", cfg.Cache.MaxSize, int64(2<<30))
	}
	if cfg.Cache.PageSize != 512<<10 {
		t.Fatalf("Cache.PageSize = %d, want %d", cfg.Cache.PageSize, int64(512<<10))
	}
	if cfg.Cache.MetadataGCInterval != 30*time.Minute {
		t.Fatalf("Cache.MetadataGCInterval = %s, want 30m", cfg.Cache.MetadataGCInterval)
	}
	if cfg.Cache.MetadataMaxAge != 12*time.Hour {
		t.Fatalf("Cache.MetadataMaxAge = %s, want 12h", cfg.Cache.MetadataMaxAge)
	}
	if cfg.Cache.MetadataGCBatchSize != 128 {
		t.Fatalf("Cache.MetadataGCBatchSize = %d, want 128", cfg.Cache.MetadataGCBatchSize)
	}
	if cfg.Cache.SQLiteCheckpointInterval != 2*time.Hour {
		t.Fatalf("Cache.SQLiteCheckpointInterval = %s, want 2h", cfg.Cache.SQLiteCheckpointInterval)
	}
	if cfg.Upstream.ResponseHeaderTimeout != 45*time.Second {
		t.Fatalf("Upstream.ResponseHeaderTimeout = %s, want 45s", cfg.Upstream.ResponseHeaderTimeout)
	}
	if cfg.HTTP.ReadHeaderTimeout != 3*time.Second {
		t.Fatalf("HTTP.ReadHeaderTimeout = %s, want 3s", cfg.HTTP.ReadHeaderTimeout)
	}
	if cfg.HTTP.ReadTimeout != 2*time.Minute {
		t.Fatalf("HTTP.ReadTimeout = %s, want 2m", cfg.HTTP.ReadTimeout)
	}
	if cfg.HTTP.WriteTimeout != 4*time.Minute {
		t.Fatalf("HTTP.WriteTimeout = %s, want 4m", cfg.HTTP.WriteTimeout)
	}
	if cfg.HTTP.IdleTimeout != 90*time.Second {
		t.Fatalf("HTTP.IdleTimeout = %s, want 90s", cfg.HTTP.IdleTimeout)
	}
	if cfg.Upload.SpoolPath != "/mnt/cache-spool" {
		t.Fatalf("Upload.SpoolPath = %q", cfg.Upload.SpoolPath)
	}
	if cfg.Upload.MaxSpoolSize != 5<<30 {
		t.Fatalf("Upload.MaxSpoolSize = %d, want %d", cfg.Upload.MaxSpoolSize, int64(5<<30))
	}
	if cfg.Peer.Mode != "peer" {
		t.Fatalf("Peer.Mode = %q", cfg.Peer.Mode)
	}
	if cfg.Peer.LocalID != "cache-0" {
		t.Fatalf("Peer.LocalID = %q", cfg.Peer.LocalID)
	}
	if cfg.Peer.AuthSecret != "configured-peer-secret" {
		t.Fatalf("Peer.AuthSecret = %q", cfg.Peer.AuthSecret)
	}
	if cfg.Peer.ForwardTimeout != 2*time.Minute {
		t.Fatalf("Peer.ForwardTimeout = %s", cfg.Peer.ForwardTimeout)
	}
	if cfg.Peer.ReadSharding != "page" {
		t.Fatalf("Peer.ReadSharding = %q, want page", cfg.Peer.ReadSharding)
	}
	if cfg.Peer.PageShardingMinPages != 4 {
		t.Fatalf("Peer.PageShardingMinPages = %d, want 4", cfg.Peer.PageShardingMinPages)
	}
	if cfg.Peer.MaxFillConcurrency != 16 {
		t.Fatalf("Peer.MaxFillConcurrency = %d, want 16", cfg.Peer.MaxFillConcurrency)
	}
	if cfg.Peer.MaxObjectFillConcurrency != 3 {
		t.Fatalf("Peer.MaxObjectFillConcurrency = %d, want 3", cfg.Peer.MaxObjectFillConcurrency)
	}
	if len(cfg.Peer.Peers) != 2 {
		t.Fatalf("len(Peer.Peers) = %d, want 2", len(cfg.Peer.Peers))
	}
	if cfg.Logging.AccessLog {
		t.Fatal("Logging.AccessLog = true, want false")
	}
	if !cfg.Logging.InternalPeerAccessLog {
		t.Fatal("Logging.InternalPeerAccessLog = false, want true")
	}
	if !cfg.Logging.InternalPeerSuccessLog {
		t.Fatal("Logging.InternalPeerSuccessLog = false, want true")
	}
	if !cfg.Operator.Enabled {
		t.Fatal("Operator.Enabled = false, want true")
	}
	if cfg.Operator.Path != "/debug/cache-peer" {
		t.Fatalf("Operator.Path = %q", cfg.Operator.Path)
	}
	if cfg.Operator.BearerToken != "configured-token" {
		t.Fatalf("Operator.BearerToken = %q", cfg.Operator.BearerToken)
	}
}

func TestLoadParsesBucketCacheOverrides(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
cache:
  max_size: 1GB
  page_size: 4MB
  buckets:
    analytics:
      max_size: 128MB
      page_size: 512KB
    media:
      max_size: 768MB
      page_size: 16MB
    logs:
      page_size: 1MB
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	analytics := cfg.Cache.Buckets["analytics"]
	if analytics.MaxSize != 128<<20 {
		t.Fatalf("analytics MaxSize = %d, want %d", analytics.MaxSize, int64(128<<20))
	}
	if analytics.PageSize != 512<<10 {
		t.Fatalf("analytics PageSize = %d, want %d", analytics.PageSize, int64(512<<10))
	}

	media := cfg.Cache.Buckets["media"]
	if media.MaxSize != 768<<20 {
		t.Fatalf("media MaxSize = %d, want %d", media.MaxSize, int64(768<<20))
	}
	if media.PageSize != 16<<20 {
		t.Fatalf("media PageSize = %d, want %d", media.PageSize, int64(16<<20))
	}

	logs := cfg.Cache.Buckets["logs"]
	if logs.MaxSize != 0 {
		t.Fatalf("logs MaxSize = %d, want unset bucket-specific cap", logs.MaxSize)
	}
	if logs.PageSize != 1<<20 {
		t.Fatalf("logs PageSize = %d, want %d", logs.PageSize, int64(1<<20))
	}
}

func TestLoadRejectsInvalidBucketCacheOverrideSizes(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		wantError string
	}{
		{
			name: "invalid bucket max size",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
cache:
  buckets:
    analytics:
      max_size: not-a-size
`,
			wantError: "cache.buckets.analytics.max_size",
		},
		{
			name: "invalid bucket page size",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
cache:
  buckets:
    analytics:
      page_size: nope
`,
			wantError: "cache.buckets.analytics.page_size",
		},
		{
			name: "bucket page size exceeds explicit bucket max size",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
cache:
  buckets:
    analytics:
      max_size: 1MB
      page_size: 2MB
`,
			wantError: "cache.buckets.analytics.page_size must not exceed cache.buckets.analytics.max_size",
		},
		{
			name: "bucket page size exceeds global max size",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
cache:
  max_size: 1MB
  buckets:
    analytics:
      page_size: 2MB
`,
			wantError: "cache.buckets.analytics.page_size must not exceed cache.max_size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, tt.config)

			_, err := Load(path)
			if err == nil {
				t.Fatal("Load() error = nil, want validation error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Load() error = %v, want containing %q", err, tt.wantError)
			}
		})
	}
}

func TestLoadRequiresUpstreamEndpoint(t *testing.T) {
	path := writeConfig(t, `
listen: ":8080"
upstream:
  access_key: test-access
  secret_key: test-secret
`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want validation error")
	}
}

func TestLoadRequiresCacheAndMetaPaths(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
cache:
  cache_path: ""
  meta_path: ""
`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want validation error")
	}
}

func TestLoadRequiresUpstreamCredentials(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want credential validation error")
	}
}

func TestLoadRejectsInvalidUpstreamHost(t *testing.T) {
	tests := []string{
		"http://rustfs.example.test",
		"rustfs.example.test/bucket",
		"rustfs example.test",
	}

	for _, host := range tests {
		t.Run(host, func(t *testing.T) {
			path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
  host: `+host+`
  access_key: test-access
  secret_key: test-secret
`)

			_, err := Load(path)
			if err == nil {
				t.Fatal("Load() error = nil, want upstream.host validation error")
			}
			if !strings.Contains(err.Error(), "upstream.host") {
				t.Fatalf("Load() error = %v, want containing upstream.host", err)
			}
		})
	}
}

func TestLoadRequiresValidCacheMaintenanceConfig(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
cache:
  metadata_gc_batch_size: 0
`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want maintenance validation error")
	}
}

func TestLoadRejectsInvalidPeerConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		wantError string
	}{
		{
			name: "unknown mode",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: cluster
`,
			wantError: "peer.mode",
		},
		{
			name: "peer mode missing local id",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: peer
  peers:
    - id: cache-0
      url: http://cache-0:8080
`,
			wantError: "peer.local_id",
		},
		{
			name: "local id absent from peers",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: peer
  local_id: cache-0
  peers:
    - id: cache-1
      url: http://cache-1:8080
`,
			wantError: "peer.peers must include peer.local_id",
		},
		{
			name: "peer mode missing auth secret",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: peer
  local_id: cache-0
  peers:
    - id: cache-0
      url: http://cache-0:8080
`,
			wantError: "peer.auth_secret",
		},
		{
			name: "duplicate peer id",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: peer
  local_id: cache-0
  peers:
    - id: cache-0
      url: http://cache-0:8080
    - id: cache-0
      url: http://cache-0-alt:8080
`,
			wantError: "duplicated",
		},
		{
			name: "invalid peer url",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: peer
  local_id: cache-0
  peers:
    - id: cache-0
      url: ftp://cache-0:8080
`,
			wantError: "must use http or https",
		},
		{
			name: "invalid read sharding",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: peer
  local_id: cache-0
  read_sharding: random
  peers:
    - id: cache-0
      url: http://cache-0:8080
`,
			wantError: "peer.read_sharding",
		},
		{
			name: "invalid page sharding threshold",
			config: `
upstream:
  endpoint: http://rustfs:9000
  access_key: test-access
  secret_key: test-secret
peer:
  mode: peer
  local_id: cache-0
  page_sharding_min_pages: 0
  peers:
    - id: cache-0
      url: http://cache-0:8080
`,
			wantError: "peer.page_sharding_min_pages",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, tt.config)

			_, err := Load(path)
			if err == nil {
				t.Fatal("Load() error = nil, want validation error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Load() error = %v, want containing %q", err, tt.wantError)
			}
		})
	}
}

func TestParseBytes(t *testing.T) {
	tests := map[string]int64{
		"1":     1,
		"4MB":   4 << 20,
		"4 MiB": 4 << 20,
		"1TB":   1 << 40,
	}

	for input, want := range tests {
		got, err := ParseBytes(input)
		if err != nil {
			t.Fatalf("ParseBytes(%q) error = %v", input, err)
		}
		if got != want {
			t.Fatalf("ParseBytes(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := map[string]time.Duration{
		"1s":   time.Second,
		"2m":   2 * time.Minute,
		"1h":   time.Hour,
		"50ms": 50 * time.Millisecond,
	}

	for input, want := range tests {
		got, err := ParseDuration(input)
		if err != nil {
			t.Fatalf("ParseDuration(%q) error = %v", input, err)
		}
		if got != want {
			t.Fatalf("ParseDuration(%q) = %s, want %s", input, got, want)
		}
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	return path
}
