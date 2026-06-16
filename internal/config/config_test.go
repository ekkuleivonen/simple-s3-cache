package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesDefaultsAndParsesSizes(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
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
}

func TestLoadParsesConfiguredValues(t *testing.T) {
	path := writeConfig(t, `
listen: "127.0.0.1:8081"
upstream:
  endpoint: https://s3.example.test
  region: eu-north-1
cache:
  cache_path: /mnt/cache-bytes
  meta_path: /mnt/cache-meta
  max_size: 2GB
  page_size: 512KB
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
	if cfg.Upstream.Region != "eu-north-1" {
		t.Fatalf("Upstream.Region = %q", cfg.Upstream.Region)
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
}

func TestLoadRequiresUpstreamEndpoint(t *testing.T) {
	path := writeConfig(t, `
listen: ":8080"
`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want validation error")
	}
}

func TestLoadRequiresCacheAndMetaPaths(t *testing.T) {
	path := writeConfig(t, `
upstream:
  endpoint: http://rustfs:9000
cache:
  cache_path: ""
  meta_path: ""
`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want validation error")
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

func writeConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	return path
}
