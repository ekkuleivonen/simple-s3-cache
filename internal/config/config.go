package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen                        = ":8080"
	defaultCachePath                     = "/cache/objects"
	defaultMetaPath                      = "/cache/meta"
	defaultMaxSize                       = int64(1 << 40) // 1 TiB
	defaultPageSize                      = int64(4 << 20) // 4 MiB
	defaultMetadataGCInterval            = time.Hour
	defaultMetadataMaxAge                = 24 * time.Hour
	defaultMetadataGCBatchSize           = 512
	defaultSQLiteCheckpointInterval      = 6 * time.Hour
	defaultRegion                        = "us-east-1"
	defaultUpstreamResponseHeaderTimeout = 30 * time.Second
	defaultReadHeaderTimeout             = 5 * time.Second
	defaultReadTimeout                   = 10 * time.Minute
	defaultWriteTimeout                  = 10 * time.Minute
	defaultIdleTimeout                   = 2 * time.Minute
	defaultMaxSpoolSize                  = int64(10 << 30) // 10 GiB
	defaultPeerMode                      = "single"
	defaultPeerForwardTimeout            = 10 * time.Minute
)

// Config is the process configuration loaded from YAML.
type Config struct {
	Listen   string         `yaml:"listen"`
	Upstream UpstreamConfig `yaml:"upstream"`
	Cache    CacheConfig    `yaml:"cache"`
	HTTP     HTTPConfig     `yaml:"http"`
	Upload   UploadConfig   `yaml:"upload"`
	Peer     PeerConfig     `yaml:"peer"`
}

type UpstreamConfig struct {
	Endpoint                  string        `yaml:"endpoint"`
	Host                      string        `yaml:"host"`
	Region                    string        `yaml:"region"`
	AccessKey                 string        `yaml:"access_key"`
	SecretKey                 string        `yaml:"secret_key"`
	SessionToken              string        `yaml:"session_token"`
	ResponseHeaderTimeout     time.Duration `yaml:"-"`
	ResponseHeaderTimeoutText string        `yaml:"response_header_timeout"`
}

type CacheConfig struct {
	CachePath                string                       `yaml:"cache_path"`
	MetaPath                 string                       `yaml:"meta_path"`
	MaxSize                  int64                        `yaml:"-"`
	PageSize                 int64                        `yaml:"-"`
	Buckets                  map[string]BucketCacheConfig `yaml:"buckets"`
	MetadataGCInterval       time.Duration                `yaml:"-"`
	MetadataMaxAge           time.Duration                `yaml:"-"`
	MetadataGCBatchSize      int                          `yaml:"metadata_gc_batch_size"`
	SQLiteCheckpointInterval time.Duration                `yaml:"-"`

	MaxSizeText                  string `yaml:"max_size"`
	PageSizeText                 string `yaml:"page_size"`
	MetadataGCIntervalText       string `yaml:"metadata_gc_interval"`
	MetadataMaxAgeText           string `yaml:"metadata_max_age"`
	SQLiteCheckpointIntervalText string `yaml:"sqlite_checkpoint_interval"`
}

type BucketCacheConfig struct {
	MaxSize      int64  `yaml:"-"`
	PageSize     int64  `yaml:"-"`
	MaxSizeText  string `yaml:"max_size"`
	PageSizeText string `yaml:"page_size"`
}

type HTTPConfig struct {
	ReadHeaderTimeout     time.Duration `yaml:"-"`
	ReadTimeout           time.Duration `yaml:"-"`
	WriteTimeout          time.Duration `yaml:"-"`
	IdleTimeout           time.Duration `yaml:"-"`
	ReadHeaderTimeoutText string        `yaml:"read_header_timeout"`
	ReadTimeoutText       string        `yaml:"read_timeout"`
	WriteTimeoutText      string        `yaml:"write_timeout"`
	IdleTimeoutText       string        `yaml:"idle_timeout"`
}

type UploadConfig struct {
	SpoolPath        string `yaml:"spool_path"`
	MaxSpoolSize     int64  `yaml:"-"`
	MaxSpoolSizeText string `yaml:"max_spool_size"`
}

type PeerConfig struct {
	Mode               string        `yaml:"mode"`
	LocalID            string        `yaml:"local_id"`
	Peers              []Peer        `yaml:"peers"`
	ForwardTimeout     time.Duration `yaml:"-"`
	ForwardTimeoutText string        `yaml:"forward_timeout"`
}

type Peer struct {
	ID  string `yaml:"id"`
	URL string `yaml:"url"`
}

// Load reads a YAML config file, applies defaults, and validates required fields.
func Load(path string) (Config, error) {
	cfg, err := readConfig(path)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// LoadGateway reads a YAML config file for the owner-aware gateway.
func LoadGateway(path string) (Config, error) {
	cfg, err := readConfig(path)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.ValidateGateway(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func readConfig(path string) (Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	if err := cfg.applyParsedSizes(); err != nil {
		return Config{}, err
	}
	if err := cfg.applyParsedDurations(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func Default() Config {
	return Config{
		Listen: defaultListen,
		Upstream: UpstreamConfig{
			Region:                defaultRegion,
			ResponseHeaderTimeout: defaultUpstreamResponseHeaderTimeout,
		},
		Cache: CacheConfig{
			CachePath:                defaultCachePath,
			MetaPath:                 defaultMetaPath,
			MaxSize:                  defaultMaxSize,
			PageSize:                 defaultPageSize,
			MetadataGCInterval:       defaultMetadataGCInterval,
			MetadataMaxAge:           defaultMetadataMaxAge,
			MetadataGCBatchSize:      defaultMetadataGCBatchSize,
			SQLiteCheckpointInterval: defaultSQLiteCheckpointInterval,
		},
		HTTP: HTTPConfig{
			ReadHeaderTimeout: defaultReadHeaderTimeout,
			ReadTimeout:       defaultReadTimeout,
			WriteTimeout:      defaultWriteTimeout,
			IdleTimeout:       defaultIdleTimeout,
		},
		Upload: UploadConfig{
			MaxSpoolSize: defaultMaxSpoolSize,
		},
		Peer: PeerConfig{
			Mode:           defaultPeerMode,
			ForwardTimeout: defaultPeerForwardTimeout,
		},
	}
}

func (cfg *Config) applyParsedSizes() error {
	if cfg.Cache.MaxSizeText != "" {
		size, err := ParseBytes(cfg.Cache.MaxSizeText)
		if err != nil {
			return fmt.Errorf("cache.max_size: %w", err)
		}
		cfg.Cache.MaxSize = size
	}

	if cfg.Cache.PageSizeText != "" {
		size, err := ParseBytes(cfg.Cache.PageSizeText)
		if err != nil {
			return fmt.Errorf("cache.page_size: %w", err)
		}
		cfg.Cache.PageSize = size
	}
	if cfg.Upload.MaxSpoolSizeText != "" {
		size, err := ParseBytes(cfg.Upload.MaxSpoolSizeText)
		if err != nil {
			return fmt.Errorf("upload.max_spool_size: %w", err)
		}
		cfg.Upload.MaxSpoolSize = size
	}
	for name, bucket := range cfg.Cache.Buckets {
		if bucket.MaxSizeText != "" {
			size, err := ParseBytes(bucket.MaxSizeText)
			if err != nil {
				return fmt.Errorf("cache.buckets.%s.max_size: %w", name, err)
			}
			bucket.MaxSize = size
		}
		if bucket.PageSizeText != "" {
			size, err := ParseBytes(bucket.PageSizeText)
			if err != nil {
				return fmt.Errorf("cache.buckets.%s.page_size: %w", name, err)
			}
			bucket.PageSize = size
		}
		cfg.Cache.Buckets[name] = bucket
	}

	return nil
}

func (cfg *Config) applyParsedDurations() error {
	durationFields := []struct {
		name  string
		text  string
		value *time.Duration
	}{
		{"upstream.response_header_timeout", cfg.Upstream.ResponseHeaderTimeoutText, &cfg.Upstream.ResponseHeaderTimeout},
		{"cache.metadata_gc_interval", cfg.Cache.MetadataGCIntervalText, &cfg.Cache.MetadataGCInterval},
		{"cache.metadata_max_age", cfg.Cache.MetadataMaxAgeText, &cfg.Cache.MetadataMaxAge},
		{"cache.sqlite_checkpoint_interval", cfg.Cache.SQLiteCheckpointIntervalText, &cfg.Cache.SQLiteCheckpointInterval},
		{"http.read_header_timeout", cfg.HTTP.ReadHeaderTimeoutText, &cfg.HTTP.ReadHeaderTimeout},
		{"http.read_timeout", cfg.HTTP.ReadTimeoutText, &cfg.HTTP.ReadTimeout},
		{"http.write_timeout", cfg.HTTP.WriteTimeoutText, &cfg.HTTP.WriteTimeout},
		{"http.idle_timeout", cfg.HTTP.IdleTimeoutText, &cfg.HTTP.IdleTimeout},
		{"peer.forward_timeout", cfg.Peer.ForwardTimeoutText, &cfg.Peer.ForwardTimeout},
	}
	for _, field := range durationFields {
		if strings.TrimSpace(field.text) == "" {
			continue
		}
		duration, err := ParseDuration(field.text)
		if err != nil {
			return fmt.Errorf("%s: %w", field.name, err)
		}
		*field.value = duration
	}
	return nil
}

func (cfg Config) Validate() error {
	var errs []error

	if strings.TrimSpace(cfg.Listen) == "" {
		errs = append(errs, errors.New("listen is required"))
	} else if _, _, err := net.SplitHostPort(cfg.Listen); err != nil {
		errs = append(errs, fmt.Errorf("listen must be host:port or :port: %w", err))
	}

	endpoint := strings.TrimSpace(cfg.Upstream.Endpoint)
	if endpoint == "" {
		errs = append(errs, errors.New("upstream.endpoint is required"))
	} else if parsed, err := url.Parse(endpoint); err != nil {
		errs = append(errs, fmt.Errorf("upstream.endpoint is invalid: %w", err))
	} else if parsed.Scheme != "http" && parsed.Scheme != "https" {
		errs = append(errs, errors.New("upstream.endpoint must use http or https"))
	} else if parsed.Host == "" {
		errs = append(errs, errors.New("upstream.endpoint must include a host"))
	}
	if host := strings.TrimSpace(cfg.Upstream.Host); host != "" {
		if parsed, err := url.Parse("//" + host); err != nil {
			errs = append(errs, fmt.Errorf("upstream.host is invalid: %w", err))
		} else if parsed.Host != host || parsed.Hostname() == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || strings.ContainsAny(host, " \t\r\n") {
			errs = append(errs, errors.New("upstream.host must be a host or host:port without scheme or path"))
		}
	}
	if strings.TrimSpace(cfg.Upstream.Region) == "" {
		errs = append(errs, errors.New("upstream.region is required"))
	}
	if strings.TrimSpace(cfg.Upstream.AccessKey) == "" {
		errs = append(errs, errors.New("upstream.access_key is required"))
	}
	if strings.TrimSpace(cfg.Upstream.SecretKey) == "" {
		errs = append(errs, errors.New("upstream.secret_key is required"))
	}
	if cfg.Upstream.ResponseHeaderTimeout <= 0 {
		errs = append(errs, errors.New("upstream.response_header_timeout must be greater than zero"))
	}

	if strings.TrimSpace(cfg.Cache.CachePath) == "" {
		errs = append(errs, errors.New("cache.cache_path is required"))
	}
	if strings.TrimSpace(cfg.Cache.MetaPath) == "" {
		errs = append(errs, errors.New("cache.meta_path is required"))
	}
	if cfg.Cache.MaxSize <= 0 {
		errs = append(errs, errors.New("cache.max_size must be greater than zero"))
	}
	if cfg.Cache.PageSize <= 0 {
		errs = append(errs, errors.New("cache.page_size must be greater than zero"))
	}
	if cfg.Cache.MaxSize > 0 && cfg.Cache.PageSize > cfg.Cache.MaxSize {
		errs = append(errs, errors.New("cache.page_size must not exceed cache.max_size"))
	}
	for name, bucket := range cfg.Cache.Buckets {
		if strings.TrimSpace(name) == "" {
			errs = append(errs, errors.New("cache.buckets bucket name must not be empty"))
			continue
		}
		if bucket.MaxSize < 0 {
			errs = append(errs, fmt.Errorf("cache.buckets.%s.max_size must not be negative", name))
		}
		if bucket.PageSize < 0 {
			errs = append(errs, fmt.Errorf("cache.buckets.%s.page_size must not be negative", name))
		}
		if bucket.MaxSize > 0 && bucket.PageSize > bucket.MaxSize {
			errs = append(errs, fmt.Errorf("cache.buckets.%s.page_size must not exceed cache.buckets.%s.max_size", name, name))
		}
		if bucket.MaxSize == 0 && bucket.PageSize > cfg.Cache.MaxSize {
			errs = append(errs, fmt.Errorf("cache.buckets.%s.page_size must not exceed cache.max_size", name))
		}
	}
	if cfg.Cache.MetadataGCInterval <= 0 {
		errs = append(errs, errors.New("cache.metadata_gc_interval must be greater than zero"))
	}
	if cfg.Cache.MetadataMaxAge <= 0 {
		errs = append(errs, errors.New("cache.metadata_max_age must be greater than zero"))
	}
	if cfg.Cache.MetadataGCBatchSize <= 0 {
		errs = append(errs, errors.New("cache.metadata_gc_batch_size must be greater than zero"))
	}
	if cfg.Cache.SQLiteCheckpointInterval <= 0 {
		errs = append(errs, errors.New("cache.sqlite_checkpoint_interval must be greater than zero"))
	}
	if cfg.HTTP.ReadHeaderTimeout <= 0 {
		errs = append(errs, errors.New("http.read_header_timeout must be greater than zero"))
	}
	if cfg.HTTP.ReadTimeout <= 0 {
		errs = append(errs, errors.New("http.read_timeout must be greater than zero"))
	}
	if cfg.HTTP.WriteTimeout <= 0 {
		errs = append(errs, errors.New("http.write_timeout must be greater than zero"))
	}
	if cfg.HTTP.IdleTimeout <= 0 {
		errs = append(errs, errors.New("http.idle_timeout must be greater than zero"))
	}
	if cfg.Upload.MaxSpoolSize <= 0 {
		errs = append(errs, errors.New("upload.max_spool_size must be greater than zero"))
	}
	errs = append(errs, cfg.validatePeer(true)...)

	return errors.Join(errs...)
}

func (cfg Config) ValidateGateway() error {
	var errs []error

	if strings.TrimSpace(cfg.Listen) == "" {
		errs = append(errs, errors.New("listen is required"))
	} else if _, _, err := net.SplitHostPort(cfg.Listen); err != nil {
		errs = append(errs, fmt.Errorf("listen must be host:port or :port: %w", err))
	}
	if cfg.HTTP.ReadHeaderTimeout <= 0 {
		errs = append(errs, errors.New("http.read_header_timeout must be greater than zero"))
	}
	if cfg.HTTP.ReadTimeout <= 0 {
		errs = append(errs, errors.New("http.read_timeout must be greater than zero"))
	}
	if cfg.HTTP.WriteTimeout <= 0 {
		errs = append(errs, errors.New("http.write_timeout must be greater than zero"))
	}
	if cfg.HTTP.IdleTimeout <= 0 {
		errs = append(errs, errors.New("http.idle_timeout must be greater than zero"))
	}
	errs = append(errs, cfg.validatePeer(false)...)

	return errors.Join(errs...)
}

func (cfg Config) validatePeer(requireLocal bool) []error {
	var errs []error
	mode := strings.TrimSpace(cfg.Peer.Mode)
	switch mode {
	case "peer":
	default:
		if requireLocal && (mode == "" || mode == "single") {
			return nil
		}
		return []error{fmt.Errorf("peer.mode must be peer")}
	}

	if requireLocal && strings.TrimSpace(cfg.Peer.LocalID) == "" {
		errs = append(errs, errors.New("peer.local_id is required in peer mode"))
	}
	if cfg.Peer.ForwardTimeout <= 0 {
		errs = append(errs, errors.New("peer.forward_timeout must be greater than zero"))
	}
	if len(cfg.Peer.Peers) == 0 {
		errs = append(errs, errors.New("peer.peers must contain at least one peer in peer mode"))
	}

	seen := map[string]struct{}{}
	hasLocal := false
	for i, peer := range cfg.Peer.Peers {
		id := strings.TrimSpace(peer.ID)
		if id == "" {
			errs = append(errs, fmt.Errorf("peer.peers[%d].id is required", i))
		} else {
			if _, ok := seen[id]; ok {
				errs = append(errs, fmt.Errorf("peer.peers id %q is duplicated", id))
			}
			seen[id] = struct{}{}
			if requireLocal && id == cfg.Peer.LocalID {
				hasLocal = true
			}
		}
		peerURL := strings.TrimSpace(peer.URL)
		if peerURL == "" {
			errs = append(errs, fmt.Errorf("peer.peers[%d].url is required", i))
			continue
		}
		parsed, err := url.Parse(peerURL)
		if err != nil {
			errs = append(errs, fmt.Errorf("peer.peers[%d].url is invalid: %w", i, err))
		} else if parsed.Scheme != "http" && parsed.Scheme != "https" {
			errs = append(errs, fmt.Errorf("peer.peers[%d].url must use http or https", i))
		} else if parsed.Host == "" {
			errs = append(errs, fmt.Errorf("peer.peers[%d].url must include a host", i))
		}
	}
	if requireLocal && len(cfg.Peer.Peers) > 0 && !hasLocal {
		errs = append(errs, errors.New("peer.peers must include peer.local_id"))
	}
	return errs
}

func ParseBytes(input string) (int64, error) {
	value := strings.TrimSpace(input)
	if value == "" {
		return 0, errors.New("size is empty")
	}

	i := 0
	for i < len(value) && (value[i] == '.' || value[i] >= '0' && value[i] <= '9') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid size %q", input)
	}

	number, err := strconv.ParseFloat(value[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", value[:i], err)
	}
	if number <= 0 {
		return 0, errors.New("size must be greater than zero")
	}

	unit := strings.ToUpper(strings.TrimSpace(value[i:]))
	multiplier, ok := map[string]float64{
		"":    1,
		"B":   1,
		"K":   1 << 10,
		"KB":  1 << 10,
		"KIB": 1 << 10,
		"M":   1 << 20,
		"MB":  1 << 20,
		"MIB": 1 << 20,
		"G":   1 << 30,
		"GB":  1 << 30,
		"GIB": 1 << 30,
		"T":   1 << 40,
		"TB":  1 << 40,
		"TIB": 1 << 40,
	}[unit]
	if !ok {
		return 0, fmt.Errorf("unsupported unit %q", unit)
	}

	size := number * multiplier
	if size > float64(^uint64(0)>>1) {
		return 0, errors.New("size is too large")
	}

	return int64(size), nil
}

func ParseDuration(input string) (time.Duration, error) {
	value := strings.TrimSpace(input)
	if value == "" {
		return 0, errors.New("duration is empty")
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", input, err)
	}
	if duration <= 0 {
		return 0, errors.New("duration must be greater than zero")
	}
	return duration, nil
}
