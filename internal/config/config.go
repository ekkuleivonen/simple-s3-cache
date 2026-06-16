package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen   = ":8080"
	defaultCacheDir = "/cache"
	defaultMaxSize  = int64(1 << 40) // 1 TiB
	defaultPageSize = int64(4 << 20) // 4 MiB
)

// Config is the process configuration loaded from YAML.
type Config struct {
	Listen   string         `yaml:"listen"`
	Upstream UpstreamConfig `yaml:"upstream"`
	Cache    CacheConfig    `yaml:"cache"`
}

type UpstreamConfig struct {
	Endpoint string `yaml:"endpoint"`
}

type CacheConfig struct {
	Path     string `yaml:"path"`
	MaxSize  int64  `yaml:"-"`
	PageSize int64  `yaml:"-"`

	MaxSizeText  string `yaml:"max_size"`
	PageSizeText string `yaml:"page_size"`
}

// Load reads a YAML config file, applies defaults, and validates required fields.
func Load(path string) (Config, error) {
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
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func Default() Config {
	return Config{
		Listen: defaultListen,
		Cache: CacheConfig{
			Path:     defaultCacheDir,
			MaxSize:  defaultMaxSize,
			PageSize: defaultPageSize,
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

	if strings.TrimSpace(cfg.Cache.Path) == "" {
		errs = append(errs, errors.New("cache.path is required"))
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

	return errors.Join(errs...)
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
