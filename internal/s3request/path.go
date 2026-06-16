package s3request

import (
	"net/url"
	"strings"
)

type Target struct {
	Bucket string
	Key    string
}

func (t Target) IsObject() bool {
	return t.Bucket != "" && t.Key != ""
}

func ParsePathStyle(path string) (Target, bool) {
	if path == "" || path == "/" || !strings.HasPrefix(path, "/") {
		return Target{}, false
	}

	trimmed := strings.TrimPrefix(path, "/")
	bucket, key, hasKey := strings.Cut(trimmed, "/")
	if bucket == "" {
		return Target{}, false
	}

	bucket, err := url.PathUnescape(bucket)
	if err != nil {
		return Target{}, false
	}

	if !hasKey || key == "" {
		return Target{Bucket: bucket}, true
	}

	key, err = url.PathUnescape(key)
	if err != nil {
		return Target{}, false
	}

	return Target{Bucket: bucket, Key: key}, true
}
