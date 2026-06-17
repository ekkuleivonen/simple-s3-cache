package s3request

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Request struct {
	Method   string
	Target   Target
	RawQuery string
	Header   http.Header
}

type Disposition string

const (
	PassThrough          Disposition = "pass_through"
	CacheableFullObject  Disposition = "cacheable_full_object"
	CacheableHeadObject  Disposition = "cacheable_head_object"
	CacheableRangeObject Disposition = "cacheable_range_object"
)

type Classification struct {
	Disposition Disposition
	Reason      string
}

type benignObjectReadQuery struct {
	Method string
	Name   string
	Value  string
}

// benignObjectReadQueryAllowlist contains query parameters that are SDK
// operation markers, not S3 object subresources, response overrides, auth
// material, or object identity selectors. Add to this list only when the
// parameter is response-neutral and safe to collapse into the plain bucket/key
// cache entry.
var benignObjectReadQueryAllowlist = []benignObjectReadQuery{
	{Method: http.MethodGet, Name: "x-id", Value: "GetObject"},
}

func Classify(req Request) Classification {
	if !req.Target.IsObject() {
		return passThrough("not_object")
	}

	if hasSSECustomerHeaders(req.Header) {
		return passThrough("sse_c")
	}

	if req.RawQuery != "" && !IsBenignObjectReadQuery(req.Method, req.RawQuery) {
		return passThrough("query")
	}

	switch req.Method {
	case http.MethodHead:
		if req.Header.Get("Range") != "" {
			return passThrough("range")
		}
		return Classification{Disposition: CacheableHeadObject}
	case http.MethodGet:
		rangeHeader := req.Header.Get("Range")
		if rangeHeader == "" {
			return Classification{Disposition: CacheableFullObject}
		}
		if isSingleRangeHeader(rangeHeader) {
			return Classification{Disposition: CacheableRangeObject}
		}
		return passThrough("range")
	default:
		return passThrough("method")
	}
}

func IsBenignObjectReadQuery(method, rawQuery string) bool {
	if rawQuery == "" {
		return false
	}

	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false
	}
	if len(values) != 1 {
		return false
	}

	for _, allowed := range benignObjectReadQueryAllowlist {
		if method != allowed.Method {
			continue
		}
		got := values[allowed.Name]
		if len(got) == 1 && got[0] == allowed.Value {
			return true
		}
	}

	return false
}

func passThrough(reason string) Classification {
	return Classification{Disposition: PassThrough, Reason: reason}
}

func hasSSECustomerHeaders(header http.Header) bool {
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-server-side-encryption-customer-") {
			return true
		}
	}

	return false
}

func isSingleRangeHeader(value string) bool {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "bytes=") {
		return false
	}

	spec := strings.TrimSpace(strings.TrimPrefix(value, "bytes="))
	if spec == "" || strings.Contains(spec, ",") {
		return false
	}

	start, end, ok := strings.Cut(spec, "-")
	if !ok {
		return false
	}

	if start == "" && end == "" {
		return false
	}
	if start != "" && !isUnsignedDecimal(start) {
		return false
	}
	if end != "" && !isUnsignedDecimal(end) {
		return false
	}
	if start != "" && end != "" {
		startValue, err := strconv.ParseInt(start, 10, 64)
		if err != nil {
			return false
		}
		endValue, err := strconv.ParseInt(end, 10, 64)
		if err != nil {
			return false
		}
		return startValue <= endValue
	}
	if start != "" {
		_, err := strconv.ParseInt(start, 10, 64)
		return err == nil
	}
	if end != "" {
		_, err := strconv.ParseInt(end, 10, 64)
		return err == nil
	}

	return true
}

func isUnsignedDecimal(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}
