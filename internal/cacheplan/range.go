package cacheplan

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ByteRange struct {
	Start int64
	End   int64
}

type PageSpan struct {
	Index int64
	Start int64
	End   int64
}

func ParseRange(header string, objectSize int64) (ByteRange, error) {
	if objectSize <= 0 {
		return ByteRange{}, errors.New("object size must be greater than zero")
	}

	value := strings.TrimSpace(header)
	if !strings.HasPrefix(value, "bytes=") {
		return ByteRange{}, fmt.Errorf("unsupported range unit in %q", header)
	}

	spec := strings.TrimSpace(strings.TrimPrefix(value, "bytes="))
	if spec == "" || strings.Contains(spec, ",") {
		return ByteRange{}, fmt.Errorf("unsupported range spec %q", header)
	}

	startText, endText, ok := strings.Cut(spec, "-")
	if !ok {
		return ByteRange{}, fmt.Errorf("invalid range spec %q", header)
	}

	if startText == "" {
		return parseSuffixRange(endText, objectSize)
	}

	start, err := parseNonNegativeInt(startText)
	if err != nil {
		return ByteRange{}, fmt.Errorf("invalid range start: %w", err)
	}
	if start >= objectSize {
		return ByteRange{}, errors.New("range start is beyond object size")
	}

	if endText == "" {
		return ByteRange{Start: start, End: objectSize - 1}, nil
	}

	end, err := parseNonNegativeInt(endText)
	if err != nil {
		return ByteRange{}, fmt.Errorf("invalid range end: %w", err)
	}
	if end < start {
		return ByteRange{}, errors.New("range end is before start")
	}
	if end >= objectSize {
		end = objectSize - 1
	}

	return ByteRange{Start: start, End: end}, nil
}

func PagesForRange(r ByteRange, pageSize int64) ([]PageSpan, error) {
	if err := validateRange(r); err != nil {
		return nil, err
	}
	if pageSize <= 0 {
		return nil, errors.New("page size must be greater than zero")
	}

	first := r.Start / pageSize
	last := r.End / pageSize
	pages := make([]PageSpan, 0, last-first+1)

	for index := first; index <= last; index++ {
		pageStart := index * pageSize
		pageEnd := pageStart + pageSize - 1
		pages = append(pages, PageSpan{
			Index: index,
			Start: maxInt64(r.Start, pageStart),
			End:   minInt64(r.End, pageEnd),
		})
	}

	return pages, nil
}

func PageBounds(index, pageSize, objectSize int64) (PageSpan, error) {
	if index < 0 {
		return PageSpan{}, errors.New("page index must not be negative")
	}
	if pageSize <= 0 {
		return PageSpan{}, errors.New("page size must be greater than zero")
	}
	if objectSize <= 0 {
		return PageSpan{}, errors.New("object size must be greater than zero")
	}

	start := index * pageSize
	if start >= objectSize {
		return PageSpan{}, errors.New("page start is beyond object size")
	}

	return PageSpan{
		Index: index,
		Start: start,
		End:   minInt64(start+pageSize-1, objectSize-1),
	}, nil
}

func parseSuffixRange(text string, objectSize int64) (ByteRange, error) {
	suffix, err := parseNonNegativeInt(text)
	if err != nil {
		return ByteRange{}, fmt.Errorf("invalid suffix range: %w", err)
	}
	if suffix == 0 {
		return ByteRange{}, errors.New("suffix range must be greater than zero")
	}
	if suffix >= objectSize {
		return ByteRange{Start: 0, End: objectSize - 1}, nil
	}

	return ByteRange{Start: objectSize - suffix, End: objectSize - 1}, nil
}

func parseNonNegativeInt(text string) (int64, error) {
	if text == "" {
		return 0, errors.New("empty integer")
	}

	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, errors.New("value must not be negative")
	}

	return value, nil
}

func validateRange(r ByteRange) error {
	if r.Start < 0 {
		return errors.New("range start must not be negative")
	}
	if r.End < r.Start {
		return errors.New("range end must not be before start")
	}

	return nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
