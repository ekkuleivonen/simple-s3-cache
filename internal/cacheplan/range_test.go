package cacheplan

import "testing"

func TestParseRange(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		objectSize int64
		want       ByteRange
	}{
		{
			name:       "bounded range",
			header:     "bytes=100-199",
			objectSize: 1000,
			want:       ByteRange{Start: 100, End: 199},
		},
		{
			name:       "open ended range",
			header:     "bytes=900-",
			objectSize: 1000,
			want:       ByteRange{Start: 900, End: 999},
		},
		{
			name:       "suffix range",
			header:     "bytes=-100",
			objectSize: 1000,
			want:       ByteRange{Start: 900, End: 999},
		},
		{
			name:       "suffix larger than object",
			header:     "bytes=-2000",
			objectSize: 1000,
			want:       ByteRange{Start: 0, End: 999},
		},
		{
			name:       "end is clamped to object size",
			header:     "bytes=900-2000",
			objectSize: 1000,
			want:       ByteRange{Start: 900, End: 999},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRange(tt.header, tt.objectSize)
			if err != nil {
				t.Fatalf("ParseRange() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("range = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestParseRangeRejectsUnsupportedRanges(t *testing.T) {
	tests := []string{
		"",
		"items=0-99",
		"bytes=0-99,200-299",
		"bytes=abc-99",
		"bytes=100-99",
		"bytes=1000-",
		"bytes=-0",
	}

	for _, header := range tests {
		t.Run(header, func(t *testing.T) {
			if _, err := ParseRange(header, 1000); err == nil {
				t.Fatal("ParseRange() error = nil, want error")
			}
		})
	}
}

func TestPagesForRange(t *testing.T) {
	tests := []struct {
		name     string
		in       ByteRange
		pageSize int64
		want     []PageSpan
	}{
		{
			name:     "single page",
			in:       ByteRange{Start: 0, End: 63},
			pageSize: 64,
			want:     []PageSpan{{Index: 0, Start: 0, End: 63}},
		},
		{
			name:     "two pages with partial second page",
			in:       ByteRange{Start: 0, End: 99},
			pageSize: 64,
			want: []PageSpan{
				{Index: 0, Start: 0, End: 63},
				{Index: 1, Start: 64, End: 99},
			},
		},
		{
			name:     "range crossing page boundary",
			in:       ByteRange{Start: 60, End: 70},
			pageSize: 64,
			want: []PageSpan{
				{Index: 0, Start: 60, End: 63},
				{Index: 1, Start: 64, End: 70},
			},
		},
		{
			name:     "aligned middle page",
			in:       ByteRange{Start: 64, End: 127},
			pageSize: 64,
			want:     []PageSpan{{Index: 1, Start: 64, End: 127}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PagesForRange(tt.in, tt.pageSize)
			if err != nil {
				t.Fatalf("PagesForRange() error = %v", err)
			}
			if !equalPageSpans(got, tt.want) {
				t.Fatalf("pages = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestPageBounds(t *testing.T) {
	got, err := PageBounds(2, 64, 150)
	if err != nil {
		t.Fatalf("PageBounds() error = %v", err)
	}

	want := PageSpan{Index: 2, Start: 128, End: 149}
	if got != want {
		t.Fatalf("bounds = %#v, want %#v", got, want)
	}
}

func equalPageSpans(a, b []PageSpan) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
