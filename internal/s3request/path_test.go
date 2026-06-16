package s3request

import "testing"

func TestParsePathStyle(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		want   Target
		wantOK bool
	}{
		{
			name:   "object at bucket root",
			path:   "/my-bucket/object.txt",
			want:   Target{Bucket: "my-bucket", Key: "object.txt"},
			wantOK: true,
		},
		{
			name:   "nested object key",
			path:   "/my-bucket/path/to/object.parquet",
			want:   Target{Bucket: "my-bucket", Key: "path/to/object.parquet"},
			wantOK: true,
		},
		{
			name:   "bucket operation",
			path:   "/my-bucket",
			want:   Target{Bucket: "my-bucket"},
			wantOK: true,
		},
		{
			name:   "bucket operation with trailing slash",
			path:   "/my-bucket/",
			want:   Target{Bucket: "my-bucket"},
			wantOK: true,
		},
		{
			name:   "url escaped bucket and key",
			path:   "/my-bucket/a%20file%2Fpart.txt",
			want:   Target{Bucket: "my-bucket", Key: "a file/part.txt"},
			wantOK: true,
		},
		{
			name:   "root path is not path-style s3",
			path:   "/",
			wantOK: false,
		},
		{
			name:   "empty path is not path-style s3",
			path:   "",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParsePathStyle(tt.path)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("target = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestTargetIsObject(t *testing.T) {
	tests := []struct {
		name string
		in   Target
		want bool
	}{
		{name: "bucket and key", in: Target{Bucket: "bucket", Key: "key"}, want: true},
		{name: "bucket only", in: Target{Bucket: "bucket"}, want: false},
		{name: "key without bucket", in: Target{Key: "key"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.IsObject(); got != tt.want {
				t.Fatalf("IsObject() = %v, want %v", got, tt.want)
			}
		})
	}
}
