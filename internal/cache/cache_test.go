package cache

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestObjectKeyIsStableAndPathSafe(t *testing.T) {
	got := ObjectKey("photos", "path/to/object")
	const want = "581eb55a2aecc971a274827e29175efd0678b3675ab9280e176c00d848866caf"
	if got != want {
		t.Fatalf("ObjectKey() = %q, want %q", got, want)
	}

	if got == ObjectKey("photos", "path/to/other") {
		t.Fatal("ObjectKey() did not include key")
	}
	if got == ObjectKey("other", "path/to/object") {
		t.Fatal("ObjectKey() did not include bucket")
	}

	c := openTestCache(t)
	path := c.pagePath(got, 12)
	if !filepath.IsLocal(path) {
		t.Fatalf("page path = %q, want local cache-relative path", path)
	}
	if filepath.Base(path) != "page-000000000012" {
		t.Fatalf("page file = %q, want page-000000000012", filepath.Base(path))
	}
}

func TestIndexStoresObjectMetadataAndPages(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)

	headers := http.Header{
		"Content-Type":     []string{"application/octet-stream"},
		"Cache-Control":    []string{"max-age=60"},
		"X-Amz-Meta-Owner": []string{"analytics"},
	}
	obj, err := c.PutObject(ctx, ObjectMetadata{
		Bucket:   "bucket",
		Key:      "dir/object.parquet",
		ETag:     `"etag-1"`,
		Size:     12345,
		PageSize: 4096,
		Headers:  headers,
	})
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	got, ok, err := c.GetObject(ctx, "bucket", "dir/object.parquet")
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	if !ok {
		t.Fatal("GetObject() ok = false, want true")
	}
	if got.ID != ObjectKey("bucket", "dir/object.parquet") {
		t.Fatalf("object ID = %q", got.ID)
	}
	if got.ETag != `"etag-1"` || got.Size != 12345 || got.PageSize != 4096 {
		t.Fatalf("object metadata = %#v", got)
	}
	if got.Headers.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("Content-Type = %q", got.Headers.Get("Content-Type"))
	}
	if got.Headers.Get("X-Amz-Meta-Owner") != "analytics" {
		t.Fatalf("X-Amz-Meta-Owner = %q", got.Headers.Get("X-Amz-Meta-Owner"))
	}

	if err := c.PutPage(ctx, Page{
		ObjectID: obj.ID,
		Index:    3,
		ETag:     obj.ETag,
		Size:     4096,
		Path:     c.pagePath(obj.ID, 3),
	}); err != nil {
		t.Fatalf("PutPage() error = %v", err)
	}

	pages, err := c.ListPages(ctx, obj.ID)
	if err != nil {
		t.Fatalf("ListPages() error = %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("pages len = %d, want 1", len(pages))
	}
	if pages[0].Index != 3 || pages[0].ETag != obj.ETag || pages[0].Size != 4096 {
		t.Fatalf("page = %#v", pages[0])
	}
}

func TestStorePageWritesFileAtomicallyThenCommitsRow(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	obj := putTestObject(t, c, "bucket", "large.bin")

	page, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    0,
		ETag:     obj.ETag,
		Data:     []byte("hello cached page"),
	})
	if err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}
	if page.Path == "" {
		t.Fatal("page path is empty")
	}

	body, ok, err := c.OpenPage(ctx, obj.ID, 0)
	if err != nil {
		t.Fatalf("OpenPage() error = %v", err)
	}
	if !ok {
		t.Fatal("OpenPage() ok = false, want true")
	}
	defer body.Close()

	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(page) error = %v", err)
	}
	if !bytes.Equal(got, []byte("hello cached page")) {
		t.Fatalf("page body = %q", got)
	}

	matches, err := filepath.Glob(filepath.Join(c.cacheRoot, "objects", "*", "*", obj.ID, "*.tmp"))
	if err != nil {
		t.Fatalf("glob temp files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files left behind: %v", matches)
	}
}

func TestOpenPageTreatsMissingFileAsMissAndRemovesStaleRow(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	obj := putTestObject(t, c, "bucket", "missing.bin")

	page, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    5,
		ETag:     obj.ETag,
		Data:     []byte("page that disappears"),
	})
	if err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}
	if err := os.Remove(filepath.Join(c.cacheRoot, page.Path)); err != nil {
		t.Fatalf("remove page file: %v", err)
	}

	body, ok, err := c.OpenPage(ctx, obj.ID, 5)
	if err != nil {
		t.Fatalf("OpenPage() error = %v", err)
	}
	if ok {
		body.Close()
		t.Fatal("OpenPage() ok = true, want false")
	}

	pages, err := c.ListPages(ctx, obj.ID)
	if err != nil {
		t.Fatalf("ListPages() error = %v", err)
	}
	if len(pages) != 0 {
		t.Fatalf("stale page rows = %#v, want none", pages)
	}
}

func TestDeleteObjectRemovesMetadataRowsAndPageFiles(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	obj := putTestObject(t, c, "bucket", "delete.bin")
	page, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    0,
		ETag:     obj.ETag,
		Data:     []byte("cached page"),
	})
	if err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}

	if err := c.DeleteObject(ctx, "bucket", "delete.bin"); err != nil {
		t.Fatalf("DeleteObject() error = %v", err)
	}

	if _, ok, err := c.GetObject(ctx, "bucket", "delete.bin"); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if ok {
		t.Fatal("GetObject() ok = true, want false")
	}
	pages, err := c.ListPages(ctx, obj.ID)
	if err != nil {
		t.Fatalf("ListPages() error = %v", err)
	}
	if len(pages) != 0 {
		t.Fatalf("pages len = %d, want 0", len(pages))
	}
	if _, err := os.Stat(filepath.Join(c.cacheRoot, page.Path)); !os.IsNotExist(err) {
		t.Fatalf("page file stat error = %v, want not exist", err)
	}
}

func TestOpenUsesSeparateCacheAndMetaPaths(t *testing.T) {
	ctx := context.Background()
	cachePath := filepath.Join(t.TempDir(), "cache-bytes")
	metaPath := filepath.Join(t.TempDir(), "cache-meta")

	c, err := Open(ctx, Options{CachePath: cachePath, MetaPath: metaPath})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	obj := putTestObject(t, c, "bucket", "split.bin")
	page, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    1,
		ETag:     obj.ETag,
		Data:     []byte("split paths"),
	})
	if err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(metaPath, "cache.db")); err != nil {
		t.Fatalf("stat metadata db: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cachePath, page.Path)); err != nil {
		t.Fatalf("stat page file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(metaPath, page.Path)); !os.IsNotExist(err) {
		t.Fatalf("page file under meta path error = %v, want not exist", err)
	}
}

func openTestCache(t *testing.T) *Cache {
	t.Helper()

	root := t.TempDir()
	c, err := Open(context.Background(), Options{
		CachePath: filepath.Join(root, "cache-bytes"),
		MetaPath:  filepath.Join(root, "cache-meta"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return c
}

func putTestObject(t *testing.T, c *Cache, bucket, key string) Object {
	t.Helper()

	obj, err := c.PutObject(context.Background(), ObjectMetadata{
		Bucket:   bucket,
		Key:      key,
		ETag:     `"etag-1"`,
		Size:     1024,
		PageSize: 128,
		Headers:  http.Header{"Content-Type": []string{"application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	return obj
}
