package cache

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		Size:     int64(len("hello cached page")),
		Source:   bytes.NewReader([]byte("hello cached page")),
	})
	if err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}
	if page.Path == "" {
		t.Fatal("page path is empty")
	}

	body, ok, err := c.OpenPage(ctx, obj.ID, 0, obj.ETag, obj.Epoch)
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
		Size:     int64(len("page that disappears")),
		Source:   bytes.NewReader([]byte("page that disappears")),
	})
	if err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}
	if err := os.Remove(filepath.Join(c.cacheRoot, page.Path)); err != nil {
		t.Fatalf("remove page file: %v", err)
	}

	body, ok, err := c.OpenPage(ctx, obj.ID, 5, obj.ETag, obj.Epoch)
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
		Size:     int64(len("cached page")),
		Source:   bytes.NewReader([]byte("cached page")),
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

func TestStorePageRejectsStaleObjectEpochAfterInvalidation(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)

	oldObj := putTestObject(t, c, "bucket", "race.bin")
	if oldObj.Epoch != 0 {
		t.Fatalf("initial epoch = %d, want 0", oldObj.Epoch)
	}
	if err := c.DeleteObject(ctx, "bucket", "race.bin"); err != nil {
		t.Fatalf("DeleteObject() error = %v", err)
	}

	newObj, err := c.PutObject(ctx, ObjectMetadata{
		Bucket:   "bucket",
		Key:      "race.bin",
		ETag:     `"etag-2"`,
		Size:     1024,
		PageSize: 128,
		Headers:  http.Header{"Content-Type": []string{"application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("PutObject(new) error = %v", err)
	}
	if newObj.Epoch <= oldObj.Epoch {
		t.Fatalf("new epoch = %d, want greater than old epoch %d", newObj.Epoch, oldObj.Epoch)
	}

	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID:      oldObj.ID,
		Index:         0,
		ETag:          oldObj.ETag,
		ExpectedEpoch: oldObj.Epoch,
		Size:          int64(len("stale page")),
		Source:        bytes.NewReader([]byte("stale page")),
	}); err == nil {
		t.Fatal("StorePage(stale) error = nil, want stale epoch error")
	}

	body, ok, err := c.OpenPage(ctx, newObj.ID, 0, newObj.ETag, newObj.Epoch)
	if err != nil {
		t.Fatalf("OpenPage() error = %v", err)
	}
	if ok {
		body.Close()
		t.Fatal("OpenPage() ok = true, want false")
	}
}

func TestStaleStorePageCannotOverwriteNewerPageAfterInvalidationRace(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	oldObj := putTestObject(t, c, "bucket", "race-overwrite.bin")

	validated := make(chan struct{})
	releaseStaleStore := make(chan struct{})
	var hookUsed atomic.Bool
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseStaleStore)
		})
	}
	t.Cleanup(func() {
		storePageAfterValidationHook = nil
		release()
	})
	storePageAfterValidationHook = func() {
		if hookUsed.CompareAndSwap(false, true) {
			close(validated)
			<-releaseStaleStore
		}
	}

	staleDone := make(chan error, 1)
	go func() {
		_, err := c.StorePage(ctx, PageWrite{
			ObjectID:      oldObj.ID,
			Index:         0,
			ETag:          oldObj.ETag,
			ExpectedEpoch: oldObj.Epoch,
			Size:          int64(len("stale page")),
			Source:        bytes.NewReader([]byte("stale page")),
		})
		staleDone <- err
	}()

	<-validated
	if err := c.DeleteObject(ctx, "bucket", "race-overwrite.bin"); err != nil {
		t.Fatalf("DeleteObject() error = %v", err)
	}
	newObj, err := c.PutObject(ctx, ObjectMetadata{
		Bucket:   "bucket",
		Key:      "race-overwrite.bin",
		ETag:     `"etag-2"`,
		Size:     1024,
		PageSize: 128,
		Headers:  http.Header{"Content-Type": []string{"application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("PutObject(new) error = %v", err)
	}
	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID:      newObj.ID,
		Index:         0,
		ETag:          newObj.ETag,
		ExpectedEpoch: newObj.Epoch,
		Size:          int64(len("new page")),
		Source:        bytes.NewReader([]byte("new page")),
	}); err != nil {
		t.Fatalf("StorePage(new) error = %v", err)
	}

	release()
	if err := <-staleDone; err == nil {
		t.Fatal("StorePage(stale) error = nil, want stale epoch error")
	}

	body, ok, err := c.OpenPage(ctx, newObj.ID, 0, newObj.ETag, newObj.Epoch)
	if err != nil {
		t.Fatalf("OpenPage(new) error = %v", err)
	}
	if !ok {
		t.Fatal("OpenPage(new) ok = false, want newer page preserved")
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(new page) error = %v", err)
	}
	if !bytes.Equal(got, []byte("new page")) {
		t.Fatalf("new page body = %q, want %q", got, "new page")
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
		Size:     int64(len("split paths")),
		Source:   bytes.NewReader([]byte("split paths")),
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

func TestOpenConfiguresSQLiteForConcurrentReadsAndLRUEviction(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)

	if got := c.db.Stats().MaxOpenConnections; got != cacheDBMaxOpenConns {
		t.Fatalf("MaxOpenConnections = %d, want %d", got, cacheDBMaxOpenConns)
	}

	rows, err := c.db.QueryContext(ctx, `PRAGMA index_list(pages)`)
	if err != nil {
		t.Fatalf("list page indexes: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan page index: %v", err)
		}
		if name == "pages_lru_idx" {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list page indexes: %v", err)
	}
	if !found {
		t.Fatal("pages_lru_idx not found")
	}
}

func TestOpenConfiguresSQLiteForBucketLRUEviction(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)

	pageIndexes := sqliteIndexNames(t, ctx, c, "pages")
	for _, name := range []string{"pages_lru_idx", "pages_object_lru_idx"} {
		if !pageIndexes[name] {
			t.Fatalf("%s not found in pages indexes: %#v", name, pageIndexes)
		}
	}

	objectIndexes := sqliteIndexNames(t, ctx, c, "objects")
	if !objectIndexes["objects_bucket_idx"] {
		t.Fatalf("objects_bucket_idx not found in objects indexes: %#v", objectIndexes)
	}
}

func TestCacheTracksStoredPageSize(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	obj := putTestObject(t, c, "bucket", "size.bin")

	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got != 0 {
		t.Fatalf("initial CurrentSize() = %d, want 0", got)
	}

	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    0,
		ETag:     obj.ETag,
		Size:     int64(len("12345")),
		Source:   bytes.NewReader([]byte("12345")),
	}); err != nil {
		t.Fatalf("StorePage(first) error = %v", err)
	}
	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    1,
		ETag:     obj.ETag,
		Size:     int64(len("abc")),
		Source:   bytes.NewReader([]byte("abc")),
	}); err != nil {
		t.Fatalf("StorePage(second) error = %v", err)
	}

	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got != 8 {
		t.Fatalf("CurrentSize() = %d, want 8", got)
	}
}

func TestStorePageRejectsPageLargerThanMaxSize(t *testing.T) {
	ctx := context.Background()
	c := openTestCacheWithMaxSize(t, 4)
	obj := putTestObject(t, c, "bucket", "too-large.bin")

	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    0,
		ETag:     obj.ETag,
		Size:     int64(len("12345")),
		Source:   bytes.NewReader([]byte("12345")),
	}); err == nil {
		t.Fatal("StorePage() error = nil, want max-size rejection")
	}

	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got != 0 {
		t.Fatalf("CurrentSize() = %d, want 0", got)
	}
}

func TestEvictLRURemovesOldestPagesUntilUnderMaxSize(t *testing.T) {
	ctx := context.Background()
	c := openTestCacheWithMaxSize(t, 7)
	obj := putTestObject(t, c, "bucket", "evict.bin")

	for i, data := range [][]byte{[]byte("1111"), []byte("2222"), []byte("3333")} {
		if _, err := c.StorePage(ctx, PageWrite{
			ObjectID: obj.ID,
			Index:    int64(i),
			ETag:     obj.ETag,
			Size:     int64(len(data)),
			Source:   bytes.NewReader(data),
		}); err != nil {
			t.Fatalf("StorePage(%d) error = %v", i, err)
		}
	}

	if err := c.Evict(ctx); err != nil {
		t.Fatalf("Evict() error = %v", err)
	}

	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got > 7 {
		t.Fatalf("CurrentSize() = %d, want <= 7", got)
	}
	if body, ok, err := c.OpenPage(ctx, obj.ID, 0, obj.ETag, obj.Epoch); err != nil {
		t.Fatalf("OpenPage(oldest) error = %v", err)
	} else if ok {
		body.Close()
		t.Fatal("oldest page still present after eviction")
	}
	if body, ok, err := c.OpenPage(ctx, obj.ID, 2, obj.ETag, obj.Epoch); err != nil {
		t.Fatalf("OpenPage(newest) error = %v", err)
	} else if !ok {
		t.Fatal("newest page was evicted, want it retained")
	} else {
		body.Close()
	}
}

func TestStorePageRejectsPageLargerThanBucketMaxSize(t *testing.T) {
	ctx := context.Background()
	c := openTestCacheWithBucketMaxSizes(t, 100, map[string]int64{
		"analytics": 4,
	})
	analyticsObj := putTestObject(t, c, "analytics", "too-large.bin")
	mediaObj := putTestObject(t, c, "media", "fits-global.bin")

	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID: analyticsObj.ID,
		Index:    0,
		ETag:     analyticsObj.ETag,
		Size:     int64(len("12345")),
		Source:   bytes.NewReader([]byte("12345")),
	}); err == nil {
		t.Fatal("StorePage(analytics) error = nil, want bucket max-size rejection")
	}

	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID: mediaObj.ID,
		Index:    0,
		ETag:     mediaObj.ETag,
		Size:     int64(len("12345")),
		Source:   bytes.NewReader([]byte("12345")),
	}); err != nil {
		t.Fatalf("StorePage(media) error = %v, want page to fit global max size", err)
	}
	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got != 5 {
		t.Fatalf("CurrentSize() = %d, want 5", got)
	}
}

func TestEvictRemovesOldestPagesFromBucketsOverTheirOwnMaxSize(t *testing.T) {
	ctx := context.Background()
	c := openTestCacheWithBucketMaxSizes(t, 100, map[string]int64{
		"analytics": 7,
		"media":     90,
	})
	mediaObj := putTestObject(t, c, "media", "old-but-within-budget.bin")
	analyticsObj := putTestObject(t, c, "analytics", "over-budget.bin")

	storeTestPage(t, c, mediaObj, 0, []byte("mmmmmmmmmm"))
	for i, data := range [][]byte{[]byte("1111"), []byte("2222"), []byte("3333")} {
		storeTestPage(t, c, analyticsObj, int64(i), data)
	}

	if err := c.Evict(ctx); err != nil {
		t.Fatalf("Evict() error = %v", err)
	}

	assertPagePresent(t, c, mediaObj, 0, true)
	assertPagePresent(t, c, analyticsObj, 0, false)
	assertPagePresent(t, c, analyticsObj, 1, false)
	assertPagePresent(t, c, analyticsObj, 2, true)
	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got != 14 {
		t.Fatalf("CurrentSize() = %d, want 14", got)
	}
}

func TestEvictStillEnforcesGlobalMaxSizeWithBucketMaxSizes(t *testing.T) {
	ctx := context.Background()
	c := openTestCacheWithBucketMaxSizes(t, 10, map[string]int64{
		"analytics": 100,
		"media":     100,
	})
	analyticsObj := putTestObject(t, c, "analytics", "oldest.bin")
	mediaObj := putTestObject(t, c, "media", "newer.bin")

	storeTestPage(t, c, analyticsObj, 0, []byte("1111"))
	storeTestPage(t, c, mediaObj, 0, []byte("2222"))
	storeTestPage(t, c, mediaObj, 1, []byte("3333"))

	if err := c.Evict(ctx); err != nil {
		t.Fatalf("Evict() error = %v", err)
	}

	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got > 10 {
		t.Fatalf("CurrentSize() = %d, want <= 10", got)
	}
	assertPagePresent(t, c, analyticsObj, 0, false)
	assertPagePresent(t, c, mediaObj, 1, true)
}

func TestOpenPageTreatsSizeMismatchAsMissAndRemovesCorruptFile(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	obj := putTestObject(t, c, "bucket", "corrupt.bin")

	page, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    0,
		ETag:     obj.ETag,
		Size:     int64(len("valid page")),
		Source:   bytes.NewReader([]byte("valid page")),
	})
	if err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}
	pagePath := filepath.Join(c.cacheRoot, page.Path)
	if err := os.WriteFile(pagePath, []byte("bad"), 0o644); err != nil {
		t.Fatalf("corrupt page file: %v", err)
	}

	body, ok, err := c.OpenPage(ctx, obj.ID, 0, obj.ETag, obj.Epoch)
	if err != nil {
		t.Fatalf("OpenPage() error = %v", err)
	}
	if ok {
		body.Close()
		t.Fatal("OpenPage() ok = true, want corrupt file miss")
	}
	if _, err := os.Stat(pagePath); !os.IsNotExist(err) {
		t.Fatalf("corrupt page file stat error = %v, want removed", err)
	}
}

func TestOpenTreatsCorruptDatabaseAsEmptyCache(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cachePath := filepath.Join(root, "cache-bytes")
	metaPath := filepath.Join(root, "cache-meta")
	if err := os.MkdirAll(metaPath, 0o755); err != nil {
		t.Fatalf("create metadata path: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(cachePath, "objects", "orphan"), 0o755); err != nil {
		t.Fatalf("create orphan cache path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cachePath, "objects", "orphan", "page"), []byte("orphan"), 0o644); err != nil {
		t.Fatalf("write orphan page: %v", err)
	}
	if err := os.WriteFile(filepath.Join(metaPath, "cache.db"), []byte("not sqlite"), 0o600); err != nil {
		t.Fatalf("write corrupt cache db: %v", err)
	}

	c, err := Open(ctx, Options{CachePath: cachePath, MetaPath: metaPath, MaxSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open() error = %v, want clean empty cache after corrupt db", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if got, err := c.CurrentSize(ctx); err != nil {
		t.Fatalf("CurrentSize() error = %v", err)
	} else if got != 0 {
		t.Fatalf("CurrentSize() = %d, want empty cache", got)
	}
	if _, err := os.Stat(filepath.Join(cachePath, "objects", "orphan", "page")); !os.IsNotExist(err) {
		t.Fatalf("orphan page stat error = %v, want removed with corrupt db reset", err)
	}
}

func TestMetadataGCDeletesOldPageLessObjects(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	c.metadataMaxAge = time.Hour
	c.metadataGCBatchSize = 10
	obj := putTestObject(t, c, "bucket", "old-metadata.bin")
	old := time.Now().Add(-2 * time.Hour).UnixNano()
	if _, err := c.execWrite(ctx, `UPDATE objects SET updated_at = ? WHERE id = ?`, old, obj.ID); err != nil {
		t.Fatalf("age object metadata: %v", err)
	}
	if _, err := c.execWrite(ctx, `UPDATE object_generations SET updated_at = ? WHERE id = ?`, old, obj.ID); err != nil {
		t.Fatalf("age object generation: %v", err)
	}

	result, err := c.collectMetadata(ctx)
	if err != nil {
		t.Fatalf("collectMetadata() error = %v", err)
	}
	if result.ObjectsDeleted != 1 {
		t.Fatalf("ObjectsDeleted = %d, want 1", result.ObjectsDeleted)
	}
	if _, ok, err := c.GetObject(ctx, "bucket", "old-metadata.bin"); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if ok {
		t.Fatal("GetObject() ok = true, want old page-less metadata removed")
	}
}

func TestMetadataGCPreservesYoungPageLessObjects(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	c.metadataMaxAge = time.Hour
	c.metadataGCBatchSize = 10
	putTestObject(t, c, "bucket", "young-metadata.bin")

	result, err := c.collectMetadata(ctx)
	if err != nil {
		t.Fatalf("collectMetadata() error = %v", err)
	}
	if result.ObjectsDeleted != 0 {
		t.Fatalf("ObjectsDeleted = %d, want 0", result.ObjectsDeleted)
	}
	if _, ok, err := c.GetObject(ctx, "bucket", "young-metadata.bin"); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if !ok {
		t.Fatal("GetObject() ok = false, want young metadata preserved")
	}
}

func TestMetadataGCPreservesObjectsWithPages(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	c.metadataMaxAge = time.Hour
	c.metadataGCBatchSize = 10
	obj := putTestObject(t, c, "bucket", "paged-metadata.bin")
	if _, err := c.StorePage(ctx, PageWrite{
		ObjectID: obj.ID,
		Index:    0,
		ETag:     obj.ETag,
		Size:     int64(len("cached page")),
		Source:   bytes.NewReader([]byte("cached page")),
	}); err != nil {
		t.Fatalf("StorePage() error = %v", err)
	}
	old := time.Now().Add(-2 * time.Hour).UnixNano()
	if _, err := c.execWrite(ctx, `UPDATE objects SET updated_at = ? WHERE id = ?`, old, obj.ID); err != nil {
		t.Fatalf("age object metadata: %v", err)
	}

	result, err := c.collectMetadata(ctx)
	if err != nil {
		t.Fatalf("collectMetadata() error = %v", err)
	}
	if result.ObjectsDeleted != 0 {
		t.Fatalf("ObjectsDeleted = %d, want 0", result.ObjectsDeleted)
	}
	if _, ok, err := c.GetObject(ctx, "bucket", "paged-metadata.bin"); err != nil {
		t.Fatalf("GetObject() error = %v", err)
	} else if !ok {
		t.Fatal("GetObject() ok = false, want metadata with pages preserved")
	}
	if body, ok, err := c.OpenPage(ctx, obj.ID, 0, obj.ETag, obj.Epoch); err != nil {
		t.Fatalf("OpenPage() error = %v", err)
	} else if !ok {
		t.Fatal("OpenPage() ok = false, want page preserved")
	} else {
		body.Close()
	}
}

func TestMetadataGCRetainsAndPrunesOldGenerations(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)
	c.metadataMaxAge = time.Hour
	c.metadataGCBatchSize = 10
	obj := putTestObject(t, c, "bucket", "generation.bin")
	if err := c.DeleteObject(ctx, "bucket", "generation.bin"); err != nil {
		t.Fatalf("DeleteObject() error = %v", err)
	}

	result, err := c.collectMetadata(ctx)
	if err != nil {
		t.Fatalf("collectMetadata() error = %v", err)
	}
	if result.GenerationsDeleted != 0 {
		t.Fatalf("GenerationsDeleted = %d, want 0 for recent invalidation", result.GenerationsDeleted)
	}
	if !generationExists(t, c, obj.ID) {
		t.Fatal("generation row was pruned before retention age")
	}

	old := time.Now().Add(-2 * time.Hour).UnixNano()
	if _, err := c.execWrite(ctx, `UPDATE object_generations SET updated_at = ? WHERE id = ?`, old, obj.ID); err != nil {
		t.Fatalf("age object generation: %v", err)
	}
	result, err = c.collectMetadata(ctx)
	if err != nil {
		t.Fatalf("collectMetadata(second) error = %v", err)
	}
	if result.GenerationsDeleted != 1 {
		t.Fatalf("GenerationsDeleted = %d, want 1", result.GenerationsDeleted)
	}
	if generationExists(t, c, obj.ID) {
		t.Fatal("generation row still present after retention age")
	}
}

func TestSQLiteCheckpointRuns(t *testing.T) {
	ctx := context.Background()
	c := openTestCache(t)

	if err := c.checkpointSQLite(ctx); err != nil {
		t.Fatalf("checkpointSQLite() error = %v", err)
	}
}

func openTestCache(t *testing.T) *Cache {
	t.Helper()

	return openTestCacheWithMaxSize(t, 1<<30)
}

func openTestCacheWithMaxSize(t *testing.T, maxSize int64) *Cache {
	t.Helper()

	root := t.TempDir()
	c, err := Open(context.Background(), Options{
		CachePath: filepath.Join(root, "cache-bytes"),
		MetaPath:  filepath.Join(root, "cache-meta"),
		MaxSize:   maxSize,
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

func openTestCacheWithBucketMaxSizes(t *testing.T, maxSize int64, maxSizeByBucket map[string]int64) *Cache {
	t.Helper()

	root := t.TempDir()
	c, err := Open(context.Background(), Options{
		CachePath:       filepath.Join(root, "cache-bytes"),
		MetaPath:        filepath.Join(root, "cache-meta"),
		MaxSize:         maxSize,
		MaxSizeByBucket: maxSizeByBucket,
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

func storeTestPage(t *testing.T, c *Cache, obj Object, index int64, data []byte) Page {
	t.Helper()

	page, err := c.StorePage(context.Background(), PageWrite{
		ObjectID: obj.ID,
		Index:    index,
		ETag:     obj.ETag,
		Size:     int64(len(data)),
		Source:   bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("StorePage(%s/%s page %d) error = %v", obj.Bucket, obj.Key, index, err)
	}
	return page
}

func assertPagePresent(t *testing.T, c *Cache, obj Object, index int64, wantPresent bool) {
	t.Helper()

	body, ok, err := c.OpenPage(context.Background(), obj.ID, index, obj.ETag, obj.Epoch)
	if err != nil {
		t.Fatalf("OpenPage(%s, %d) error = %v", obj.ID, index, err)
	}
	if ok {
		_ = body.Close()
	}
	if ok != wantPresent {
		t.Fatalf("OpenPage(%s, %d) present = %v, want %v", obj.ID, index, ok, wantPresent)
	}
}

func generationExists(t *testing.T, c *Cache, objectID string) bool {
	t.Helper()

	var count int
	if err := c.db.QueryRow(`SELECT COUNT(*) FROM object_generations WHERE id = ?`, objectID).Scan(&count); err != nil {
		t.Fatalf("query generation count: %v", err)
	}
	return count > 0
}

func sqliteIndexNames(t *testing.T, ctx context.Context, c *Cache, table string) map[string]bool {
	t.Helper()

	rows, err := c.db.QueryContext(ctx, `PRAGMA index_list(`+table+`)`)
	if err != nil {
		t.Fatalf("list %s indexes: %v", table, err)
	}
	defer rows.Close()

	indexes := map[string]bool{}
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan %s index: %v", table, err)
		}
		indexes[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list %s indexes: %v", table, err)
	}
	return indexes
}
