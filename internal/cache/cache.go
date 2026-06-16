package cache

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"

	_ "modernc.org/sqlite"
)

type Options struct {
	CachePath string
	MetaPath  string
	MaxSize   int64
	Metrics   *metrics.Recorder
}

type Cache struct {
	cacheRoot    string
	metaRoot     string
	maxSize      int64
	db           *sql.DB
	evictionCh   chan struct{}
	touchCh      chan pageRef
	cancelWorker context.CancelFunc
	workerWG     sync.WaitGroup
	metrics      *metrics.Recorder
}

type ObjectMetadata struct {
	Bucket   string
	Key      string
	ETag     string
	Size     int64
	PageSize int64
	Headers  http.Header
}

type Object struct {
	ID       string
	Bucket   string
	Key      string
	ETag     string
	Size     int64
	PageSize int64
	Headers  http.Header
	Epoch    int64
}

type Page struct {
	ObjectID string
	Index    int64
	ETag     string
	Size     int64
	Path     string
}

type PageWrite struct {
	ObjectID      string
	Index         int64
	ETag          string
	ExpectedEpoch int64
	Data          []byte
}

type pageRef struct {
	objectID string
	index    int64
}

func Open(ctx context.Context, opts Options) (*Cache, error) {
	if opts.CachePath == "" {
		return nil, errors.New("cache path is required")
	}
	if opts.MetaPath == "" {
		return nil, errors.New("metadata path is required")
	}
	if opts.MaxSize <= 0 {
		opts.MaxSize = 1 << 60
	}
	if err := os.MkdirAll(filepath.Join(opts.CachePath, "objects"), 0o755); err != nil {
		return nil, fmt.Errorf("create cache directories: %w", err)
	}
	if err := os.MkdirAll(opts.MetaPath, 0o755); err != nil {
		return nil, fmt.Errorf("create metadata directory: %w", err)
	}

	db, err := sql.Open("sqlite", filepath.Join(opts.MetaPath, "cache.db"))
	if err != nil {
		return nil, fmt.Errorf("open cache db: %w", err)
	}
	db.SetMaxOpenConns(1)

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	c := &Cache{
		cacheRoot:    opts.CachePath,
		metaRoot:     opts.MetaPath,
		maxSize:      opts.MaxSize,
		db:           db,
		evictionCh:   make(chan struct{}, 1),
		touchCh:      make(chan pageRef, 1024),
		cancelWorker: cancelWorker,
		metrics:      opts.Metrics,
	}
	if err := c.init(ctx); err != nil {
		cancelWorker()
		_ = db.Close()
		return nil, err
	}
	c.updateCacheSizeMetrics(ctx)

	c.workerWG.Add(1)
	go c.maintenanceLoop(workerCtx)

	return c, nil
}

func (c *Cache) Close() error {
	c.cancelWorker()
	c.workerWG.Wait()
	return c.db.Close()
}

func ObjectKey(bucket, key string) string {
	sum := sha256.Sum256([]byte(bucket + "\x00" + key))
	return hex.EncodeToString(sum[:])
}

func (c *Cache) PutObject(ctx context.Context, meta ObjectMetadata) (Object, error) {
	if meta.Bucket == "" {
		return Object{}, errors.New("bucket is required")
	}
	if meta.Key == "" {
		return Object{}, errors.New("key is required")
	}
	if meta.ETag == "" {
		return Object{}, errors.New("etag is required")
	}
	if meta.Size < 0 {
		return Object{}, errors.New("object size must not be negative")
	}
	if meta.PageSize <= 0 {
		return Object{}, errors.New("page size must be greater than zero")
	}

	id := ObjectKey(meta.Bucket, meta.Key)
	headersJSON, err := marshalHeaders(meta.Headers)
	if err != nil {
		return Object{}, err
	}

	if err := c.ensureGeneration(ctx, id); err != nil {
		return Object{}, err
	}
	epoch, err := c.generation(ctx, id)
	if err != nil {
		return Object{}, err
	}

	_, err = c.db.ExecContext(ctx, `
INSERT INTO objects (id, bucket, key, etag, size, page_size, headers_json, epoch, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	bucket = excluded.bucket,
	key = excluded.key,
	etag = excluded.etag,
	size = excluded.size,
	page_size = excluded.page_size,
	headers_json = excluded.headers_json,
	epoch = excluded.epoch,
	updated_at = excluded.updated_at
`, id, meta.Bucket, meta.Key, meta.ETag, meta.Size, meta.PageSize, headersJSON, epoch, time.Now().UnixNano())
	if err != nil {
		return Object{}, fmt.Errorf("put object: %w", err)
	}

	obj, ok, err := c.GetObject(ctx, meta.Bucket, meta.Key)
	if err != nil {
		return Object{}, err
	}
	if !ok {
		return Object{}, errors.New("object was not readable after insert")
	}
	return obj, nil
}

func (c *Cache) GetObject(ctx context.Context, bucket, key string) (Object, bool, error) {
	row := c.db.QueryRowContext(ctx, `
SELECT id, bucket, key, etag, size, page_size, headers_json, epoch
FROM objects
WHERE id = ?
`, ObjectKey(bucket, key))

	var obj Object
	var headersJSON string
	if err := row.Scan(&obj.ID, &obj.Bucket, &obj.Key, &obj.ETag, &obj.Size, &obj.PageSize, &headersJSON, &obj.Epoch); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Object{}, false, nil
		}
		return Object{}, false, fmt.Errorf("get object: %w", err)
	}

	headers, err := unmarshalHeaders(headersJSON)
	if err != nil {
		return Object{}, false, err
	}
	obj.Headers = headers
	return obj, true, nil
}

func (c *Cache) DeleteObject(ctx context.Context, bucket, key string) error {
	obj, ok, err := c.GetObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	pages, err := c.ListPages(ctx, obj.ID)
	if err != nil {
		return err
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete object: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO object_generations (id, epoch)
VALUES (?, 0)
ON CONFLICT(id) DO NOTHING
`, obj.ID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("ensure object generation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE object_generations SET epoch = epoch + 1 WHERE id = ?`, obj.ID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("bump object generation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE id = ?`, obj.ID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete object: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete object: %w", err)
	}

	for _, page := range pages {
		if err := os.Remove(filepath.Join(c.cacheRoot, page.Path)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete page file: %w", err)
		}
	}
	c.updateCacheSizeMetrics(ctx)

	return nil
}

func (c *Cache) PutPage(ctx context.Context, page Page) error {
	if err := validatePage(page); err != nil {
		return err
	}

	_, err := c.db.ExecContext(ctx, `
INSERT INTO pages (object_id, page_index, etag, size, path, created_at, last_accessed_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(object_id, page_index) DO UPDATE SET
	etag = excluded.etag,
	size = excluded.size,
	path = excluded.path,
	last_accessed_at = excluded.last_accessed_at
`, page.ObjectID, page.Index, page.ETag, page.Size, page.Path, time.Now().UnixNano(), time.Now().UnixNano())
	if err != nil {
		return fmt.Errorf("put page: %w", err)
	}
	return nil
}

func (c *Cache) ListPages(ctx context.Context, objectID string) ([]Page, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT object_id, page_index, etag, size, path
FROM pages
WHERE object_id = ?
ORDER BY page_index
`, objectID)
	if err != nil {
		return nil, fmt.Errorf("list pages: %w", err)
	}
	defer rows.Close()

	var pages []Page
	for rows.Next() {
		var page Page
		if err := rows.Scan(&page.ObjectID, &page.Index, &page.ETag, &page.Size, &page.Path); err != nil {
			return nil, fmt.Errorf("scan page: %w", err)
		}
		pages = append(pages, page)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pages: %w", err)
	}

	return pages, nil
}

func (c *Cache) StorePage(ctx context.Context, write PageWrite) (Page, error) {
	if write.ObjectID == "" {
		return Page{}, errors.New("object id is required")
	}
	if write.Index < 0 {
		return Page{}, errors.New("page index must not be negative")
	}
	if write.ETag == "" {
		return Page{}, errors.New("etag is required")
	}
	if int64(len(write.Data)) > c.maxSize {
		return Page{}, fmt.Errorf("page size %d exceeds cache max size %d", len(write.Data), c.maxSize)
	}
	if err := c.validatePageCommit(ctx, write.ObjectID, write.ETag, write.ExpectedEpoch); err != nil {
		return Page{}, err
	}

	relPath := c.pagePath(write.ObjectID, write.Index)
	absPath := filepath.Join(c.cacheRoot, relPath)
	dir := filepath.Dir(absPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Page{}, fmt.Errorf("create page directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, fmt.Sprintf(".page-%012d-*.tmp", write.Index))
	if err != nil {
		return Page{}, fmt.Errorf("create temp page: %w", err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(write.Data); err != nil {
		_ = tmp.Close()
		return Page{}, fmt.Errorf("write temp page: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return Page{}, fmt.Errorf("sync temp page: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Page{}, fmt.Errorf("close temp page: %w", err)
	}
	if err := os.Rename(tmpPath, absPath); err != nil {
		return Page{}, fmt.Errorf("commit page file: %w", err)
	}
	committed = true

	page := Page{
		ObjectID: write.ObjectID,
		Index:    write.Index,
		ETag:     write.ETag,
		Size:     int64(len(write.Data)),
		Path:     relPath,
	}
	if err := c.putPageIfCurrent(ctx, page, write.ExpectedEpoch); err != nil {
		_ = os.Remove(absPath)
		return Page{}, err
	}
	c.updateCacheSizeMetrics(ctx)
	c.requestEviction()

	return page, nil
}

func (c *Cache) OpenPage(ctx context.Context, objectID string, index int64) (io.ReadCloser, bool, error) {
	page, ok, err := c.getPage(ctx, objectID, index)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	absPath := filepath.Join(c.cacheRoot, page.Path)
	info, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if deleteErr := c.deletePage(ctx, objectID, index); deleteErr != nil {
				return nil, false, deleteErr
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat page file: %w", err)
	}
	if info.Size() != page.Size {
		if err := c.deletePage(ctx, objectID, index); err != nil {
			return nil, false, err
		}
		if err := os.Remove(absPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, false, fmt.Errorf("delete corrupt page file: %w", err)
		}
		return nil, false, nil
	}

	file, err := os.Open(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if deleteErr := c.deletePage(ctx, objectID, index); deleteErr != nil {
				return nil, false, deleteErr
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("open page file: %w", err)
	}
	c.requestTouch(objectID, index)

	return file, true, nil
}

func (c *Cache) CurrentSize(ctx context.Context) (int64, error) {
	row := c.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size), 0) FROM pages`)
	var size int64
	if err := row.Scan(&size); err != nil {
		return 0, fmt.Errorf("read cache size: %w", err)
	}
	return size, nil
}

func (c *Cache) CurrentSizeByBucket(ctx context.Context) (map[string]int64, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT o.bucket, COALESCE(SUM(p.size), 0)
FROM pages p
JOIN objects o ON o.id = p.object_id
GROUP BY o.bucket
`)
	if err != nil {
		return nil, fmt.Errorf("read cache size by bucket: %w", err)
	}
	defer rows.Close()

	sizes := map[string]int64{}
	for rows.Next() {
		var bucket string
		var size int64
		if err := rows.Scan(&bucket, &size); err != nil {
			return nil, fmt.Errorf("scan cache size by bucket: %w", err)
		}
		sizes[bucket] = size
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read cache size by bucket: %w", err)
	}
	return sizes, nil
}

func (c *Cache) Evict(ctx context.Context) error {
	for {
		size, err := c.CurrentSize(ctx)
		if err != nil {
			return err
		}
		if size <= c.maxSize {
			return nil
		}

		page, ok, err := c.oldestPage(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := c.removePage(ctx, page); err != nil {
			return err
		}
	}
}

func (c *Cache) pagePath(objectID string, index int64) string {
	return filepath.Join("objects", objectID[:2], objectID[2:4], objectID, fmt.Sprintf("page-%012d", index))
}

func (c *Cache) init(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE IF NOT EXISTS object_generations (
			id TEXT PRIMARY KEY,
			epoch INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS objects (
			id TEXT PRIMARY KEY,
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			etag TEXT NOT NULL,
			size INTEGER NOT NULL,
			page_size INTEGER NOT NULL,
			headers_json TEXT NOT NULL,
			epoch INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL,
			UNIQUE(bucket, key)
		)`,
		`CREATE TABLE IF NOT EXISTS pages (
			object_id TEXT NOT NULL,
			page_index INTEGER NOT NULL,
			etag TEXT NOT NULL,
			size INTEGER NOT NULL,
			path TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			last_accessed_at INTEGER NOT NULL,
			PRIMARY KEY (object_id, page_index),
			FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE
		)`,
		`INSERT OR IGNORE INTO object_generations (id, epoch)
			SELECT id, epoch FROM objects`,
		`CREATE INDEX IF NOT EXISTS pages_object_idx ON pages(object_id)`,
	}

	for _, statement := range statements {
		if _, err := c.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("init cache db: %w", err)
		}
	}
	return nil
}

func (c *Cache) updateCacheSizeMetrics(ctx context.Context) {
	if c.metrics == nil {
		return
	}
	total, err := c.CurrentSize(ctx)
	if err != nil {
		return
	}
	byBucket, err := c.CurrentSizeByBucket(ctx)
	if err != nil {
		return
	}
	c.metrics.SetCachedBytes(total, byBucket)
}

func (c *Cache) ensureGeneration(ctx context.Context, objectID string) error {
	_, err := c.db.ExecContext(ctx, `
INSERT INTO object_generations (id, epoch)
VALUES (?, 0)
ON CONFLICT(id) DO NOTHING
`, objectID)
	if err != nil {
		return fmt.Errorf("ensure object generation: %w", err)
	}
	return nil
}

func (c *Cache) generation(ctx context.Context, objectID string) (int64, error) {
	row := c.db.QueryRowContext(ctx, `SELECT epoch FROM object_generations WHERE id = ?`, objectID)
	var epoch int64
	if err := row.Scan(&epoch); err != nil {
		return 0, fmt.Errorf("read object generation: %w", err)
	}
	return epoch, nil
}

func (c *Cache) validatePageCommit(ctx context.Context, objectID, etag string, expectedEpoch int64) error {
	var currentETag string
	var currentEpoch int64
	err := c.db.QueryRowContext(ctx, `
SELECT o.etag, g.epoch
FROM objects o
JOIN object_generations g ON g.id = o.id
WHERE o.id = ?
`, objectID).Scan(&currentETag, &currentEpoch)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("object metadata is not current")
		}
		return fmt.Errorf("validate page commit: %w", err)
	}
	if currentEpoch != expectedEpoch {
		return fmt.Errorf("object epoch changed from %d to %d", expectedEpoch, currentEpoch)
	}
	if currentETag != etag {
		return fmt.Errorf("object etag changed from %s to %s", etag, currentETag)
	}
	return nil
}

func (c *Cache) putPageIfCurrent(ctx context.Context, page Page, expectedEpoch int64) error {
	if err := validatePage(page); err != nil {
		return err
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin put page: %w", err)
	}
	var currentETag string
	var currentEpoch int64
	err = tx.QueryRowContext(ctx, `
SELECT o.etag, g.epoch
FROM objects o
JOIN object_generations g ON g.id = o.id
WHERE o.id = ?
`, page.ObjectID).Scan(&currentETag, &currentEpoch)
	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("object metadata is not current")
		}
		return fmt.Errorf("validate page commit: %w", err)
	}
	if currentEpoch != expectedEpoch {
		_ = tx.Rollback()
		return fmt.Errorf("object epoch changed from %d to %d", expectedEpoch, currentEpoch)
	}
	if currentETag != page.ETag {
		_ = tx.Rollback()
		return fmt.Errorf("object etag changed from %s to %s", page.ETag, currentETag)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO pages (object_id, page_index, etag, size, path, created_at, last_accessed_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(object_id, page_index) DO UPDATE SET
	etag = excluded.etag,
	size = excluded.size,
	path = excluded.path,
	last_accessed_at = excluded.last_accessed_at
`, page.ObjectID, page.Index, page.ETag, page.Size, page.Path, time.Now().UnixNano(), time.Now().UnixNano())
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("put page: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit put page: %w", err)
	}
	return nil
}

func (c *Cache) getPage(ctx context.Context, objectID string, index int64) (Page, bool, error) {
	row := c.db.QueryRowContext(ctx, `
SELECT object_id, page_index, etag, size, path
FROM pages
WHERE object_id = ? AND page_index = ?
`, objectID, index)

	var page Page
	if err := row.Scan(&page.ObjectID, &page.Index, &page.ETag, &page.Size, &page.Path); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Page{}, false, nil
		}
		return Page{}, false, fmt.Errorf("get page: %w", err)
	}
	return page, true, nil
}

func (c *Cache) deletePage(ctx context.Context, objectID string, index int64) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM pages WHERE object_id = ? AND page_index = ?`, objectID, index)
	if err != nil {
		return fmt.Errorf("delete stale page row: %w", err)
	}
	return nil
}

func (c *Cache) touchPage(ctx context.Context, objectID string, index int64, at int64) error {
	_, err := c.db.ExecContext(ctx, `UPDATE pages SET last_accessed_at = ? WHERE object_id = ? AND page_index = ?`, at, objectID, index)
	if err != nil {
		return fmt.Errorf("touch page: %w", err)
	}
	return nil
}

func (c *Cache) oldestPage(ctx context.Context) (Page, bool, error) {
	row := c.db.QueryRowContext(ctx, `
SELECT object_id, page_index, etag, size, path
FROM pages
ORDER BY last_accessed_at ASC, created_at ASC
LIMIT 1
`)

	var page Page
	if err := row.Scan(&page.ObjectID, &page.Index, &page.ETag, &page.Size, &page.Path); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Page{}, false, nil
		}
		return Page{}, false, fmt.Errorf("select eviction candidate: %w", err)
	}
	return page, true, nil
}

func (c *Cache) removePage(ctx context.Context, page Page) error {
	bucket, _ := c.pageBucket(ctx, page.ObjectID)
	if err := c.deletePage(ctx, page.ObjectID, page.Index); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(c.cacheRoot, page.Path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete evicted page file: %w", err)
	}
	if c.metrics != nil {
		c.metrics.RecordEviction(bucket)
	}
	c.updateCacheSizeMetrics(ctx)
	return nil
}

func (c *Cache) pageBucket(ctx context.Context, objectID string) (string, error) {
	row := c.db.QueryRowContext(ctx, `SELECT bucket FROM objects WHERE id = ?`, objectID)
	var bucket string
	if err := row.Scan(&bucket); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("read page bucket: %w", err)
	}
	return bucket, nil
}

func (c *Cache) requestEviction() {
	select {
	case c.evictionCh <- struct{}{}:
	default:
	}
}

func (c *Cache) requestTouch(objectID string, index int64) {
	select {
	case c.touchCh <- pageRef{objectID: objectID, index: index}:
	default:
	}
}

func (c *Cache) maintenanceLoop(ctx context.Context) {
	defer c.workerWG.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	pendingTouches := map[pageRef]struct{}{}

	for {
		select {
		case <-ctx.Done():
			_ = c.flushTouches(context.Background(), pendingTouches)
			return
		case <-c.evictionCh:
			_ = c.Evict(ctx)
		case ref := <-c.touchCh:
			pendingTouches[ref] = struct{}{}
			if len(pendingTouches) >= 256 {
				_ = c.flushTouches(ctx, pendingTouches)
			}
		case <-ticker.C:
			_ = c.flushTouches(ctx, pendingTouches)
		}
	}
}

func (c *Cache) flushTouches(ctx context.Context, pending map[pageRef]struct{}) error {
	if len(pending) == 0 {
		return nil
	}

	now := time.Now().UnixNano()
	for ref := range pending {
		if err := c.touchPage(ctx, ref.objectID, ref.index, now); err != nil {
			return err
		}
		delete(pending, ref)
	}
	return nil
}

func validatePage(page Page) error {
	if page.ObjectID == "" {
		return errors.New("object id is required")
	}
	if page.Index < 0 {
		return errors.New("page index must not be negative")
	}
	if page.ETag == "" {
		return errors.New("etag is required")
	}
	if page.Size < 0 {
		return errors.New("page size must not be negative")
	}
	if page.Path == "" {
		return errors.New("page path is required")
	}
	if !filepath.IsLocal(page.Path) {
		return errors.New("page path must be cache-relative")
	}
	return nil
}

func marshalHeaders(headers http.Header) (string, error) {
	if headers == nil {
		headers = http.Header{}
	}
	data, err := json.Marshal(headers)
	if err != nil {
		return "", fmt.Errorf("marshal headers: %w", err)
	}
	return string(data), nil
}

func unmarshalHeaders(input string) (http.Header, error) {
	headers := http.Header{}
	if input == "" {
		return headers, nil
	}
	if err := json.Unmarshal([]byte(input), &headers); err != nil {
		return nil, fmt.Errorf("unmarshal headers: %w", err)
	}
	return headers, nil
}
