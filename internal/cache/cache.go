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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ekkuleivonen/simple-s3-cache/internal/metrics"

	_ "modernc.org/sqlite"
)

const (
	cacheDBMaxOpenConns    = 8
	cacheDBMaxIdleConns    = 8
	evictionBatchSize      = 128
	metricsRefreshInterval = 5 * time.Second
	metadataGCInterval     = time.Hour
	metadataMaxAge         = 24 * time.Hour
	metadataGCBatchSize    = 512
	sqliteCheckpointPeriod = 6 * time.Hour
	touchFlushInterval     = time.Second
	touchFlushBatchSize    = 256
	touchQueueSize         = 1024
)

type Options struct {
	CachePath                string
	MetaPath                 string
	MaxSize                  int64
	MaxSizeByBucket          map[string]int64
	MetadataGCInterval       time.Duration
	MetadataMaxAge           time.Duration
	MetadataGCBatchSize      int
	SQLiteCheckpointInterval time.Duration
	Metrics                  *metrics.Recorder
}

type Cache struct {
	cacheRoot                string
	metaRoot                 string
	maxSize                  int64
	maxSizeByBucket          map[string]int64
	metadataGCInterval       time.Duration
	metadataMaxAge           time.Duration
	metadataGCBatchSize      int
	sqliteCheckpointInterval time.Duration
	db                       *sql.DB
	evictionCh               chan struct{}
	touchCh                  chan pageRef
	metricsCh                chan struct{}
	writeMu                  sync.Mutex
	cancelWorker             context.CancelFunc
	workerWG                 sync.WaitGroup
	metrics                  *metrics.Recorder
	sizeMu                   sync.RWMutex
	cachedSize               int64
	cachedByBucket           map[string]int64
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
	Size          int64
	Source        io.Reader
}

type pageRef struct {
	objectID string
	index    int64
}

type metadataGCResult struct {
	ObjectsDeleted     int64
	GenerationsDeleted int64
}

var storePageAfterValidationHook func()

func openCacheDB(dbPath string) (*sql.DB, error) {
	values := url.Values{}
	values.Add("_pragma", "journal_mode(WAL)")
	values.Add("_pragma", "busy_timeout(5000)")
	values.Add("_pragma", "foreign_keys(1)")

	dsn := (&url.URL{
		Scheme:   "file",
		Path:     dbPath,
		RawQuery: values.Encode(),
	}).String()

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cacheDBMaxOpenConns)
	db.SetMaxIdleConns(cacheDBMaxIdleConns)
	return db, nil
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
	if opts.MetadataGCInterval <= 0 {
		opts.MetadataGCInterval = metadataGCInterval
	}
	if opts.MetadataMaxAge <= 0 {
		opts.MetadataMaxAge = metadataMaxAge
	}
	if opts.MetadataGCBatchSize <= 0 {
		opts.MetadataGCBatchSize = metadataGCBatchSize
	}
	if opts.SQLiteCheckpointInterval <= 0 {
		opts.SQLiteCheckpointInterval = sqliteCheckpointPeriod
	}
	if err := os.MkdirAll(filepath.Join(opts.CachePath, "objects"), 0o755); err != nil {
		return nil, fmt.Errorf("create cache directories: %w", err)
	}
	if err := os.MkdirAll(opts.MetaPath, 0o755); err != nil {
		return nil, fmt.Errorf("create metadata directory: %w", err)
	}

	dbPath := filepath.Join(opts.MetaPath, "cache.db")
	db, err := openCacheDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open cache db: %w", err)
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	c := &Cache{
		cacheRoot:                opts.CachePath,
		metaRoot:                 opts.MetaPath,
		maxSize:                  opts.MaxSize,
		maxSizeByBucket:          copyMaxSizeByBucket(opts.MaxSizeByBucket),
		metadataGCInterval:       opts.MetadataGCInterval,
		metadataMaxAge:           opts.MetadataMaxAge,
		metadataGCBatchSize:      opts.MetadataGCBatchSize,
		sqliteCheckpointInterval: opts.SQLiteCheckpointInterval,
		db:                       db,
		evictionCh:               make(chan struct{}, 1),
		touchCh:                  make(chan pageRef, touchQueueSize),
		metricsCh:                make(chan struct{}, 1),
		cancelWorker:             cancelWorker,
		metrics:                  opts.Metrics,
		cachedByBucket:           map[string]int64{},
	}
	initErr := c.init(ctx)
	if initErr != nil && isCorruptDatabaseError(initErr) {
		_ = db.Close()
		if resetErr := resetLocalCacheState(opts.CachePath, dbPath); resetErr != nil {
			cancelWorker()
			return nil, resetErr
		}
		db, err = openCacheDB(dbPath)
		if err != nil {
			cancelWorker()
			return nil, fmt.Errorf("open cache db after reset: %w", err)
		}
		c.db = db
		initErr = c.init(ctx)
	}
	if initErr != nil {
		cancelWorker()
		if c.db != nil {
			_ = db.Close()
		}
		return nil, initErr
	}

	total, byBucket, err := c.readCacheSize(ctx)
	if err != nil {
		cancelWorker()
		_ = c.db.Close()
		return nil, err
	}
	c.setCacheSize(total, byBucket)
	c.publishCacheSizeMetrics()

	c.workerWG.Add(1)
	go c.maintenanceLoop(workerCtx)

	return c, nil
}

func copyMaxSizeByBucket(input map[string]int64) map[string]int64 {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]int64, len(input))
	for bucket, size := range input {
		if size > 0 {
			out[bucket] = size
		}
	}
	return out
}

func (c *Cache) Close() error {
	c.cancelWorker()
	c.workerWG.Wait()
	return c.db.Close()
}

func (c *Cache) execWrite(ctx context.Context, query string, args ...any) (sql.Result, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.db.ExecContext(ctx, query, args...)
}

func (c *Cache) beginWriteTx(ctx context.Context) (*sql.Tx, func(), error) {
	c.writeMu.Lock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		c.writeMu.Unlock()
		return nil, nil, err
	}
	return tx, c.writeMu.Unlock, nil
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

	_, err = c.execWrite(ctx, `
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

	now := time.Now().UnixNano()
	tx, unlock, err := c.beginWriteTx(ctx)
	if err != nil {
		return fmt.Errorf("begin delete object: %w", err)
	}
	defer unlock()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO object_generations (id, epoch, updated_at)
VALUES (?, 0, ?)
ON CONFLICT(id) DO NOTHING
`, obj.ID, now); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("ensure object generation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE object_generations SET epoch = epoch + 1, updated_at = ? WHERE id = ?`, now, obj.ID); err != nil {
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
	c.adjustCacheSize(obj.Bucket, -pagesSize(pages))
	c.requestMetricsRefresh()

	return nil
}

func (c *Cache) PutPage(ctx context.Context, page Page) error {
	if err := validatePage(page); err != nil {
		return err
	}

	previous, hadPrevious, err := c.getPage(ctx, page.ObjectID, page.Index)
	if err != nil {
		return err
	}
	_, err = c.execWrite(ctx, `
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
	bucket, _ := c.pageBucket(ctx, page.ObjectID)
	delta := page.Size
	if hadPrevious {
		delta -= previous.Size
	}
	c.adjustCacheSize(bucket, delta)
	c.requestMetricsRefresh()
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
	if write.Size < 0 {
		return Page{}, errors.New("page size must not be negative")
	}
	if write.Source == nil {
		return Page{}, errors.New("page source is required")
	}
	if write.Size > c.maxSize {
		return Page{}, fmt.Errorf("page size %d exceeds cache max size %d", write.Size, c.maxSize)
	}
	if err := c.validatePageCommit(ctx, write.ObjectID, write.ETag, write.ExpectedEpoch); err != nil {
		return Page{}, err
	}
	bucket, err := c.pageBucket(ctx, write.ObjectID)
	if err != nil {
		return Page{}, err
	}
	if maxSize := c.maxSizeForBucket(bucket); maxSize > 0 && write.Size > maxSize {
		return Page{}, fmt.Errorf("page size %d exceeds cache max size %d for bucket %q", write.Size, maxSize, bucket)
	}
	if storePageAfterValidationHook != nil {
		storePageAfterValidationHook()
	}

	relPath := c.pagePathForVersion(write.ObjectID, write.Index, write.ETag, write.ExpectedEpoch)
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

	written, err := io.Copy(tmp, write.Source)
	if err != nil {
		_ = tmp.Close()
		return Page{}, fmt.Errorf("write temp page: %w", err)
	}
	if written != write.Size {
		_ = tmp.Close()
		return Page{}, fmt.Errorf("write temp page: got %d bytes, want %d", written, write.Size)
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
		Size:     write.Size,
		Path:     relPath,
	}
	bucket, previousSize, err := c.putPageIfCurrent(ctx, page, write.ExpectedEpoch)
	if err != nil {
		_ = os.Remove(absPath)
		return Page{}, err
	}
	c.adjustCacheSize(bucket, page.Size-previousSize)
	c.requestMetricsRefresh()
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
			bucket, _ := c.pageBucket(ctx, objectID)
			deleted, deleteErr := c.deletePage(ctx, objectID, index)
			if deleteErr != nil {
				return nil, false, deleteErr
			}
			if deleted {
				c.adjustCacheSize(bucket, -page.Size)
				c.requestMetricsRefresh()
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat page file: %w", err)
	}
	if info.Size() != page.Size {
		bucket, _ := c.pageBucket(ctx, objectID)
		deleted, err := c.deletePage(ctx, objectID, index)
		if err != nil {
			return nil, false, err
		}
		if err := os.Remove(absPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, false, fmt.Errorf("delete corrupt page file: %w", err)
		}
		if deleted {
			c.adjustCacheSize(bucket, -page.Size)
			c.requestMetricsRefresh()
		}
		return nil, false, nil
	}

	file, err := os.Open(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			bucket, _ := c.pageBucket(ctx, objectID)
			deleted, deleteErr := c.deletePage(ctx, objectID, index)
			if deleteErr != nil {
				return nil, false, deleteErr
			}
			if deleted {
				c.adjustCacheSize(bucket, -page.Size)
				c.requestMetricsRefresh()
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("open page file: %w", err)
	}
	c.requestTouch(objectID, index)

	return file, true, nil
}

func (c *Cache) CurrentSize(ctx context.Context) (int64, error) {
	return c.currentSize(), nil
}

func (c *Cache) CurrentSizeByBucket(ctx context.Context) (map[string]int64, error) {
	return c.currentSizeByBucket(), nil
}

func (c *Cache) readCacheSize(ctx context.Context) (int64, map[string]int64, error) {
	row := c.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size), 0) FROM pages`)
	var total int64
	if err := row.Scan(&total); err != nil {
		return 0, nil, fmt.Errorf("read cache size: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, `
SELECT o.bucket, COALESCE(SUM(p.size), 0)
FROM pages p
JOIN objects o ON o.id = p.object_id
GROUP BY o.bucket
`)
	if err != nil {
		return 0, nil, fmt.Errorf("read cache size by bucket: %w", err)
	}
	defer rows.Close()

	byBucket := map[string]int64{}
	for rows.Next() {
		var bucket string
		var size int64
		if err := rows.Scan(&bucket, &size); err != nil {
			return 0, nil, fmt.Errorf("scan cache size by bucket: %w", err)
		}
		byBucket[bucket] = size
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("read cache size by bucket: %w", err)
	}
	return total, byBucket, nil
}

func (c *Cache) setCacheSize(total int64, byBucket map[string]int64) {
	c.sizeMu.Lock()
	defer c.sizeMu.Unlock()

	c.cachedSize = total
	c.cachedByBucket = make(map[string]int64, len(byBucket))
	for bucket, size := range byBucket {
		c.cachedByBucket[bucket] = size
	}
}

func (c *Cache) currentSize() int64 {
	c.sizeMu.RLock()
	defer c.sizeMu.RUnlock()
	return c.cachedSize
}

func (c *Cache) currentSizeByBucket() map[string]int64 {
	c.sizeMu.RLock()
	defer c.sizeMu.RUnlock()

	byBucket := make(map[string]int64, len(c.cachedByBucket))
	for bucket, size := range c.cachedByBucket {
		byBucket[bucket] = size
	}
	return byBucket
}

func (c *Cache) maxSizeForBucket(bucket string) int64 {
	if c.maxSizeByBucket == nil {
		return 0
	}
	return c.maxSizeByBucket[bucket]
}

func (c *Cache) adjustCacheSize(bucket string, delta int64) {
	if delta == 0 {
		return
	}

	c.sizeMu.Lock()
	defer c.sizeMu.Unlock()

	c.cachedSize += delta
	if c.cachedSize < 0 {
		c.cachedSize = 0
	}
	if bucket != "" {
		c.cachedByBucket[bucket] += delta
		if c.cachedByBucket[bucket] <= 0 {
			delete(c.cachedByBucket, bucket)
		}
	}
}

func (c *Cache) publishCacheSizeMetrics() {
	if c.metrics == nil {
		return
	}
	c.metrics.SetCachedBytes(c.currentSize(), c.currentSizeByBucket())
}

func (c *Cache) requestMetricsRefresh() {
	if c.metrics == nil {
		return
	}
	select {
	case c.metricsCh <- struct{}{}:
	default:
	}
}

func pagesSize(pages []Page) int64 {
	var size int64
	for _, page := range pages {
		size += page.Size
	}
	return size
}

func (c *Cache) Evict(ctx context.Context) error {
	if err := c.evictBuckets(ctx); err != nil {
		return err
	}
	for {
		size := c.currentSize()
		if size <= c.maxSize {
			c.requestMetricsRefresh()
			return nil
		}

		pages, err := c.oldestPages(ctx, evictionBatchSize)
		if err != nil {
			return err
		}
		if len(pages) == 0 {
			return nil
		}
		targetBytes := size - c.maxSize
		var removedBytes int64
		for _, page := range pages {
			if err := c.removePage(ctx, page); err != nil {
				return err
			}
			removedBytes += page.Size
			if removedBytes >= targetBytes {
				break
			}
		}
	}
}

func (c *Cache) evictBuckets(ctx context.Context) error {
	for bucket, maxSize := range c.maxSizeByBucket {
		for {
			size := c.currentSizeByBucket()[bucket]
			if size <= maxSize {
				break
			}
			pages, err := c.oldestPagesForBucket(ctx, bucket, evictionBatchSize)
			if err != nil {
				return err
			}
			if len(pages) == 0 {
				break
			}
			targetBytes := size - maxSize
			var removedBytes int64
			for _, page := range pages {
				if err := c.removePage(ctx, page); err != nil {
					return err
				}
				removedBytes += page.Size
				if removedBytes >= targetBytes {
					break
				}
			}
		}
	}
	return nil
}

func (c *Cache) collectMetadata(ctx context.Context) (metadataGCResult, error) {
	cutoff := time.Now().Add(-c.metadataMaxAge).UnixNano()
	objectsDeleted, err := c.deleteOldPageLessObjects(ctx, cutoff, c.metadataGCBatchSize)
	if err != nil {
		return metadataGCResult{}, err
	}
	generationsDeleted, err := c.deleteOldUnreferencedGenerations(ctx, cutoff, c.metadataGCBatchSize)
	if err != nil {
		return metadataGCResult{}, err
	}
	return metadataGCResult{
		ObjectsDeleted:     objectsDeleted,
		GenerationsDeleted: generationsDeleted,
	}, nil
}

func (c *Cache) deleteOldPageLessObjects(ctx context.Context, olderThan int64, limit int) (int64, error) {
	result, err := c.execWrite(ctx, `
WITH victims(id) AS (
	SELECT o.id
	FROM objects o
	WHERE o.updated_at < ?
		AND NOT EXISTS (
			SELECT 1
			FROM pages p
			WHERE p.object_id = o.id
		)
	LIMIT ?
)
DELETE FROM objects
WHERE id IN (SELECT id FROM victims)
`, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old page-less objects: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read old page-less object count: %w", err)
	}
	return rows, nil
}

func (c *Cache) deleteOldUnreferencedGenerations(ctx context.Context, olderThan int64, limit int) (int64, error) {
	result, err := c.execWrite(ctx, `
WITH victims(id) AS (
	SELECT g.id
	FROM object_generations g
	WHERE g.updated_at < ?
		AND NOT EXISTS (
			SELECT 1
			FROM objects o
			WHERE o.id = g.id
		)
	LIMIT ?
)
DELETE FROM object_generations
WHERE id IN (SELECT id FROM victims)
`, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old object generations: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read old object generation count: %w", err)
	}
	return rows, nil
}

func (c *Cache) checkpointSQLite(ctx context.Context) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	rows, err := c.db.QueryContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`)
	if err != nil {
		return fmt.Errorf("checkpoint cache db: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("checkpoint cache db: %w", err)
	}
	return nil
}

func (c *Cache) pagePath(objectID string, index int64) string {
	return filepath.Join("objects", objectID[:2], objectID[2:4], objectID, fmt.Sprintf("page-%012d", index))
}

func (c *Cache) pagePathForVersion(objectID string, index int64, etag string, epoch int64) string {
	sum := sha256.Sum256([]byte(etag))
	version := fmt.Sprintf("epoch-%020d-etag-%s", epoch, hex.EncodeToString(sum[:]))
	return filepath.Join("objects", objectID[:2], objectID[2:4], objectID, version, fmt.Sprintf("page-%012d", index))
}

func isCorruptDatabaseError(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "file is not a database") ||
		strings.Contains(text, "database disk image is malformed")
}

func resetLocalCacheState(cachePath, dbPath string) error {
	if err := os.RemoveAll(filepath.Join(cachePath, "objects")); err != nil {
		return fmt.Errorf("reset cache objects after corrupt db: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cachePath, "objects"), 0o755); err != nil {
		return fmt.Errorf("recreate cache objects after corrupt db: %w", err)
	}
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove corrupt cache db file: %w", err)
		}
	}
	return nil
}

func (c *Cache) init(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE IF NOT EXISTS object_generations (
			id TEXT PRIMARY KEY,
			epoch INTEGER NOT NULL,
			updated_at INTEGER NOT NULL DEFAULT 0
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
		`CREATE INDEX IF NOT EXISTS pages_object_idx ON pages(object_id)`,
		`CREATE INDEX IF NOT EXISTS pages_lru_idx ON pages(last_accessed_at, created_at)`,
	}

	for _, statement := range statements {
		if _, err := c.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("init cache db: %w", err)
		}
	}
	if err := c.ensureObjectGenerationUpdatedAtColumn(ctx); err != nil {
		return err
	}
	if _, err := c.db.ExecContext(ctx, `
INSERT OR IGNORE INTO object_generations (id, epoch, updated_at)
SELECT id, epoch, updated_at FROM objects
`); err != nil {
		return fmt.Errorf("init object generations: %w", err)
	}
	return nil
}

func (c *Cache) ensureObjectGenerationUpdatedAtColumn(ctx context.Context) error {
	rows, err := c.db.QueryContext(ctx, `PRAGMA table_info(object_generations)`)
	if err != nil {
		return fmt.Errorf("inspect object generations schema: %w", err)
	}
	defer rows.Close()

	hasUpdatedAt := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("scan object generations schema: %w", err)
		}
		if name == "updated_at" {
			hasUpdatedAt = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect object generations schema: %w", err)
	}
	if hasUpdatedAt {
		return nil
	}

	if _, err := c.execWrite(ctx, `ALTER TABLE object_generations ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`); err != nil {
		return fmt.Errorf("migrate object generations schema: %w", err)
	}
	return nil
}

func (c *Cache) ensureGeneration(ctx context.Context, objectID string) error {
	_, err := c.execWrite(ctx, `
INSERT INTO object_generations (id, epoch, updated_at)
VALUES (?, 0, ?)
ON CONFLICT(id) DO NOTHING
`, objectID, time.Now().UnixNano())
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

func (c *Cache) putPageIfCurrent(ctx context.Context, page Page, expectedEpoch int64) (string, int64, error) {
	if err := validatePage(page); err != nil {
		return "", 0, err
	}

	tx, unlock, err := c.beginWriteTx(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("begin put page: %w", err)
	}
	defer unlock()
	var currentETag string
	var currentEpoch int64
	var bucket string
	err = tx.QueryRowContext(ctx, `
SELECT o.etag, g.epoch, o.bucket
FROM objects o
JOIN object_generations g ON g.id = o.id
WHERE o.id = ?
`, page.ObjectID).Scan(&currentETag, &currentEpoch, &bucket)
	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, errors.New("object metadata is not current")
		}
		return "", 0, fmt.Errorf("validate page commit: %w", err)
	}
	if currentEpoch != expectedEpoch {
		_ = tx.Rollback()
		return "", 0, fmt.Errorf("object epoch changed from %d to %d", expectedEpoch, currentEpoch)
	}
	if currentETag != page.ETag {
		_ = tx.Rollback()
		return "", 0, fmt.Errorf("object etag changed from %s to %s", page.ETag, currentETag)
	}
	var previousSize int64
	err = tx.QueryRowContext(ctx, `
SELECT size
FROM pages
WHERE object_id = ? AND page_index = ?
`, page.ObjectID, page.Index).Scan(&previousSize)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return "", 0, fmt.Errorf("read previous page size: %w", err)
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
		return "", 0, fmt.Errorf("put page: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", 0, fmt.Errorf("commit put page: %w", err)
	}
	return bucket, previousSize, nil
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

func (c *Cache) deletePage(ctx context.Context, objectID string, index int64) (bool, error) {
	result, err := c.execWrite(ctx, `DELETE FROM pages WHERE object_id = ? AND page_index = ?`, objectID, index)
	if err != nil {
		return false, fmt.Errorf("delete stale page row: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read deleted page count: %w", err)
	}
	return rows > 0, nil
}

func (c *Cache) touchPage(ctx context.Context, objectID string, index int64, at int64) error {
	_, err := c.execWrite(ctx, `UPDATE pages SET last_accessed_at = ? WHERE object_id = ? AND page_index = ?`, at, objectID, index)
	if err != nil {
		return fmt.Errorf("touch page: %w", err)
	}
	return nil
}

func (c *Cache) oldestPages(ctx context.Context, limit int) ([]Page, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT object_id, page_index, etag, size, path
FROM pages
ORDER BY last_accessed_at ASC, created_at ASC
LIMIT ?
`, limit)
	if err != nil {
		return nil, fmt.Errorf("select eviction candidates: %w", err)
	}
	defer rows.Close()

	var pages []Page
	for rows.Next() {
		var page Page
		if err := rows.Scan(&page.ObjectID, &page.Index, &page.ETag, &page.Size, &page.Path); err != nil {
			return nil, fmt.Errorf("scan eviction candidate: %w", err)
		}
		pages = append(pages, page)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select eviction candidates: %w", err)
	}
	return pages, nil
}

func (c *Cache) oldestPagesForBucket(ctx context.Context, bucket string, limit int) ([]Page, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT p.object_id, p.page_index, p.etag, p.size, p.path
FROM pages p
JOIN objects o ON o.id = p.object_id
WHERE o.bucket = ?
ORDER BY p.last_accessed_at ASC, p.created_at ASC
LIMIT ?
`, bucket, limit)
	if err != nil {
		return nil, fmt.Errorf("select bucket eviction candidates: %w", err)
	}
	defer rows.Close()

	var pages []Page
	for rows.Next() {
		var page Page
		if err := rows.Scan(&page.ObjectID, &page.Index, &page.ETag, &page.Size, &page.Path); err != nil {
			return nil, fmt.Errorf("scan bucket eviction candidate: %w", err)
		}
		pages = append(pages, page)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select bucket eviction candidates: %w", err)
	}
	return pages, nil
}

func (c *Cache) removePage(ctx context.Context, page Page) error {
	bucket, _ := c.pageBucket(ctx, page.ObjectID)
	deleted, err := c.deletePage(ctx, page.ObjectID, page.Index)
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(c.cacheRoot, page.Path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete evicted page file: %w", err)
	}
	if deleted {
		c.adjustCacheSize(bucket, -page.Size)
		c.requestMetricsRefresh()
	}
	if c.metrics != nil {
		c.metrics.RecordEviction(bucket)
	}
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

	touchTicker := time.NewTicker(touchFlushInterval)
	defer touchTicker.Stop()
	metricsTicker := time.NewTicker(metricsRefreshInterval)
	defer metricsTicker.Stop()
	metadataGCTicker := time.NewTicker(c.metadataGCInterval)
	defer metadataGCTicker.Stop()
	sqliteCheckpointTicker := time.NewTicker(c.sqliteCheckpointInterval)
	defer sqliteCheckpointTicker.Stop()
	pendingTouches := map[pageRef]struct{}{}
	metricsDirty := false

	for {
		select {
		case <-ctx.Done():
			_ = c.flushTouches(context.Background(), pendingTouches)
			c.publishCacheSizeMetrics()
			return
		case <-c.evictionCh:
			_ = c.Evict(ctx)
		case <-c.metricsCh:
			metricsDirty = true
		case ref := <-c.touchCh:
			pendingTouches[ref] = struct{}{}
			if len(pendingTouches) >= touchFlushBatchSize {
				_ = c.flushTouches(ctx, pendingTouches)
			}
		case <-touchTicker.C:
			_ = c.flushTouches(ctx, pendingTouches)
		case <-metadataGCTicker.C:
			_, _ = c.collectMetadata(ctx)
		case <-sqliteCheckpointTicker.C:
			_ = c.checkpointSQLite(ctx)
		case <-metricsTicker.C:
			if metricsDirty {
				c.publishCacheSizeMetrics()
				metricsDirty = false
			}
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
