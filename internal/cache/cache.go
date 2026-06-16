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
	"time"

	_ "modernc.org/sqlite"
)

type Options struct {
	CachePath string
	MetaPath  string
}

type Cache struct {
	cacheRoot string
	metaRoot  string
	db        *sql.DB
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

func Open(ctx context.Context, opts Options) (*Cache, error) {
	if opts.CachePath == "" {
		return nil, errors.New("cache path is required")
	}
	if opts.MetaPath == "" {
		return nil, errors.New("metadata path is required")
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

	c := &Cache{cacheRoot: opts.CachePath, metaRoot: opts.MetaPath, db: db}
	if err := c.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return c, nil
}

func (c *Cache) Close() error {
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

	file, err := os.Open(filepath.Join(c.cacheRoot, page.Path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if deleteErr := c.deletePage(ctx, objectID, index); deleteErr != nil {
				return nil, false, deleteErr
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("open page file: %w", err)
	}

	return file, true, nil
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
