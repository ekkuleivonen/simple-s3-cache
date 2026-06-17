package peer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	PageFrameContentType = "application/vnd.simple-s3-cache.pages.v1"
	PageFrameVersion     = uint16(1)
)

const (
	pageFrameHeaderSize = 6
	pageFrameMetaSize   = 16
	pageFrameTypePage   = byte(1)
	pageFrameTypeEnd    = byte(2)
)

var pageFrameMagic = [4]byte{'S', '3', 'P', 'F'}

var (
	ErrInvalidPageFrameVersion = errors.New("invalid page frame protocol version")
	ErrInvalidPageFrame        = errors.New("invalid page frame")
	ErrTruncatedPageFrame      = errors.New("truncated page frame")
	ErrOversizedPageFrame      = errors.New("oversized page frame")
	ErrDuplicatePageFrame      = errors.New("duplicate page frame")
	ErrUnexpectedPageFrame     = errors.New("unexpected page frame")
	ErrOutOfOrderPageFrame     = errors.New("out-of-order page frame")
	ErrUnexpectedEndFrame      = errors.New("unexpected end page frame")
	ErrPageFrameWriterClosed   = errors.New("page frame writer is closed")
)

type PageFrame struct {
	Index int64
	Bytes []byte
}

type PageFrameStream struct {
	Index int64
	Size  int64
	Body  io.ReadCloser
}

type PageFrameWriter struct {
	w         io.Writer
	lastIndex int64
	wrotePage bool
	closed    bool
}

// NewPageFrameWriter writes the v1 stream header immediately.
//
// Writes are synchronous and only buffer one frame header, so HTTP flow control
// provides backpressure. Cancellation is handled by the caller through the
// underlying writer or request context.
func NewPageFrameWriter(w io.Writer) (*PageFrameWriter, error) {
	if w == nil {
		return nil, errors.New("page frame writer is nil")
	}
	header := make([]byte, pageFrameHeaderSize)
	copy(header[:4], pageFrameMagic[:])
	binary.BigEndian.PutUint16(header[4:6], PageFrameVersion)
	if err := writePageFrameFull(w, header); err != nil {
		return nil, err
	}
	return &PageFrameWriter{w: w}, nil
}

func (w *PageFrameWriter) WritePage(index int64, data []byte) error {
	return w.WritePageFrom(index, int64(len(data)), bytes.NewReader(data))
}

func (w *PageFrameWriter) WritePageFrom(index int64, size int64, src io.Reader) error {
	if w.closed {
		return ErrPageFrameWriterClosed
	}
	if index < 0 {
		return fmt.Errorf("%w: negative page index %d", ErrInvalidPageFrame, index)
	}
	if size < 0 {
		return fmt.Errorf("%w: negative page size %d", ErrInvalidPageFrame, size)
	}
	if src == nil {
		return fmt.Errorf("%w: page source is nil", ErrInvalidPageFrame)
	}
	if w.wrotePage && index <= w.lastIndex {
		return fmt.Errorf("%w: page index %d after %d", ErrOutOfOrderPageFrame, index, w.lastIndex)
	}

	header := make([]byte, 1+pageFrameMetaSize)
	header[0] = pageFrameTypePage
	binary.BigEndian.PutUint64(header[1:9], uint64(index))
	binary.BigEndian.PutUint64(header[9:17], uint64(size))
	if err := writePageFrameFull(w.w, header); err != nil {
		return err
	}
	if size > 0 {
		written, err := io.CopyN(w.w, src, size)
		if err != nil {
			return err
		}
		if written != size {
			return io.ErrShortWrite
		}
	}
	w.lastIndex = index
	w.wrotePage = true
	return nil
}

func (w *PageFrameWriter) WriteEnd() error {
	if w.closed {
		return ErrPageFrameWriterClosed
	}
	if err := writePageFrameFull(w.w, []byte{pageFrameTypeEnd}); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func writePageFrameFull(w io.Writer, data []byte) error {
	written, err := w.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

type PageFrameReader struct {
	r            io.Reader
	expected     []int64
	expectedSet  map[int64]struct{}
	seen         map[int64]struct{}
	maxPageBytes int64
	next         int
	ended        bool
}

// NewPageFrameReader reads and validates the v1 stream header immediately.
//
// The reader accepts only the requested page indexes in increasing order. It
// rejects duplicates, unexpected pages, out-of-order pages, truncated frames,
// oversized frames, and streams that end without an explicit end marker.
func NewPageFrameReader(r io.Reader, expectedPages []int64, maxPageBytes int64) (*PageFrameReader, error) {
	if r == nil {
		return nil, errors.New("page frame reader is nil")
	}
	if maxPageBytes <= 0 {
		return nil, errors.New("max page bytes must be greater than zero")
	}
	expected := append([]int64(nil), expectedPages...)
	expectedSet := make(map[int64]struct{}, len(expected))
	for i, index := range expected {
		if index < 0 {
			return nil, fmt.Errorf("%w: negative expected page index %d", ErrInvalidPageFrame, index)
		}
		if i > 0 && index <= expected[i-1] {
			return nil, fmt.Errorf("%w: expected pages must be strictly increasing", ErrInvalidPageFrame)
		}
		expectedSet[index] = struct{}{}
	}

	header := make([]byte, pageFrameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTruncatedPageFrame, err)
	}
	if string(header[:4]) != string(pageFrameMagic[:]) {
		return nil, fmt.Errorf("%w: bad magic", ErrInvalidPageFrameVersion)
	}
	version := binary.BigEndian.Uint16(header[4:6])
	if version != PageFrameVersion {
		return nil, fmt.Errorf("%w: got %d want %d", ErrInvalidPageFrameVersion, version, PageFrameVersion)
	}

	return &PageFrameReader{
		r:            r,
		expected:     expected,
		expectedSet:  expectedSet,
		seen:         make(map[int64]struct{}, len(expected)),
		maxPageBytes: maxPageBytes,
	}, nil
}

func (r *PageFrameReader) NextPage() (PageFrame, error) {
	stream, err := r.NextPageStream()
	if err != nil {
		return PageFrame{}, err
	}
	defer stream.Body.Close()
	data, err := io.ReadAll(stream.Body)
	if err != nil {
		return PageFrame{}, err
	}
	return PageFrame{Index: stream.Index, Bytes: data}, nil
}

func (r *PageFrameReader) NextPageStream() (PageFrameStream, error) {
	if r.ended {
		return PageFrameStream{}, io.EOF
	}

	var frameType [1]byte
	if _, err := io.ReadFull(r.r, frameType[:]); err != nil {
		return PageFrameStream{}, fmt.Errorf("%w: missing end marker: %v", ErrTruncatedPageFrame, err)
	}
	switch frameType[0] {
	case pageFrameTypePage:
		return r.readPageStream()
	case pageFrameTypeEnd:
		if r.next != len(r.expected) {
			return PageFrameStream{}, fmt.Errorf("%w: got %d pages want %d", ErrUnexpectedEndFrame, r.next, len(r.expected))
		}
		r.ended = true
		return PageFrameStream{}, io.EOF
	default:
		return PageFrameStream{}, fmt.Errorf("%w: unknown frame type %d", ErrInvalidPageFrame, frameType[0])
	}
}

func (r *PageFrameReader) readPageStream() (PageFrameStream, error) {
	meta := make([]byte, pageFrameMetaSize)
	if _, err := io.ReadFull(r.r, meta); err != nil {
		return PageFrameStream{}, fmt.Errorf("%w: page metadata: %v", ErrTruncatedPageFrame, err)
	}

	indexValue := binary.BigEndian.Uint64(meta[:8])
	if indexValue > uint64(math.MaxInt64) {
		return PageFrameStream{}, fmt.Errorf("%w: page index %d overflows int64", ErrInvalidPageFrame, indexValue)
	}
	index := int64(indexValue)
	length := binary.BigEndian.Uint64(meta[8:16])
	if length > uint64(r.maxPageBytes) || length > uint64(math.MaxInt) {
		return PageFrameStream{}, fmt.Errorf("%w: page %d length %d exceeds limit %d", ErrOversizedPageFrame, index, length, r.maxPageBytes)
	}
	if _, ok := r.seen[index]; ok {
		return PageFrameStream{}, fmt.Errorf("%w: page %d", ErrDuplicatePageFrame, index)
	}
	if _, ok := r.expectedSet[index]; !ok {
		return PageFrameStream{}, fmt.Errorf("%w: page %d", ErrUnexpectedPageFrame, index)
	}
	if r.next >= len(r.expected) {
		return PageFrameStream{}, fmt.Errorf("%w: got extra page %d", ErrOutOfOrderPageFrame, index)
	}
	if index != r.expected[r.next] {
		return PageFrameStream{}, fmt.Errorf("%w: got page %d want page %d", ErrOutOfOrderPageFrame, index, r.expected[r.next])
	}

	r.seen[index] = struct{}{}
	r.next++
	return PageFrameStream{
		Index: index,
		Size:  int64(length),
		Body:  &pageFramePayloadReader{r: r.r, pageIndex: index, remaining: int64(length)},
	}, nil
}

type pageFramePayloadReader struct {
	r         io.Reader
	pageIndex int64
	remaining int64
	closed    bool
}

func (r *pageFramePayloadReader) Read(data []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(data)) > r.remaining {
		data = data[:int(r.remaining)]
	}
	n, err := r.r.Read(data)
	r.remaining -= int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) && r.remaining > 0 {
			return n, fmt.Errorf("%w: page %d bytes: %v", ErrTruncatedPageFrame, r.pageIndex, err)
		}
		return n, err
	}
	return n, nil
}

func (r *pageFramePayloadReader) Close() error {
	if r.closed {
		return nil
	}
	if r.remaining == 0 {
		r.closed = true
		return nil
	}
	_, err := io.Copy(io.Discard, r)
	r.closed = true
	return err
}
