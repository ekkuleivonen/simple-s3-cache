package peer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestPageFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	writer, err := NewPageFrameWriter(&buf)
	if err != nil {
		t.Fatalf("NewPageFrameWriter() error = %v", err)
	}
	if err := writer.WritePage(2, []byte("two")); err != nil {
		t.Fatalf("WritePage(2) error = %v", err)
	}
	if err := writer.WritePage(5, []byte("five")); err != nil {
		t.Fatalf("WritePage(5) error = %v", err)
	}
	if err := writer.WriteEnd(); err != nil {
		t.Fatalf("WriteEnd() error = %v", err)
	}

	reader, err := NewPageFrameReader(&buf, []int64{2, 5}, 1024)
	if err != nil {
		t.Fatalf("NewPageFrameReader() error = %v", err)
	}

	first, err := reader.NextPage()
	if err != nil {
		t.Fatalf("NextPage(first) error = %v", err)
	}
	if first.Index != 2 || string(first.Bytes) != "two" {
		t.Fatalf("first frame = %+v, want page 2", first)
	}
	second, err := reader.NextPage()
	if err != nil {
		t.Fatalf("NextPage(second) error = %v", err)
	}
	if second.Index != 5 || string(second.Bytes) != "five" {
		t.Fatalf("second frame = %+v, want page 5", second)
	}
	if _, err := reader.NextPage(); !errors.Is(err, io.EOF) {
		t.Fatalf("NextPage(end) error = %v, want EOF", err)
	}
	if _, err := reader.NextPage(); !errors.Is(err, io.EOF) {
		t.Fatalf("NextPage(after end) error = %v, want EOF", err)
	}
}

func TestPageFrameWriterStreamsPageFromReader(t *testing.T) {
	var buf bytes.Buffer
	writer, err := NewPageFrameWriter(&buf)
	if err != nil {
		t.Fatalf("NewPageFrameWriter() error = %v", err)
	}
	if err := writer.WritePageFrom(7, 6, strings.NewReader("stream")); err != nil {
		t.Fatalf("WritePageFrom() error = %v", err)
	}
	if err := writer.WriteEnd(); err != nil {
		t.Fatalf("WriteEnd() error = %v", err)
	}

	reader, err := NewPageFrameReader(&buf, []int64{7}, 1024)
	if err != nil {
		t.Fatalf("NewPageFrameReader() error = %v", err)
	}
	frame, err := reader.NextPage()
	if err != nil {
		t.Fatalf("NextPage() error = %v", err)
	}
	if frame.Index != 7 || string(frame.Bytes) != "stream" {
		t.Fatalf("frame = %+v, want streamed page", frame)
	}
}

func TestPageFrameReaderStreamsPageBody(t *testing.T) {
	var buf bytes.Buffer
	writePageFrameStreamHeader(&buf)
	writePageFrame(&buf, 3, []byte("abcdef"))
	writeEndFrame(&buf)

	reader, err := NewPageFrameReader(&buf, []int64{3}, 1024)
	if err != nil {
		t.Fatalf("NewPageFrameReader() error = %v", err)
	}
	stream, err := reader.NextPageStream()
	if err != nil {
		t.Fatalf("NextPageStream() error = %v", err)
	}
	if stream.Index != 3 || stream.Size != 6 {
		t.Fatalf("stream metadata = %+v, want page 3 size 6", stream)
	}
	first := make([]byte, 2)
	if _, err := io.ReadFull(stream.Body, first); err != nil {
		t.Fatalf("read partial stream body: %v", err)
	}
	if string(first) != "ab" {
		t.Fatalf("first bytes = %q, want ab", first)
	}
	if err := stream.Body.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := reader.NextPageStream(); !errors.Is(err, io.EOF) {
		t.Fatalf("NextPageStream(end) error = %v, want EOF", err)
	}
}

func TestPageFrameRejectsBadVersionHeader(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "bad magic", data: append([]byte("BAD!"), 0, 1)},
		{name: "bad version", data: []byte{'S', '3', 'P', 'F', 0, 2}},
		{name: "truncated", data: []byte{'S', '3', 'P'}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPageFrameReader(bytes.NewReader(tt.data), nil, 1024)
			if tt.name == "truncated" {
				if !errors.Is(err, ErrTruncatedPageFrame) {
					t.Fatalf("NewPageFrameReader() error = %v, want truncated", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidPageFrameVersion) {
				t.Fatalf("NewPageFrameReader() error = %v, want bad version", err)
			}
		})
	}
}

func TestPageFrameReaderRejectsInvalidExpectedPages(t *testing.T) {
	tests := []struct {
		name     string
		expected []int64
	}{
		{name: "negative", expected: []int64{-1}},
		{name: "duplicate", expected: []int64{1, 1}},
		{name: "descending", expected: []int64{2, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPageFrameReader(bytes.NewReader(pageFrameStreamHeader()), tt.expected, 1024)
			if !errors.Is(err, ErrInvalidPageFrame) {
				t.Fatalf("NewPageFrameReader() error = %v, want invalid frame", err)
			}
		})
	}
}

func TestPageFrameRejectsMissingEndMarker(t *testing.T) {
	var buf bytes.Buffer
	writePageFrameStreamHeader(&buf)
	writePageFrame(&buf, 1, []byte("one"))

	reader, err := NewPageFrameReader(&buf, []int64{1}, 1024)
	if err != nil {
		t.Fatalf("NewPageFrameReader() error = %v", err)
	}
	if _, err := reader.NextPage(); err != nil {
		t.Fatalf("NextPage(page) error = %v", err)
	}
	if _, err := reader.NextPage(); !errors.Is(err, ErrTruncatedPageFrame) {
		t.Fatalf("NextPage(missing end) error = %v, want truncated", err)
	}
}

func TestPageFrameRejectsTruncatedFrames(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "metadata", data: append(pageFrameStreamHeader(), pageFrameTypePage, 0, 0)},
		{name: "bytes", data: truncatedPageFrame(3, 4, []byte("ab"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewPageFrameReader(bytes.NewReader(tt.data), []int64{3}, 1024)
			if err != nil {
				t.Fatalf("NewPageFrameReader() error = %v", err)
			}
			if _, err := reader.NextPage(); !errors.Is(err, ErrTruncatedPageFrame) {
				t.Fatalf("NextPage() error = %v, want truncated", err)
			}
		})
	}
}

func TestPageFrameRejectsUnknownFrameType(t *testing.T) {
	var buf bytes.Buffer
	writePageFrameStreamHeader(&buf)
	buf.WriteByte(99)

	reader, err := NewPageFrameReader(&buf, nil, 1024)
	if err != nil {
		t.Fatalf("NewPageFrameReader() error = %v", err)
	}
	if _, err := reader.NextPage(); !errors.Is(err, ErrInvalidPageFrame) {
		t.Fatalf("NextPage() error = %v, want invalid frame", err)
	}
}

func TestPageFrameRejectsOversizedFrame(t *testing.T) {
	data := truncatedPageFrame(1, 5, nil)
	reader, err := NewPageFrameReader(bytes.NewReader(data), []int64{1}, 4)
	if err != nil {
		t.Fatalf("NewPageFrameReader() error = %v", err)
	}
	if _, err := reader.NextPage(); !errors.Is(err, ErrOversizedPageFrame) {
		t.Fatalf("NextPage() error = %v, want oversized", err)
	}
}

func TestPageFrameRejectsDuplicateUnexpectedAndOutOfOrderFrames(t *testing.T) {
	tests := []struct {
		name          string
		pages         []int64
		expected      []int64
		wantFirstErr  error
		wantSecondErr error
	}{
		{name: "duplicate", pages: []int64{1, 1}, expected: []int64{1, 2}, wantSecondErr: ErrDuplicatePageFrame},
		{name: "unexpected", pages: []int64{9}, expected: []int64{1}, wantFirstErr: ErrUnexpectedPageFrame},
		{name: "out of order", pages: []int64{2}, expected: []int64{1, 2}, wantFirstErr: ErrOutOfOrderPageFrame},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writePageFrameStreamHeader(&buf)
			for _, page := range tt.pages {
				writePageFrame(&buf, page, []byte("data"))
			}
			writeEndFrame(&buf)

			reader, err := NewPageFrameReader(&buf, tt.expected, 1024)
			if err != nil {
				t.Fatalf("NewPageFrameReader() error = %v", err)
			}
			_, firstErr := reader.NextPage()
			if tt.wantFirstErr != nil {
				if !errors.Is(firstErr, tt.wantFirstErr) {
					t.Fatalf("NextPage(first) error = %v, want %v", firstErr, tt.wantFirstErr)
				}
				return
			}
			if firstErr != nil {
				t.Fatalf("NextPage(first) error = %v", firstErr)
			}
			_, secondErr := reader.NextPage()
			if !errors.Is(secondErr, tt.wantSecondErr) {
				t.Fatalf("NextPage(second) error = %v, want %v", secondErr, tt.wantSecondErr)
			}
		})
	}
}

func TestPageFrameRejectsUnexpectedEndFrame(t *testing.T) {
	var buf bytes.Buffer
	writePageFrameStreamHeader(&buf)
	writePageFrame(&buf, 1, []byte("one"))
	writeEndFrame(&buf)

	reader, err := NewPageFrameReader(&buf, []int64{1, 2}, 1024)
	if err != nil {
		t.Fatalf("NewPageFrameReader() error = %v", err)
	}
	if _, err := reader.NextPage(); err != nil {
		t.Fatalf("NextPage(page) error = %v", err)
	}
	if _, err := reader.NextPage(); !errors.Is(err, ErrUnexpectedEndFrame) {
		t.Fatalf("NextPage(end) error = %v, want unexpected end", err)
	}
}

func TestPageFrameWriterRejectsShortWrites(t *testing.T) {
	if _, err := NewPageFrameWriter(shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("NewPageFrameWriter(short writer) error = %v, want short write", err)
	}

	var buf bytes.Buffer
	writer, err := NewPageFrameWriter(&buf)
	if err != nil {
		t.Fatalf("NewPageFrameWriter() error = %v", err)
	}
	writer.w = shortWriter{}
	if err := writer.WritePage(1, []byte("one")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WritePage(short writer) error = %v, want short write", err)
	}
}

func TestPageFrameWriterRejectsOutOfOrderAndClosedWrites(t *testing.T) {
	var buf bytes.Buffer
	writer, err := NewPageFrameWriter(&buf)
	if err != nil {
		t.Fatalf("NewPageFrameWriter() error = %v", err)
	}
	if err := writer.WritePage(3, []byte("three")); err != nil {
		t.Fatalf("WritePage(3) error = %v", err)
	}
	if err := writer.WritePage(2, []byte("two")); !errors.Is(err, ErrOutOfOrderPageFrame) {
		t.Fatalf("WritePage(out of order) error = %v, want out of order", err)
	}
	if err := writer.WriteEnd(); err != nil {
		t.Fatalf("WriteEnd() error = %v", err)
	}
	if err := writer.WritePage(4, []byte("four")); !errors.Is(err, ErrPageFrameWriterClosed) {
		t.Fatalf("WritePage(after end) error = %v, want closed", err)
	}
	if err := writer.WriteEnd(); !errors.Is(err, ErrPageFrameWriterClosed) {
		t.Fatalf("WriteEnd(after end) error = %v, want closed", err)
	}
}

func pageFrameStreamHeader() []byte {
	var buf bytes.Buffer
	writePageFrameStreamHeader(&buf)
	return buf.Bytes()
}

func writePageFrameStreamHeader(buf *bytes.Buffer) {
	buf.Write(pageFrameMagic[:])
	_ = binary.Write(buf, binary.BigEndian, PageFrameVersion)
}

func writePageFrame(buf *bytes.Buffer, index int64, data []byte) {
	truncated := truncatedPageFrame(index, uint64(len(data)), data)
	buf.Write(truncated[pageFrameHeaderSize:])
}

func truncatedPageFrame(index int64, length uint64, data []byte) []byte {
	var buf bytes.Buffer
	writePageFrameStreamHeader(&buf)
	buf.WriteByte(pageFrameTypePage)
	_ = binary.Write(&buf, binary.BigEndian, uint64(index))
	_ = binary.Write(&buf, binary.BigEndian, length)
	buf.Write(data)
	return buf.Bytes()
}

func writeEndFrame(buf *bytes.Buffer) {
	buf.WriteByte(pageFrameTypeEnd)
}

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	return len(data) - 1, nil
}
