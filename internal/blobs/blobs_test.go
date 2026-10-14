package blobs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/store"
)

// pngHeader is enough of a PNG for http.DetectContentType to recognise it.
var pngHeader = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")

type origin struct {
	mu    sync.Mutex
	files map[string]struct {
		mime string
		body []byte
	}
	hits atomic.Int64
}

func newOrigin() *origin {
	return &origin{files: map[string]struct {
		mime string
		body []byte
	}{}}
}

func (o *origin) set(path, mime string, body []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.files[path] = struct {
		mime string
		body []byte
	}{mime, body}
}

func (o *origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.hits.Add(1)
	o.mu.Lock()
	f, ok := o.files[r.URL.Path]
	o.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	if f.mime != "" {
		w.Header().Set("Content-Type", f.mime)
	}
	w.Write(f.body)
}

func newTestService(t *testing.T) (*Service, *origin, string) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "blobs.db")), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := store.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	o := newOrigin()
	srv := httptest.NewServer(o)
	t.Cleanup(srv.Close)
	return NewService(db, t.TempDir()), o, srv.URL
}

func TestIngestStoresFileAndRow(t *testing.T) {
	s, o, base := newTestService(t)
	o.set("/a.png", "image/png; charset=binary", pngHeader)

	blob, err := s.Ingest(context.Background(), "did:plc:x", base+"/a.png")
	if err != nil {
		t.Fatal(err)
	}
	if blob.MimeType != "image/png" {
		t.Fatalf("mime = %q, want image/png (parameters stripped)", blob.MimeType)
	}
	if blob.Size != int64(len(pngHeader)) {
		t.Fatalf("size = %d, want %d", blob.Size, len(pngHeader))
	}
	cidStr := blob.Ref.String()
	data, err := os.ReadFile(s.path("did:plc:x", cidStr))
	if err != nil {
		t.Fatalf("blob file not written: %v", err)
	}
	if !bytes.Equal(data, pngHeader) {
		t.Fatal("blob file content mismatch")
	}
	entries, _ := os.ReadDir(filepath.Join(s.dataDir, "did:plc:x"))
	if len(entries) != 1 {
		t.Fatalf("temp file left behind: %d entries", len(entries))
	}
	var rec store.Blob
	if err := s.db.First(&rec, "did = ? AND cid = ?", "did:plc:x", cidStr).Error; err != nil {
		t.Fatal(err)
	}
	if rec.SrcURL != base+"/a.png" || rec.Mime != "image/png" {
		t.Fatalf("row = %+v", rec)
	}
}

func TestIngestVideo(t *testing.T) {
	s, o, base := newTestService(t)
	video := bytes.Repeat([]byte{0xab}, MaxImageSize+1) // over the image cap, fine for video
	o.set("/v.mp4", "video/mp4", video)

	blob, err := s.Ingest(context.Background(), "did:plc:x", base+"/v.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if blob.MimeType != "video/mp4" || blob.Size != int64(len(video)) {
		t.Fatalf("blob = %+v", blob)
	}
	if !IsVideo(blob.MimeType) {
		t.Fatal("IsVideo false for video/mp4")
	}
}

func TestIngestRejectsUnsupportedAndOversized(t *testing.T) {
	s, o, base := newTestService(t)
	o.set("/v.webm", "video/webm", []byte("xx"))
	o.set("/big.png", "image/png", bytes.Repeat([]byte{1}, MaxImageSize+1))
	o.set("/missing-ct.bin", "", []byte("plain text, no header"))

	if _, err := s.Ingest(context.Background(), "did:plc:x", base+"/v.webm"); !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("webm: err = %v, want ErrUnsupportedMedia", err)
	}
	if _, err := s.Ingest(context.Background(), "did:plc:x", base+"/big.png"); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized: err = %v, want ErrTooLarge", err)
	}
	if _, err := s.Ingest(context.Background(), "did:plc:x", base+"/missing-ct.bin"); !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("sniffed text: err = %v, want ErrUnsupportedMedia", err)
	}
	entries, _ := os.ReadDir(filepath.Join(s.dataDir, "did:plc:x"))
	if len(entries) != 0 {
		t.Fatalf("rejected downloads left files behind: %d", len(entries))
	}
}

func TestIngestSniffsMimeWithoutHeader(t *testing.T) {
	s, o, base := newTestService(t)
	o.set("/noct", "", pngHeader)

	blob, err := s.Ingest(context.Background(), "did:plc:x", base+"/noct")
	if err != nil {
		t.Fatal(err)
	}
	if blob.MimeType != "image/png" {
		t.Fatalf("mime = %q, want image/png", blob.MimeType)
	}
	_, r, err := s.Open(context.Background(), "did:plc:x", blob.Ref.String())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if !bytes.Equal(data, pngHeader) {
		t.Fatal("sniffed bytes were not replayed into the stored file")
	}
}

func TestOpenRestoresMissingFileFromSource(t *testing.T) {
	s, o, base := newTestService(t)
	o.set("/a.png", "image/png", pngHeader)
	blob, err := s.Ingest(context.Background(), "did:plc:x", base+"/a.png")
	if err != nil {
		t.Fatal(err)
	}
	cidStr := blob.Ref.String()
	if err := os.Remove(s.path("did:plc:x", cidStr)); err != nil {
		t.Fatal(err)
	}
	o.hits.Store(0)

	rec, r, err := s.Open(context.Background(), "did:plc:x", cidStr)
	if err != nil {
		t.Fatalf("Open after file loss: %v", err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if !bytes.Equal(data, pngHeader) || rec.Mime != "image/png" {
		t.Fatal("restored content mismatch")
	}
	if o.hits.Load() != 1 {
		t.Fatalf("origin hits = %d, want 1", o.hits.Load())
	}
	// Restored file is cached again: a second Open does not hit the origin.
	_, r2, err := s.Open(context.Background(), "did:plc:x", cidStr)
	if err != nil {
		t.Fatal(err)
	}
	r2.Close()
	if o.hits.Load() != 1 {
		t.Fatalf("origin hits after cached open = %d, want 1", o.hits.Load())
	}
}

func TestOpenRejectsChangedSource(t *testing.T) {
	s, o, base := newTestService(t)
	o.set("/a.png", "image/png", pngHeader)
	blob, err := s.Ingest(context.Background(), "did:plc:x", base+"/a.png")
	if err != nil {
		t.Fatal(err)
	}
	cidStr := blob.Ref.String()
	os.Remove(s.path("did:plc:x", cidStr))
	o.set("/a.png", "image/png", append(append([]byte{}, pngHeader...), 'x'))

	_, _, err = s.Open(context.Background(), "did:plc:x", cidStr)
	if !errors.Is(err, ErrContentChanged) {
		t.Fatalf("err = %v, want ErrContentChanged", err)
	}
	if _, statErr := os.Stat(s.path("did:plc:x", cidStr)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatal("mismatching content must not be stored under the old cid")
	}
	entries, _ := os.ReadDir(filepath.Join(s.dataDir, "did:plc:x"))
	if len(entries) != 0 {
		t.Fatalf("temp file left behind: %d entries", len(entries))
	}
}

func TestOpenUnknownBlob(t *testing.T) {
	s, _, _ := newTestService(t)
	_, _, err := s.Open(context.Background(), "did:plc:x", "bafkreinonexistent")
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("err = %v, want gorm.ErrRecordNotFound", err)
	}
}

func TestOpenRestoreIsDedupedAcrossConcurrentReaders(t *testing.T) {
	s, o, base := newTestService(t)
	video := bytes.Repeat([]byte{0xcd}, 1<<20)
	o.set("/v.mp4", "video/mp4", video)
	blob, err := s.Ingest(context.Background(), "did:plc:x", base+"/v.mp4")
	if err != nil {
		t.Fatal(err)
	}
	cidStr := blob.Ref.String()
	os.Remove(s.path("did:plc:x", cidStr))
	o.hits.Store(0)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, r, err := s.Open(context.Background(), "did:plc:x", cidStr)
			if err != nil {
				errs <- err
				return
			}
			defer r.Close()
			data, _ := io.ReadAll(r)
			if !bytes.Equal(data, video) {
				errs <- errors.New("content mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if hits := o.hits.Load(); hits > 2 {
		t.Fatalf("origin hits = %d, want restore to be deduplicated", hits)
	}
}
