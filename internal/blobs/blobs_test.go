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
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/media"
	"github.com/concrnt/atproto/internal/media/mediatest"
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
	return NewService(db, t.TempDir(), nil), o, srv.URL
}

func TestIngestStoresFileAndRow(t *testing.T) {
	s, o, base := newTestService(t)
	o.set("/a.png", "image/png; charset=binary", pngHeader)

	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/a.png")
	if err != nil {
		t.Fatal(err)
	}
	if ing.Blob.MimeType != "image/png" {
		t.Fatalf("mime = %q, want image/png (parameters stripped)", ing.Blob.MimeType)
	}
	if ing.Blob.Size != int64(len(pngHeader)) {
		t.Fatalf("size = %d, want %d", ing.Blob.Size, len(pngHeader))
	}
	cidStr := ing.Blob.Ref.String()
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

	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/v.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if ing.Blob.MimeType != "video/mp4" || ing.Blob.Size != int64(len(video)) {
		t.Fatalf("blob = %+v", ing.Blob)
	}
	if !IsVideo(ing.Blob.MimeType) {
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

	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/noct")
	if err != nil {
		t.Fatal(err)
	}
	if ing.Blob.MimeType != "image/png" {
		t.Fatalf("mime = %q, want image/png", ing.Blob.MimeType)
	}
	_, r, err := s.Open(context.Background(), "did:plc:x", ing.Blob.Ref.String())
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
	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/a.png")
	if err != nil {
		t.Fatal(err)
	}
	cidStr := ing.Blob.Ref.String()
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
	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/a.png")
	if err != nil {
		t.Fatal(err)
	}
	cidStr := ing.Blob.Ref.String()
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
	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/v.mp4")
	if err != nil {
		t.Fatal(err)
	}
	cidStr := ing.Blob.Ref.String()
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

// --- transcoding (skipped without ffmpeg) ---

func newTranscodingService(t *testing.T) (*Service, *origin, string) {
	t.Helper()
	mediatest.Require(t)
	s, o, base := newTestService(t)
	tc, err := media.New("", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	s.tc = tc
	return s, o, base
}

func probeFile(t *testing.T, s *Service, did, cidStr string) *media.Info {
	t.Helper()
	info, err := s.tc.Probe(context.Background(), s.path(did, cidStr))
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestIngestTranscodesNonMp4Video(t *testing.T) {
	s, o, base := newTranscodingService(t)
	mov := mediatest.Fixture(t, "mpeg4", "aac", "mov")
	o.set("/clip.mov", "video/quicktime", mov)

	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if ing.Blob.MimeType != "video/mp4" {
		t.Fatalf("mime = %q, want video/mp4", ing.Blob.MimeType)
	}
	if ing.Width != 320 || ing.Height != 240 {
		t.Fatalf("dimensions = %dx%d", ing.Width, ing.Height)
	}
	cidStr := ing.Blob.Ref.String()
	st, err := os.Stat(s.path("did:plc:x", cidStr))
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != ing.Blob.Size {
		t.Fatalf("size = %d, file is %d", ing.Blob.Size, st.Size())
	}
	if bytes.Equal(mov, mustRead(t, s.path("did:plc:x", cidStr))) {
		t.Fatal("stored file is the untranscoded source")
	}
	info := probeFile(t, s, "did:plc:x", cidStr)
	if info.VideoCodec != "h264" || info.AudioCodec != "aac" {
		t.Fatalf("stored video = %+v", info)
	}
	entries, _ := os.ReadDir(filepath.Join(s.dataDir, "did:plc:x"))
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %d entries", len(entries))
	}
	var rec store.Blob
	if err := s.db.First(&rec, "did = ? AND cid = ?", "did:plc:x", cidStr).Error; err != nil {
		t.Fatal(err)
	}
	if rec.Mime != "video/mp4" || rec.SrcURL != base+"/clip.mov" {
		t.Fatalf("row = %+v", rec)
	}
}

func TestIngestReencodesNonH264Mp4(t *testing.T) {
	s, o, base := newTranscodingService(t)
	src := mediatest.Fixture(t, "mpeg4", "aac", "mp4")
	o.set("/old.mp4", "video/mp4", src)

	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/old.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(src, mustRead(t, s.path("did:plc:x", ing.Blob.Ref.String()))) {
		t.Fatal("mpeg4-in-mp4 was passed through")
	}
	if info := probeFile(t, s, "did:plc:x", ing.Blob.Ref.String()); info.VideoCodec != "h264" {
		t.Fatalf("stored video = %+v", info)
	}
}

func TestIngestPassesThroughValidMp4(t *testing.T) {
	s, o, base := newTranscodingService(t)
	src := mediatest.Fixture(t, "libx264", "aac", "mp4")
	o.set("/good.mp4", "video/mp4", src)

	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/good.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(src, mustRead(t, s.path("did:plc:x", ing.Blob.Ref.String()))) {
		t.Fatal("valid mp4 was re-encoded")
	}
	if ing.Blob.Size != int64(len(src)) || ing.Width != 320 || ing.Height != 240 {
		t.Fatalf("ingested = %+v", ing)
	}
}

func TestIngestVideoWithoutTranscoderIsUnchecked(t *testing.T) {
	// No ffmpeg: mp4 passes as before, even when it is not a bsky-playable
	// mp4, and other containers are rejected.
	s, o, base := newTestService(t)
	o.set("/v.mp4", "video/mp4", []byte("not really mp4"))
	o.set("/v.mov", "video/quicktime", []byte("mov"))
	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/v.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if ing.Width != 0 || ing.Height != 0 {
		t.Fatalf("dimensions without ffprobe = %dx%d", ing.Width, ing.Height)
	}
	if _, err := s.Ingest(context.Background(), "did:plc:x", base+"/v.mov"); !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("err = %v, want ErrUnsupportedMedia", err)
	}
}

func TestIngestRejectsUndecodableVideo(t *testing.T) {
	s, o, base := newTranscodingService(t)
	o.set("/bad.webm", "video/webm", []byte("definitely not webm"))
	if _, err := s.Ingest(context.Background(), "did:plc:x", base+"/bad.webm"); err == nil {
		t.Fatal("expected error")
	}
	entries, _ := os.ReadDir(filepath.Join(s.dataDir, "did:plc:x"))
	if len(entries) != 0 {
		t.Fatalf("temp files left behind: %d entries", len(entries))
	}
}

func TestOpenRestoresTranscodedBlob(t *testing.T) {
	s, o, base := newTranscodingService(t)
	o.set("/clip.mov", "video/quicktime", mediatest.Fixture(t, "mpeg4", "aac", "mov"))
	ing, err := s.Ingest(context.Background(), "did:plc:x", base+"/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	cidStr := ing.Blob.Ref.String()
	want := mustRead(t, s.path("did:plc:x", cidStr))
	if err := os.Remove(s.path("did:plc:x", cidStr)); err != nil {
		t.Fatal(err)
	}

	rec, r, err := s.Open(context.Background(), "did:plc:x", cidStr)
	if err != nil {
		t.Fatalf("restore via re-transcode failed: %v", err)
	}
	defer r.Close()
	got, _ := io.ReadAll(r)
	if rec.Mime != "video/mp4" || !bytes.Equal(got, want) {
		t.Fatal("restored blob differs from original transcode")
	}
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
