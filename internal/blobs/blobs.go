// Package blobs ingests media referenced by URL from concrnt posts into
// content-addressed local storage, and serves them for
// com.atproto.sync.getBlob. Pre-ingestion (rather than proxying) is required
// because a blob's CID must match its exact bytes forever.
//
// The local file is a cache, not the only copy: the source URL is kept in the
// database, and a blob whose file went missing is re-fetched from it on the
// next read. This assumes the content behind a media URL never changes
// (concrnt media URLs are immutable); a re-fetch that hashes to a different
// CID is rejected rather than served.
//
// Videos may be transcoded and images are re-encoded on ingest (see package
// media); the stored blob is then ffmpeg's output, not the source bytes. A
// re-fetch runs the same pipeline, so restoring such a blob works only while
// ffmpeg produces identical output (same build, same flags). After an ffmpeg
// upgrade a lost transcoded file is treated like changed content: not served.
package blobs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/atproto/internal/media"
	"github.com/concrnt/atproto/internal/store"
)

const (
	// DefaultImageMaxBytes caps image blobs. app.bsky.embed.images allows
	// 2,000,000 today; 1,000,000 is the historical limit, what the official
	// PDS enforces on upload and what the official app compresses to, so it
	// is the safe target for every client.
	DefaultImageMaxBytes = media.DefaultImageMaxBytes
	// DefaultImageMaxDimension caps the long side of re-encoded images.
	DefaultImageMaxDimension = media.DefaultImageMaxSide
	// DefaultImageSourceMaxBytes caps image downloads when ffmpeg re-encodes
	// them (camera HEIC/JPEG sources run several MB).
	DefaultImageSourceMaxBytes = 20 << 20 // 20MB
	// MaxVideoSize matches app.bsky.embed.video's documented blob limit.
	MaxVideoSize = 100 << 20 // 100MB
	// MaxBlobSize is the largest blob of any kind; the per-kind cap is
	// decided once the content type is known.
	MaxBlobSize = MaxVideoSize

	// fetchTimeout bounds a single download end to end. Videos on a slow
	// origin take a while; the response-header timeout below catches a dead
	// origin much sooner.
	fetchTimeout          = 10 * time.Minute
	responseHeaderTimeout = 30 * time.Second
)

// passthroughImageMimes are the image types accepted unchanged when no
// transcoder is available: what Bluesky clients render natively. With ffmpeg
// any image/* is accepted and re-encoded (see limitFor).
var passthroughImageMimes = map[string]bool{
	"image/jpeg": true,
	"image/png":  true,
	"image/webp": true,
	"image/gif":  true,
	"image/avif": true,
}

// limitFor returns the ingest cap for a media type, and whether the type is
// accepted at all. With a transcoder any video/* (turned into mp4) and any
// image/* (re-encoded under the image cap) is accepted; without one only
// video/mp4 and the passthrough image types are, and images must already be
// under the image cap.
func (s *Service) limitFor(mime string) (int64, bool) {
	switch {
	case mime == "video/mp4":
		return MaxVideoSize, true
	case s.tc != nil && IsVideo(mime):
		return MaxVideoSize, true
	case s.tc != nil && IsImage(mime):
		return s.imageSourceMaxBytes, true
	case passthroughImageMimes[mime]:
		return s.imageMaxBytes, true
	}
	return 0, false
}

// ErrUnsupportedMedia is returned (wrapped) for media types outside allowedMimes.
var ErrUnsupportedMedia = errors.New("unsupported media type")

// ErrTooLarge is returned (wrapped) when a download exceeds its kind's cap.
var ErrTooLarge = errors.New("media exceeds size limit")

// ErrContentChanged is returned (wrapped) when a re-fetch of a missing blob
// does not hash to the recorded CID.
var ErrContentChanged = errors.New("source content no longer matches blob cid")

// IsVideo reports whether mime is a video type.
func IsVideo(mime string) bool {
	return strings.HasPrefix(mime, "video/")
}

// IsImage reports whether mime is an image type.
func IsImage(mime string) bool {
	return strings.HasPrefix(mime, "image/")
}

type Service struct {
	db      *gorm.DB
	dataDir string
	http    *http.Client
	// tc is nil when ffmpeg is unavailable or transcoding is disabled.
	tc *media.Transcoder

	imageMaxBytes       int64
	imageMaxDimension   int
	imageSourceMaxBytes int64

	// restoring dedups concurrent re-fetches of the same missing blob.
	restoringMu sync.Mutex
	restoring   map[string]*sync.WaitGroup
}

// Options tunes image handling. Zero values take the Default* constants.
type Options struct {
	// ImageMaxBytes caps stored image blobs (re-encoded with ffmpeg, or the
	// passthrough limit without it).
	ImageMaxBytes int64
	// ImageMaxDimension caps the long side of re-encoded images in pixels.
	ImageMaxDimension int
	// ImageSourceMaxBytes caps image downloads when they will be re-encoded.
	ImageSourceMaxBytes int64
}

// NewService stores blobs under dataDir/blobs. tc may be nil (no transcoding:
// only video/mp4 and the passthrough image types are accepted, unchecked
// apart from the size caps).
func NewService(db *gorm.DB, dataDir string, tc *media.Transcoder, opts Options) *Service {
	if opts.ImageMaxBytes <= 0 {
		opts.ImageMaxBytes = DefaultImageMaxBytes
	}
	if opts.ImageMaxDimension <= 0 {
		opts.ImageMaxDimension = DefaultImageMaxDimension
	}
	if opts.ImageSourceMaxBytes <= 0 {
		opts.ImageSourceMaxBytes = DefaultImageSourceMaxBytes
	}
	return &Service{
		db:      db,
		dataDir: filepath.Join(dataDir, "blobs"),
		tc:      tc,
		http: &http.Client{
			Timeout:   fetchTimeout,
			Transport: &http.Transport{ResponseHeaderTimeout: responseHeaderTimeout},
		},
		imageMaxBytes:       opts.ImageMaxBytes,
		imageMaxDimension:   opts.ImageMaxDimension,
		imageSourceMaxBytes: opts.ImageSourceMaxBytes,
		restoring:           map[string]*sync.WaitGroup{},
	}
}

func (s *Service) path(did, cidStr string) string {
	return filepath.Join(s.dataDir, did, cidStr)
}

// Ingested is the result of Ingest: the blob reference for records plus, for
// media that went through ffprobe (videos and re-encoded images), the display
// dimensions (0 when unknown).
type Ingested struct {
	Blob   *lexutil.LexBlob
	Width  int
	Height int
}

// Ingest downloads url, stores it under did (transcoding videos to mp4 when
// needed and possible), and returns a LexBlob usable in records. Unsupported
// mime types and oversized files return an error.
func (s *Service) Ingest(ctx context.Context, did, url string) (*Ingested, error) {
	f, err := s.download(ctx, did, url, "")
	if err != nil {
		return nil, err
	}

	rec := store.Blob{
		DID:    did,
		CID:    f.cid.String(),
		Mime:   f.mime,
		Size:   f.size,
		SrcURL: url,
	}
	if err := s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&rec).Error; err != nil {
		return nil, err
	}

	return &Ingested{
		Blob: &lexutil.LexBlob{
			Ref:      lexutil.LexLink(f.cid),
			MimeType: f.mime,
			Size:     f.size,
		},
		Width:  f.width,
		Height: f.height,
	}, nil
}

// Open returns the stored blob's metadata and a reader over its bytes. A blob
// whose local file is gone is restored from its source URL first.
func (s *Service) Open(ctx context.Context, did, cidStr string) (*store.Blob, io.ReadCloser, error) {
	var rec store.Blob
	if err := s.db.Where("did = ? AND cid = ?", did, cidStr).First(&rec).Error; err != nil {
		return nil, nil, err
	}
	f, err := os.Open(s.path(did, cidStr))
	if errors.Is(err, os.ErrNotExist) {
		if err := s.restore(ctx, &rec); err != nil {
			return nil, nil, err
		}
		f, err = os.Open(s.path(did, cidStr))
	}
	if err != nil {
		return nil, nil, err
	}
	return &rec, f, nil
}

// restore re-downloads rec from its source URL and verifies it still hashes
// to rec.CID. Concurrent callers for the same blob wait for the first one.
func (s *Service) restore(ctx context.Context, rec *store.Blob) error {
	if rec.SrcURL == "" {
		return fmt.Errorf("blob %s/%s has no source url to restore from", rec.DID, rec.CID)
	}

	key := rec.DID + "/" + rec.CID
	s.restoringMu.Lock()
	if wg, ok := s.restoring[key]; ok {
		s.restoringMu.Unlock()
		wg.Wait()
		if _, err := os.Stat(s.path(rec.DID, rec.CID)); err == nil {
			return nil
		}
		return fmt.Errorf("blob %s restore failed", key)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	s.restoring[key] = wg
	s.restoringMu.Unlock()
	defer func() {
		s.restoringMu.Lock()
		delete(s.restoring, key)
		s.restoringMu.Unlock()
		wg.Done()
	}()

	_, err := s.download(ctx, rec.DID, rec.SrcURL, rec.CID)
	return err
}

type fetched struct {
	cid    cid.Cid
	mime   string
	size   int64
	width  int
	height int
}

// download streams url into a temp file under did, runs videos through the
// transcoder when they are not already a valid bsky mp4, hashes the result
// and moves it into place under its CID. With wantCID set, the result must
// hash to exactly that CID or the file is discarded (ErrContentChanged).
func (s *Service) download(ctx context.Context, did, url, wantCID string) (*fetched, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch media %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch media %s: status %d", url, resp.StatusCode)
	}

	body, mimeType, err := sniffMime(resp)
	if err != nil {
		return nil, err
	}
	limit, ok := s.limitFor(mimeType)
	if !ok {
		return nil, fmt.Errorf("%w %q for %s", ErrUnsupportedMedia, mimeType, url)
	}
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("%w: media %s is %d bytes (limit %d)", ErrTooLarge, url, resp.ContentLength, limit)
	}

	dir := filepath.Join(s.dataDir, did)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, ".ingest-*")
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	// Any early return leaves tmpName behind; the deferred remove is a no-op
	// once the file has been renamed into place.
	defer os.Remove(tmpName)

	n, err := io.Copy(tmp, io.LimitReader(body, limit+1))
	closeErr := tmp.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to download media %s: %w", url, err)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if n > limit {
		return nil, fmt.Errorf("%w: media %s exceeds %d bytes", ErrTooLarge, url, limit)
	}

	f := &fetched{mime: mimeType, size: n}
	if s.tc != nil && (IsVideo(mimeType) || IsImage(mimeType)) {
		var outName string
		if IsVideo(mimeType) {
			outName, err = s.prepareVideo(ctx, dir, tmpName, url, f)
		} else {
			outName, err = s.prepareImage(ctx, dir, tmpName, url, f)
		}
		if err != nil {
			return nil, err
		}
		if outName != tmpName {
			os.Remove(tmpName)
			tmpName = outName
			defer os.Remove(tmpName)
		}
	}

	c, err := hashFile(tmpName)
	if err != nil {
		return nil, err
	}
	if wantCID != "" && c.String() != wantCID {
		return nil, fmt.Errorf("%w: %s hashed to %s, want %s", ErrContentChanged, url, c, wantCID)
	}
	f.cid = c

	if err := os.Rename(tmpName, s.path(did, c.String())); err != nil {
		return nil, err
	}
	return f, nil
}

// prepareVideo probes the downloaded video at in and, when it is not already
// an mp4 bsky can play, transcodes it into a sibling temp file. It returns
// the path of the file to keep (in itself when no transcode was needed) and
// fills f's mime, size and dimensions.
func (s *Service) prepareVideo(ctx context.Context, dir, in, url string, f *fetched) (string, error) {
	info, err := s.tc.Probe(ctx, in)
	if err != nil {
		return "", fmt.Errorf("failed to probe video %s: %w", url, err)
	}
	if !media.NeedsTranscode(f.mime, info) {
		f.width, f.height = info.Width, info.Height
		return in, nil
	}

	out, err := os.CreateTemp(dir, ".transcode-*.mp4")
	if err != nil {
		return "", err
	}
	outName := out.Name()
	out.Close()
	started := time.Now()
	if err := s.tc.Transcode(ctx, in, outName); err != nil {
		os.Remove(outName)
		return "", fmt.Errorf("failed to transcode video %s: %w", url, err)
	}
	st, err := os.Stat(outName)
	if err != nil {
		os.Remove(outName)
		return "", err
	}
	if st.Size() > MaxVideoSize {
		os.Remove(outName)
		return "", fmt.Errorf("%w: transcoded video %s is %d bytes (limit %d)", ErrTooLarge, url, st.Size(), MaxVideoSize)
	}
	outInfo, err := s.tc.Probe(ctx, outName)
	if err != nil {
		os.Remove(outName)
		return "", fmt.Errorf("failed to probe transcoded video %s: %w", url, err)
	}
	slog.Info("transcoded video",
		"url", url, "from", f.mime, "codec", info.VideoCodec+"/"+info.AudioCodec,
		"inBytes", f.size, "outBytes", st.Size(), "took", time.Since(started).Round(time.Millisecond))
	f.mime = "video/mp4"
	f.size = st.Size()
	f.width, f.height = outInfo.Width, outInfo.Height
	return outName, nil
}

// prepareImage re-encodes the downloaded image at in (JPEG, or PNG for alpha
// sources; small animated GIFs pass through) under the image caps, fills f's
// mime, size and dimensions, and returns the path of the file to keep.
func (s *Service) prepareImage(ctx context.Context, dir, in, url string, f *fetched) (string, error) {
	started := time.Now()
	res, err := s.tc.PrepareImage(ctx, in, f.mime, dir, media.ImageOptions{
		MaxBytes: s.imageMaxBytes,
		MaxSide:  s.imageMaxDimension,
	})
	if errors.Is(err, media.ErrImageTooLarge) {
		return "", fmt.Errorf("%w: image %s: %v", ErrTooLarge, url, err)
	}
	if err != nil {
		return "", fmt.Errorf("failed to re-encode image %s: %w", url, err)
	}
	if res.Path != in {
		slog.Info("re-encoded image",
			"url", url, "from", f.mime, "to", res.Mime,
			"inBytes", f.size, "outBytes", res.Size, "size", fmt.Sprintf("%dx%d", res.Width, res.Height),
			"attempts", res.Attempts, "took", time.Since(started).Round(time.Millisecond))
	}
	f.mime = res.Mime
	f.size = res.Size
	f.width, f.height = res.Width, res.Height
	return res.Path, nil
}

// hashFile returns the raw CIDv1 (sha2-256) of the file at path.
func hashFile(path string) (cid.Cid, error) {
	fh, err := os.Open(path)
	if err != nil {
		return cid.Undef, err
	}
	defer fh.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, fh); err != nil {
		return cid.Undef, err
	}
	return cidOf(hasher)
}

// sniffMime returns the response's media type (parameters such as charset
// stripped), falling back to content sniffing when the header is absent.
// The returned reader replays any bytes consumed by sniffing.
func sniffMime(resp *http.Response) (io.Reader, string, error) {
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err == nil && mt != "application/octet-stream" {
			return resp.Body, mt, nil
		}
	}
	head := make([]byte, 512)
	n, err := io.ReadFull(resp.Body, head)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, "", err
	}
	head = head[:n]
	mt, _, _ := mime.ParseMediaType(http.DetectContentType(head))
	return io.MultiReader(strings.NewReader(string(head)), resp.Body), mt, nil
}

func cidOf(h hash.Hash) (cid.Cid, error) {
	mh, err := multihash.Encode(h.Sum(nil), multihash.SHA2_256)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, mh), nil
}

// List returns blob CIDs for a did, for com.atproto.sync.listBlobs.
func (s *Service) List(did string, limit int) ([]string, error) {
	var recs []store.Blob
	if err := s.db.Where("did = ?", did).Order("created_at").Limit(limit).Find(&recs).Error; err != nil {
		return nil, err
	}
	cids := make([]string, len(recs))
	for i, r := range recs {
		cids[i] = r.CID
	}
	return cids, nil
}
