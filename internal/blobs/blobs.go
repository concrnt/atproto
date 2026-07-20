// Package blobs ingests media referenced by URL from concrnt posts into
// content-addressed local storage, and serves them for
// com.atproto.sync.getBlob. Pre-ingestion (rather than proxying) is required
// because a blob's CID must match its exact bytes forever.
package blobs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/atproto/internal/store"
)

const MaxBlobSize = 5 << 20 // 5MB, bsky image limit is lower but this is the ingest cap

var allowedMimes = map[string]bool{
	"image/jpeg": true,
	"image/png":  true,
	"image/webp": true,
	"image/gif":  true,
	"image/avif": true,
}

type Service struct {
	db      *gorm.DB
	dataDir string
	http    *http.Client
}

func NewService(db *gorm.DB, dataDir string) *Service {
	return &Service{
		db:      db,
		dataDir: filepath.Join(dataDir, "blobs"),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *Service) path(did, cidStr string) string {
	return filepath.Join(s.dataDir, did, cidStr)
}

// Ingest downloads url, stores it under did, and returns a LexBlob usable in
// records. Unsupported mime types and oversized files return an error.
func (s *Service) Ingest(ctx context.Context, did, url string) (*lexutil.LexBlob, error) {
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

	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxBlobSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxBlobSize {
		return nil, fmt.Errorf("media %s exceeds %d bytes", url, MaxBlobSize)
	}

	mime := resp.Header.Get("Content-Type")
	if mime == "" {
		mime = http.DetectContentType(data)
	}
	if !allowedMimes[mime] {
		return nil, fmt.Errorf("unsupported media type %q for %s", mime, url)
	}

	sum := sha256.Sum256(data)
	mh, err := multihash.Encode(sum[:], multihash.SHA2_256)
	if err != nil {
		return nil, err
	}
	c := cid.NewCidV1(cid.Raw, mh)

	p := s.path(did, c.String())
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return nil, err
	}

	rec := store.Blob{
		DID:    did,
		CID:    c.String(),
		Mime:   mime,
		Size:   int64(len(data)),
		SrcURL: url,
	}
	if err := s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&rec).Error; err != nil {
		return nil, err
	}

	return &lexutil.LexBlob{
		Ref:      lexutil.LexLink(c),
		MimeType: mime,
		Size:     int64(len(data)),
	}, nil
}

// Open returns the stored blob's metadata and a reader over its bytes.
func (s *Service) Open(did, cidStr string) (*store.Blob, io.ReadCloser, error) {
	var rec store.Blob
	if err := s.db.Where("did = ? AND cid = ?", did, cidStr).First(&rec).Error; err != nil {
		return nil, nil, err
	}
	f, err := os.Open(s.path(did, cidStr))
	if err != nil {
		return nil, nil, err
	}
	return &rec, f, nil
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
