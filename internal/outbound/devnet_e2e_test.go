package outbound_test

// End-to-end test of the outbound path against a running concrnt devnet:
// a real post commit → core Redis event → daemon → bsky record in the repo.
// Skipped unless DEVNET_E2E=1 (see internal/core/devnet_e2e_test.go for the
// environment variables). Requires the devnet's Redis on localhost:6379.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	concrnt "github.com/concrnt/concrnt"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/blobs"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/media"
	"github.com/concrnt/atproto/internal/media/mediatest"
	"github.com/concrnt/atproto/internal/outbound"
	"github.com/concrnt/atproto/internal/plcm"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

func TestDevnetOutboundPath(t *testing.T) {
	if os.Getenv("DEVNET_E2E") != "1" {
		t.Skip("set DEVNET_E2E=1 to run against a devnet")
	}
	ccid := os.Getenv("DEVNET_CCID")
	privkey := os.Getenv("DEVNET_PRIVKEY")
	devnetURL := os.Getenv("DEVNET_URL")
	redisURL := os.Getenv("DEVNET_REDIS")
	if devnetURL == "" {
		devnetURL = "http://localhost:8000"
	}
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	if ccid == "" || privkey == "" {
		t.Fatal("DEVNET_CCID and DEVNET_PRIVKEY are required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Local bridge state (sqlite + temp carstore), real devnet events.
	// File-backed: sqlite ":memory:" gives each pooled connection its own
	// database, which corrupts carstore metadata under concurrency.
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "e2e.db")), &gorm.Config{
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
	ks, err := keys.NewService(db, strings.Repeat("cd", 32))
	if err != nil {
		t.Fatal(err)
	}
	rot, _ := atcrypto.GeneratePrivateKeyK256()
	repos, err := repoman.New(db, t.TempDir(), ks, plcm.NewService("http://unused.invalid", rot, "test.example"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// Bridge the devnet account manually (no PLC round-trip in tests).
	did := "did:plc:outbounde2etest"
	signing, _ := keys.Generate()
	if err := ks.StoreSigningKey(did, signing); err != nil {
		t.Fatal(err)
	}
	ent := store.Entity{CCID: ccid, DID: did, Handle: "e2e.test.example", Status: "active", Enabled: true}
	if err := db.Create(&ent).Error; err != nil {
		t.Fatal(err)
	}
	if err := repos.InitActor(ctx, did, "e2e.test.example"); err != nil {
		t.Fatal(err)
	}

	coreSvc := core.NewService(ccid, devnetURL, privkey)
	events, err := core.Events(ctx, redisURL)
	if err != nil {
		t.Fatalf("redis subscribe failed: %v", err)
	}

	blobDir := t.TempDir()
	// With ffmpeg installed the real transcoding path is exercised; without
	// it the video steps below fall back to a passthrough-only service.
	var tc *media.Transcoder
	if transcoder, err := media.New("", "", time.Minute); err == nil {
		tc = transcoder
	} else {
		t.Logf("ffmpeg unavailable, transcoding steps skipped: %v", err)
	}
	blobSvc := blobs.NewService(db, blobDir, tc)
	daemon := outbound.NewDaemon(db, coreSvc, repos, blobSvc, "", time.Second)
	go daemon.Run(ctx, events)
	time.Sleep(500 * time.Millisecond) // let the daemon load entities

	// Post as the bridged user on their home timeline, like a real client.
	body := fmt.Sprintf("outbound e2e test post %d", time.Now().Unix())
	postKey := fmt.Sprintf("cckv://%s/e2etest/%d", ccid, time.Now().UnixNano())
	home := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/home-timeline", ccid)
	doc := concrnt.Document[world.MessageValue]{
		Kind:        "record",
		Key:         postKey,
		Schema:      world.SchemaMarkdown,
		Value:       world.MessageValue{Body: body},
		Author:      ccid,
		CreatedAt:   time.Now().UTC(),
		Distributes: &[]string{home},
	}
	if _, err := core.Commit(ctx, coreSvc, doc); err != nil {
		t.Fatalf("post commit failed: %v", err)
	}

	// The daemon should pick up the Redis event and write a bsky record.
	var m store.URIMap
	deadline := time.Now().Add(15 * time.Second)
	for {
		if err := db.Where("cc_uri = ? AND ref_type = ?", postKey, "outbound-post").First(&m).Error; err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the daemon to export the post")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !strings.HasPrefix(m.AtURI, "at://"+did+"/app.bsky.feed.post/") {
		t.Fatalf("unexpected at-uri %s", m.AtURI)
	}

	// Verify the record content inside the repo.
	rec, err := repos.GetRecordJSON(ctx, did, m.Collection, m.Rkey)
	if err != nil {
		t.Fatalf("record not readable: %v", err)
	}
	if !strings.Contains(string(rec), body) {
		t.Errorf("record does not carry the post body: %s", rec)
	}

	// A post with a video attachment becomes an app.bsky.embed.video record
	// whose blob is ingested from the media URL; losing the local blob file
	// must be recoverable from that URL through the getBlob path.
	// A valid H.264/AAC mp4 is stored byte for byte (no transcode), so the
	// restore path can be checked against the source bytes.
	video := bytes.Repeat([]byte{0x5a}, 300_000)
	if tc != nil {
		video = mediatest.Fixture(t, "libx264", "aac", "mp4")
	}
	var mediaHits int
	media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mediaHits++
		w.Header().Set("Content-Type", "video/mp4")
		w.Write(video)
	}))
	defer media.Close()
	videoKey := fmt.Sprintf("cckv://%s/e2etest/%d", ccid, time.Now().UnixNano())
	videoDoc := concrnt.Document[world.MessageValue]{
		Kind: "record", Key: videoKey, Schema: world.SchemaMarkdown,
		Value: world.MessageValue{
			Body:   "outbound e2e video post",
			Medias: []world.Media{{MediaURL: media.URL + "/clip.mp4", MediaType: "video/mp4"}},
		},
		Author: ccid, CreatedAt: time.Now().UTC(), Distributes: &[]string{home},
	}
	if _, err := core.Commit(ctx, coreSvc, videoDoc); err != nil {
		t.Fatalf("video post commit failed: %v", err)
	}
	var vm store.URIMap
	deadline = time.Now().Add(15 * time.Second)
	for {
		if err := db.Where("cc_uri = ? AND ref_type = ?", videoKey, "outbound-post").First(&vm).Error; err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the daemon to export the video post")
		}
		time.Sleep(200 * time.Millisecond)
	}
	vrec, err := repos.GetRecordJSON(ctx, did, vm.Collection, vm.Rkey)
	if err != nil {
		t.Fatalf("video record not readable: %v", err)
	}
	if !strings.Contains(string(vrec), `"app.bsky.embed.video"`) || !strings.Contains(string(vrec), `"video/mp4"`) {
		t.Errorf("video record lacks the video embed: %s", vrec)
	}
	var blobRow store.Blob
	if err := db.Where("did = ? AND mime = ?", did, "video/mp4").First(&blobRow).Error; err != nil {
		t.Fatalf("video blob row missing: %v", err)
	}
	if blobRow.Size != int64(len(video)) {
		t.Errorf("blob size = %d, want %d", blobRow.Size, len(video))
	}
	if mediaHits != 1 {
		t.Errorf("media origin hits after ingest = %d, want 1", mediaHits)
	}
	if err := os.Remove(filepath.Join(blobDir, "blobs", did, blobRow.CID)); err != nil {
		t.Fatalf("could not remove blob file: %v", err)
	}
	_, r, err := blobSvc.Open(ctx, did, blobRow.CID)
	if err != nil {
		t.Fatalf("blob not restored from media URL after file loss: %v", err)
	}
	restored, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(restored, video) {
		t.Error("restored blob content differs from the source")
	}
	if mediaHits != 2 {
		t.Errorf("media origin hits after restore = %d, want 2", mediaHits)
	}
	if _, err := core.Commit(ctx, coreSvc, concrnt.Document[string]{
		Kind: "delete", Schema: world.SchemaDelete, Value: videoKey,
		Author: ccid, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Logf("video cleanup delete failed: %v", err)
	}

	if tc != nil {
		testDevnetTranscodedVideo(ctx, t, db, repos, blobSvc, coreSvc, blobDir, ccid, did, home)
	}

	// Cleanup on the devnet side.
	del := concrnt.Document[string]{
		Kind: "delete", Schema: world.SchemaDelete, Value: postKey,
		Author: ccid, CreatedAt: time.Now().UTC(),
	}
	if _, err := core.Commit(ctx, coreSvc, del); err != nil {
		t.Logf("cleanup delete failed: %v", err)
	} else {
		// The daemon should delete the exported record too.
		deadline = time.Now().Add(10 * time.Second)
		for {
			if _, err := repos.GetRecordJSON(ctx, did, m.Collection, m.Rkey); err != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Error("timed out waiting for the exported record to be deleted")
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		// Distinguish real deletion from a broken repo: other records must
		// still be readable.
		if _, err := repos.GetRecordJSON(ctx, did, "app.bsky.actor.profile", "self"); err != nil {
			t.Errorf("repo unreadable after delete: %v", err)
		}
	}
}

// testDevnetTranscodedVideo posts a QuickTime (mpeg4) clip: the daemon must
// transcode it to an H.264/AAC mp4 blob, record aspectRatio from ffprobe,
// and be able to rebuild the blob by re-transcoding after file loss.
func testDevnetTranscodedVideo(ctx context.Context, t *testing.T, db *gorm.DB, repos *repoman.Manager,
	blobSvc *blobs.Service, coreSvc *core.Service, blobDir, ccid, did, home string) {
	t.Helper()
	mov := mediatest.Fixture(t, "mpeg4", "aac", "mov")
	var hits int
	media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "video/quicktime")
		w.Write(mov)
	}))
	defer media.Close()

	key := fmt.Sprintf("cckv://%s/e2etest/%d", ccid, time.Now().UnixNano())
	doc := concrnt.Document[world.MessageValue]{
		Kind: "record", Key: key, Schema: world.SchemaMarkdown,
		Value: world.MessageValue{
			Body:   "outbound e2e transcoded video post",
			Medias: []world.Media{{MediaURL: media.URL + "/clip.mov", MediaType: "video/quicktime"}},
		},
		Author: ccid, CreatedAt: time.Now().UTC(), Distributes: &[]string{home},
	}
	if _, err := core.Commit(ctx, coreSvc, doc); err != nil {
		t.Fatalf("mov post commit failed: %v", err)
	}
	var m store.URIMap
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := db.Where("cc_uri = ? AND ref_type = ?", key, "outbound-post").First(&m).Error; err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the daemon to export the mov post")
		}
		time.Sleep(200 * time.Millisecond)
	}
	defer func() {
		if _, err := core.Commit(ctx, coreSvc, concrnt.Document[string]{
			Kind: "delete", Schema: world.SchemaDelete, Value: key,
			Author: ccid, CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Logf("mov cleanup delete failed: %v", err)
		}
	}()

	rec, err := repos.GetRecordJSON(ctx, did, m.Collection, m.Rkey)
	if err != nil {
		t.Fatalf("mov record not readable: %v", err)
	}
	for _, want := range []string{`"app.bsky.embed.video"`, `"video/mp4"`, `"aspectRatio"`, `"width":320`, `"height":240`} {
		if !strings.Contains(string(rec), want) {
			t.Errorf("transcoded record lacks %s: %s", want, rec)
		}
	}
	if strings.Contains(string(rec), "video/quicktime") {
		t.Errorf("record still references the source mime: %s", rec)
	}

	var row store.Blob
	if err := db.Where("did = ? AND src_url = ?", did, media.URL+"/clip.mov").First(&row).Error; err != nil {
		t.Fatalf("transcoded blob row missing: %v", err)
	}
	if row.Mime != "video/mp4" || row.Size == int64(len(mov)) {
		t.Errorf("blob row = %+v (source was %d bytes of quicktime)", row, len(mov))
	}
	path := filepath.Join(blobDir, "blobs", did, row.CID)
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	_, r, err := blobSvc.Open(ctx, did, row.CID)
	if err != nil {
		t.Fatalf("transcoded blob not restored after file loss: %v", err)
	}
	restored, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(restored, stored) {
		t.Error("re-transcoded blob differs from the original transcode")
	}
	if hits != 2 {
		t.Errorf("media origin hits = %d, want 2 (ingest + restore)", hits)
	}
}
