package outbound_test

// End-to-end test of the outbound path against a running concrnt devnet:
// a real post commit → core Redis event → daemon → bsky record in the repo.
// Skipped unless DEVNET_E2E=1 (see internal/core/devnet_e2e_test.go for the
// environment variables). Requires the devnet's Redis on localhost:6379.

import (
	"context"
	"fmt"
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

	daemon := outbound.NewDaemon(db, coreSvc, repos, blobs.NewService(db, t.TempDir()), "", time.Second)
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
