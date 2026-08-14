package outbound_test

// End-to-end tests of the cckv settings record and the follow reconcile
// (backfill) against a running concrnt devnet. Same environment contract as
// devnet_e2e_test.go: DEVNET_E2E=1, DEVNET_CCID, DEVNET_PRIVKEY, optionally
// DEVNET_URL / DEVNET_REDIS.

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
	"github.com/concrnt/concrnt/cdid"
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

type devnetHarness struct {
	ccid     string
	did      string
	redisURL string
	db       *gorm.DB
	repos    *repoman.Manager
	core     *core.Service
	blobs    *blobs.Service
}

func newDevnetHarness(t *testing.T, did string) *devnetHarness {
	t.Helper()
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

	signing, _ := keys.Generate()
	if err := ks.StoreSigningKey(did, signing); err != nil {
		t.Fatal(err)
	}
	ent := store.Entity{CCID: ccid, DID: did, Handle: "e2e.test.example", Status: "active", Enabled: true}
	if err := db.Create(&ent).Error; err != nil {
		t.Fatal(err)
	}
	if err := repos.InitActor(context.Background(), did, "e2e.test.example"); err != nil {
		t.Fatal(err)
	}

	return &devnetHarness{
		ccid:     ccid,
		did:      did,
		redisURL: redisURL,
		db:       db,
		repos:    repos,
		core:     core.NewService(ccid, devnetURL, privkey),
		blobs:    blobs.NewService(db, t.TempDir()),
	}
}

func (h *devnetHarness) startDaemon(t *testing.T, ctx context.Context) *outbound.Daemon {
	t.Helper()
	events, err := core.Events(ctx, h.redisURL)
	if err != nil {
		t.Fatalf("redis subscribe failed: %v", err)
	}
	daemon := outbound.NewDaemon(h.db, h.core, h.repos, h.blobs, "", time.Second)
	go daemon.Run(ctx, events)
	return daemon
}

func (h *devnetHarness) commitSettings(t *testing.T, ctx context.Context, timelines []string, enabled bool) {
	t.Helper()
	doc := concrnt.Document[world.AtprotoSettings]{
		Kind:      "record",
		Key:       fmt.Sprintf("cckv://%s/%s", h.ccid, world.SettingsKeySuffix),
		Schema:    world.SchemaAtprotoSettings,
		Value:     world.AtprotoSettings{ListenTimelines: timelines, Enabled: &enabled},
		Author:    h.ccid,
		CreatedAt: time.Now().UTC(),
	}
	if _, err := core.Commit(ctx, h.core, doc); err != nil {
		t.Fatalf("settings commit failed: %v", err)
	}
}

func (h *devnetHarness) deleteRecord(t *testing.T, ctx context.Context, key string) {
	t.Helper()
	del := concrnt.Document[string]{
		Kind: "delete", Schema: world.SchemaDelete, Value: key,
		Author: h.ccid, CreatedAt: time.Now().UTC(),
	}
	if _, err := core.Commit(ctx, h.core, del); err != nil {
		t.Logf("cleanup delete of %s failed: %v", key, err)
	}
}

func (h *devnetHarness) post(t *testing.T, ctx context.Context, timeline string) string {
	t.Helper()
	key := fmt.Sprintf("cckv://%s/e2etest/%d", h.ccid, time.Now().UnixNano())
	doc := concrnt.Document[world.MessageValue]{
		Kind:        "record",
		Key:         key,
		Schema:      world.SchemaMarkdown,
		Value:       world.MessageValue{Body: fmt.Sprintf("settings e2e %d", time.Now().UnixNano())},
		Author:      h.ccid,
		CreatedAt:   time.Now().UTC(),
		Distributes: &[]string{timeline},
	}
	if _, err := core.Commit(ctx, h.core, doc); err != nil {
		t.Fatalf("post commit failed: %v", err)
	}
	return key
}

func (h *devnetHarness) waitExported(t *testing.T, key string) bool {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var m store.URIMap
		if err := h.db.Where("cc_uri = ? AND ref_type = ?", key, "outbound-post").First(&m).Error; err == nil {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// TestDevnetSettingsRecord pins the cckv settings record as the source of
// truth: listenTimelines steer which posts get exported and enabled gates the
// whole daemon, with the at_entities.enabled column following.
func TestDevnetSettingsRecord(t *testing.T) {
	h := newDevnetHarness(t, "did:plc:settingse2etest")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.startDaemon(t, ctx)
	time.Sleep(500 * time.Millisecond)

	custom := fmt.Sprintf("cckv://%s/e2e-listen/%d", h.ccid, time.Now().UnixNano())
	home := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/home-timeline", h.ccid)
	settingsUri := fmt.Sprintf("cckv://%s/%s", h.ccid, world.SettingsKeySuffix)
	defer h.deleteRecord(t, context.Background(), settingsUri)

	// Point the daemon at the custom timeline (replacing the home fallback).
	h.commitSettings(t, ctx, []string{custom}, true)
	time.Sleep(2 * time.Second) // let the settings event apply

	postCustom := h.post(t, ctx, custom)
	defer h.deleteRecord(t, context.Background(), postCustom)
	if !h.waitExported(t, postCustom) {
		t.Fatal("post to the configured listen timeline was not exported")
	}

	postHome := h.post(t, ctx, home)
	defer h.deleteRecord(t, context.Background(), postHome)
	time.Sleep(4 * time.Second)
	var count int64
	h.db.Model(&store.URIMap{}).Where("cc_uri = ?", postHome).Count(&count)
	if count != 0 {
		t.Fatal("post to the home timeline was exported despite not being listened to")
	}

	// Disabling via the record must stop exports and follow into the DB cache.
	h.commitSettings(t, ctx, []string{custom}, false)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var ent store.Entity
		if err := h.db.Where("ccid = ?", h.ccid).First(&ent).Error; err == nil && !ent.Enabled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("at_entities.enabled did not follow the settings record")
		}
		time.Sleep(200 * time.Millisecond)
	}
	postDisabled := h.post(t, ctx, custom)
	defer h.deleteRecord(t, context.Background(), postDisabled)
	time.Sleep(4 * time.Second)
	h.db.Model(&store.URIMap{}).Where("cc_uri = ?", postDisabled).Count(&count)
	if count != 0 {
		t.Fatal("post was exported while the bridge was disabled")
	}

	// Deleting the record restores the defaults (enabled again).
	h.deleteRecord(t, ctx, settingsUri)
	deadline = time.Now().Add(10 * time.Second)
	for {
		var ent store.Entity
		if err := h.db.Where("ccid = ?", h.ccid).First(&ent).Error; err == nil && ent.Enabled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deleting the settings record did not restore enabled")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestDevnetFollowReconcile pins the backfill: follow documents committed or
// deleted while the daemon is down are reconciled into the repo on startup.
func TestDevnetFollowReconcile(t *testing.T) {
	h := newDevnetHarness(t, "did:plc:reconcilee2etest")

	subject := fmt.Sprintf("did:plc:reconciletarget%d", time.Now().Unix())
	// Same key shape as the client's followKey (hash of the subject DID).
	followUri := fmt.Sprintf("cckv://%s/%s%s", h.ccid, world.FollowsKeyPrefix, cdid.MakeHash([]byte(subject)).String())

	// Committed while no daemon is running: only the backfill can see it.
	setupCtx := context.Background()
	doc := concrnt.Document[world.AtprotoFollow]{
		Kind:      "record",
		Key:       followUri,
		Schema:    world.SchemaAtprotoFollow,
		Value:     world.AtprotoFollow{DID: subject},
		Author:    h.ccid,
		CreatedAt: time.Now().UTC(),
	}
	if _, err := core.Commit(setupCtx, h.core, doc); err != nil {
		t.Fatalf("follow commit failed: %v", err)
	}
	defer h.deleteRecord(t, setupCtx, followUri)

	ctx1, cancel1 := context.WithCancel(context.Background())
	h.startDaemon(t, ctx1)

	var follow store.Follow
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := h.db.Where("ccid = ? AND subject_did = ?", h.ccid, subject).First(&follow).Error; err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel1()
			t.Fatal("reconcile did not backfill the offline follow")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := h.repos.GetRecordJSON(setupCtx, h.did, "app.bsky.graph.follow", follow.FollowRkey); err != nil {
		cancel1()
		t.Fatalf("backfilled follow record not readable: %v", err)
	}

	// Stop the daemon, unfollow on the devnet, restart: the sweep must
	// remove the stale mirror.
	cancel1()
	time.Sleep(500 * time.Millisecond)
	h.deleteRecord(t, setupCtx, followUri)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	h.startDaemon(t, ctx2)

	deadline = time.Now().Add(20 * time.Second)
	for {
		var count int64
		h.db.Model(&store.Follow{}).Where("ccid = ? AND subject_did = ?", h.ccid, subject).Count(&count)
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reconcile did not remove the offline unfollow")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := h.repos.GetRecordJSON(ctx2, h.did, "app.bsky.graph.follow", follow.FollowRkey); err == nil {
		t.Fatal("stale follow record still present in the repo")
	}
}
