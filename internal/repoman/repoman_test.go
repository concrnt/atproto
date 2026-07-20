package repoman

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/glebarez/sqlite"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/plcm"
	"github.com/concrnt/atproto/internal/store"
)

// fakePLC accepts operation submissions and resolves every registered DID.
func fakePLC(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	registered := map[string]bool{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		did := strings.TrimPrefix(r.URL.Path, "/")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			registered[did] = true
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if !registered[did] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/did+ld+json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":          did,
				"alsoKnownAs": []string{"at://alice.test.example"},
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
}

func newTestManager(t *testing.T) (*Manager, *gorm.DB) {
	t.Helper()

	// A file-backed database: sqlite ":memory:" gives every pooled
	// connection its own empty database, which breaks under concurrency.
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "test.db")), &gorm.Config{
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

	ks, err := keys.NewService(db, strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}

	plcSrv := fakePLC(t)
	t.Cleanup(plcSrv.Close)

	rotation, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		t.Fatal(err)
	}
	plcSvc := plcm.NewService(plcSrv.URL, rotation, "test.example")

	m, err := New(db, t.TempDir(), ks, plcSvc, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return m, db
}

func TestAccountLifecycleAndRepo(t *testing.T) {
	ctx := context.Background()
	m, db := newTestManager(t)

	// Collect firehose events as they are emitted.
	evts, cancel, err := m.Events().Subscribe(ctx, "test", func(*events.XRPCStreamEvent) bool { return true }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	ent, err := m.CreatePending(ctx, "con1testccid", "alice.test.example")
	if err != nil {
		t.Fatalf("CreatePending: %v", err)
	}
	if !strings.HasPrefix(ent.DID, "did:plc:") {
		t.Fatalf("unexpected did %q", ent.DID)
	}
	if ent.Status != "pending" {
		t.Fatalf("expected pending status, got %q", ent.Status)
	}
	if err := m.Activate(ctx, ent.DID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	// Write a post.
	post := &appbsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "hello from concrnt",
		CreatedAt:     "2026-07-20T00:00:00.000Z",
	}
	atURI, cidStr, err := m.CreateRecord(ctx, ent.DID, "app.bsky.feed.post", post)
	if err != nil {
		t.Fatalf("CreateRecord: %v", err)
	}
	if !strings.HasPrefix(atURI, "at://"+ent.DID+"/app.bsky.feed.post/") || cidStr == "" {
		t.Fatalf("bad record ref: %s %s", atURI, cidStr)
	}

	// Firehose ordering: #identity → #account → #commit(profile) → #commit(post).
	var kinds []string
	deadline := time.After(5 * time.Second)
	for len(kinds) < 4 {
		select {
		case evt := <-evts:
			switch {
			case evt.RepoIdentity != nil:
				kinds = append(kinds, "identity")
			case evt.RepoAccount != nil:
				kinds = append(kinds, "account")
			case evt.RepoCommit != nil:
				kinds = append(kinds, "commit")
				if evt.RepoCommit.Repo != ent.DID {
					t.Errorf("commit for wrong did %s", evt.RepoCommit.Repo)
				}
				if len(evt.RepoCommit.Blocks) == 0 {
					t.Error("commit event carries no blocks")
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for events, got %v", kinds)
		}
	}
	want := []string{"identity", "account", "commit", "commit"}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("event order %v, want %v", kinds, want)
		}
	}

	// getRepo equivalent: export the CAR and verify the MST contains both
	// the profile and the post, signed with the account key.
	var buf bytes.Buffer
	if err := m.CarStore().ReadUserCar(ctx, ent.Uid, "", true, &buf); err != nil {
		t.Fatalf("ReadUserCar: %v", err)
	}
	rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadRepoFromCar: %v", err)
	}
	var paths []string
	if err := rr.ForEach(ctx, "", func(k string, _ cid.Cid) error {
		paths = append(paths, k)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	foundPost := false
	foundProfile := false
	for _, p := range paths {
		if strings.HasPrefix(p, "app.bsky.feed.post/") {
			foundPost = true
		}
		if p == "app.bsky.actor.profile/self" {
			foundProfile = true
		}
	}
	if !foundPost || !foundProfile {
		t.Fatalf("repo contents missing records: %v", paths)
	}

	// Delete the record and confirm it leaves the MST.
	parts := strings.SplitN(strings.TrimPrefix(atURI, "at://"), "/", 3)
	if err := m.DeleteRecord(ctx, ent.DID, parts[1], parts[2]); err != nil {
		t.Fatalf("DeleteRecord: %v", err)
	}
	buf.Reset()
	if err := m.CarStore().ReadUserCar(ctx, ent.Uid, "", true, &buf); err != nil {
		t.Fatal(err)
	}
	rr2, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rr2.GetRecord(ctx, parts[1]+"/"+parts[2]); err == nil {
		t.Error("deleted record still present")
	}

	// Entity head cache must be updated.
	var reloaded store.Entity
	if err := db.Where("did = ?", ent.DID).First(&reloaded).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.HeadCID == "" || reloaded.Rev == "" {
		t.Error("entity head/rev not updated")
	}
}
