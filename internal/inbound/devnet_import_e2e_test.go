package inbound

// End-to-end test of on-demand post import against a running concrnt devnet
// (the appview is faked). Skipped unless DEVNET_E2E=1.
//
//	DEVNET_E2E=1 DEVNET_URL=http://localhost:8000 \
//	DEVNET_CCID=con1... DEVNET_PRIVKEY=<hex> go test ./internal/inbound/ -run Devnet -v

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

func TestDevnetImportPost(t *testing.T) {
	if os.Getenv("DEVNET_E2E") != "1" {
		t.Skip("set DEVNET_E2E=1 to run against a devnet")
	}
	ccid := os.Getenv("DEVNET_CCID")
	privkey := os.Getenv("DEVNET_PRIVKEY")
	url := os.Getenv("DEVNET_URL")
	if url == "" {
		url = "http://localhost:8000"
	}
	if ccid == "" || privkey == "" {
		t.Fatal("DEVNET_CCID and DEVNET_PRIVKEY are required")
	}

	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "i.db")), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}

	// The post is 30 days old: older than the server's backdate window, so
	// a plain commit would be rejected and only the import path accepts it.
	atURI := fmt.Sprintf("at://did:plc:importe2e/app.bsky.feed.post/3kimp%d", time.Now().Unix())
	createdAt := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Second)
	appviewCalls := 0
	fakeAppview := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/app.bsky.feed.getPosts":
			appviewCalls++
			posts := []map[string]any{}
			for _, u := range r.URL.Query()["uris"] {
				if u != atURI {
					continue
				}
				posts = append(posts, map[string]any{
					"uri":    u,
					"cid":    "bafyimported",
					"author": map[string]any{"did": "did:plc:importe2e", "handle": "importer.test", "displayName": "Import E2E"},
					"record": map[string]any{
						"text":      "imported on demand",
						"createdAt": createdAt.Format(time.RFC3339),
						"reply":     map[string]any{"root": map[string]any{"uri": "at://did:plc:x/app.bsky.feed.post/root", "cid": "bafyroot"}},
					},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"posts": posts})
		case "/xrpc/app.bsky.actor.getProfile":
			_ = json.NewEncoder(w).Encode(map[string]any{"did": "did:plc:importe2e", "handle": "importer.test", "displayName": "Import E2E", "avatar": "https://example.com/a.png"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fakeAppview.Close)

	ctx := context.Background()
	svc := core.NewService(ccid, url, privkey)
	g := NewIngester(db, svc, appview.NewClient(fakeAppview.URL), time.Hour, nil)

	key, err := g.ImportPost(ctx, atURI)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if want := toconcrnt.InboxKey(ccid, atURI); key != want {
		t.Errorf("key = %s, want %s", key, want)
	}

	doc, err := core.GetDocument[world.AtprotoRecord](ctx, svc, key)
	if err != nil {
		t.Fatalf("resolve imported record failed: %v", err)
	}
	if doc.Value.AtURI != atURI || doc.Value.CID != "bafyimported" {
		t.Errorf("unexpected record value: %+v", doc.Value)
	}
	if !doc.CreatedAt.Equal(createdAt) {
		t.Errorf("createdAt = %s, want the post's %s (backdated past the window)", doc.CreatedAt, createdAt)
	}
	if doc.Distributes != nil && len(*doc.Distributes) > 0 {
		t.Errorf("imported record must not be distributed: %v", *doc.Distributes)
	}

	var rp store.RemotePost
	if err := db.Where("at_uri = ?", atURI).First(&rp).Error; err != nil {
		t.Errorf("remote post not cached: %v", err)
	} else if rp.CID != "bafyimported" || len(rp.Record) == 0 {
		t.Errorf("remote post cache = %+v", rp)
	}
	if !g.index.hasPost(atURI) {
		t.Error("imported post not indexed for delete handling")
	}

	// Importing again is a no-op that returns the same key without
	// consulting the appview (the record is already there).
	before := appviewCalls
	again, err := g.ImportPost(ctx, atURI)
	if err != nil || again != key {
		t.Errorf("second import = %s, %v; want %s, nil", again, err, key)
	}
	if appviewCalls != before {
		t.Errorf("second import hit the appview %d more times", appviewCalls-before)
	}

	if _, err := g.ImportPost(ctx, "at://did:plc:importe2e/app.bsky.feed.post/3knowhere"); err != ErrPostNotFound {
		t.Errorf("unknown post: err = %v, want ErrPostNotFound", err)
	}

	// Cleanup: delete the mirror so reruns stay independent.
	del := toconcrnt.BuildDeleteDoc(ccid, key)
	if _, err := core.Commit(ctx, svc, del); err != nil {
		t.Logf("cleanup delete failed: %v", err)
	}
}
