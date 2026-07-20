package mgmt

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/store"
)

func TestFollowersProxyAndCache(t *testing.T) {
	srv, _, db := setupServer(t)

	var hits atomic.Int64
	fakeAppview := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/app.bsky.graph.getFollowers" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if actor := r.URL.Query().Get("actor"); actor != "did:plc:alice" {
			t.Errorf("unexpected actor %s", actor)
		}
		hits.Add(1)
		resp := map[string]any{
			"followers": []map[string]any{
				{"did": "did:plc:bob", "handle": "bob.bsky.social", "displayName": "Bob", "avatar": "https://cdn/bob.jpg"},
			},
		}
		if r.URL.Query().Get("cursor") == "" {
			resp["cursor"] = "page2"
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(fakeAppview.Close)
	srv.appview = appview.NewClient(fakeAppview.URL)

	db.Create(&store.Entity{CCID: "con1alice", DID: "did:plc:alice", Handle: "alice.example.com", Status: "active"})
	db.Create(&store.Entity{CCID: "con1carol", Handle: "carol.example.com", Status: "pending"})

	e := echo.New()
	srv.Register(e)

	// first page: proxied, cursor surfaced.
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/atproto/api/followers", "", "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var resp struct {
		Followers []map[string]string `json:"followers"`
		Cursor    string              `json:"cursor"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Followers) != 1 || resp.Followers[0]["did"] != "did:plc:bob" || resp.Followers[0]["handle"] != "bob.bsky.social" {
		t.Fatalf("unexpected followers %+v", resp.Followers)
	}
	if resp.Cursor != "page2" {
		t.Fatalf("cursor not surfaced: %+v", resp)
	}

	// same query within TTL: served from cache, appview untouched.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/atproto/api/followers", "", "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if hits.Load() != 1 {
		t.Fatalf("cached request hit the appview: %d hits", hits.Load())
	}

	// cursor passthrough is a distinct cache key and reaches the appview.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/atproto/api/followers?cursor=page2", "", "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	resp.Cursor = ""
	resp.Followers = nil
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Cursor != "" {
		t.Fatalf("final page should omit cursor, got %q", resp.Cursor)
	}
	if hits.Load() != 2 {
		t.Fatalf("cursor page should miss the cache: %d hits", hits.Load())
	}

	// entity without a DID cannot be queried.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/atproto/api/followers", "", "con1carol"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("did-less entity should 400, got %d", rec.Code)
	}
}
