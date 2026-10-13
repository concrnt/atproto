package mgmt

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/store"
)

// The following list must show a followee's handle/name even when the
// bridge has never ingested a post from them (at_actors row absent), and a
// stale row must be refreshed; fresh rows are served without touching the
// appview.
func TestFollowingHydratesUncachedActors(t *testing.T) {
	srv, _, db := setupServer(t)

	var hits atomic.Int64
	var lastActors []string
	fakeAppview := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/app.bsky.actor.getProfiles" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		hits.Add(1)
		lastActors = r.URL.Query()["actors"]
		profiles := []map[string]any{}
		for _, a := range lastActors {
			switch a {
			case "did:plc:bob":
				profiles = append(profiles, map[string]any{"did": a, "handle": "bob.bsky.social", "displayName": "Bob", "avatar": "https://cdn/bob.jpg"})
			case "did:plc:stale":
				profiles = append(profiles, map[string]any{"did": a, "handle": "stale.new", "displayName": "Stale (renamed)"})
			}
			// did:plc:gone is unresolvable and simply omitted by the appview.
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"profiles": profiles})
	}))
	t.Cleanup(fakeAppview.Close)
	srv.appview = appview.NewClient(fakeAppview.URL)

	db.Create(&store.Entity{CCID: "con1alice", DID: "did:plc:alice", Handle: "alice.example.com", Status: "active"})
	db.Create(&store.Actor{DID: "did:plc:fresh", Handle: "fresh.bsky.social", DisplayName: "Fresh", FetchedAt: time.Now()})
	db.Create(&store.Actor{DID: "did:plc:stale", Handle: "stale.old", DisplayName: "Stale", FetchedAt: time.Now().Add(-2 * srv.ActorTTL)})
	for _, did := range []string{"did:plc:bob", "did:plc:fresh", "did:plc:stale", "did:plc:gone"} {
		db.Create(&store.Follow{CCID: "con1alice", SubjectDID: did, FollowRkey: "rk-" + did})
	}

	e := echo.New()
	srv.Register(e)

	type entry struct {
		DID         string `json:"did"`
		Handle      string `json:"handle"`
		DisplayName string `json:"displayName"`
		Avatar      string `json:"avatar"`
	}
	call := func() map[string]entry {
		t.Helper()
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, authedReq(http.MethodGet, "/atproto/api/following", "", "con1alice"))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
		var resp struct {
			Following []entry `json:"following"`
		}
		json.Unmarshal(rec.Body.Bytes(), &resp)
		if len(resp.Following) != 4 {
			t.Fatalf("expected 4 entries, got %+v", resp.Following)
		}
		byDID := map[string]entry{}
		for _, f := range resp.Following {
			byDID[f.DID] = f
		}
		return byDID
	}

	got := call()
	if hits.Load() != 1 {
		t.Fatalf("expected one batched appview call, got %d", hits.Load())
	}
	if len(lastActors) != 3 {
		t.Fatalf("only missing/stale actors should be requested, got %v", lastActors)
	}
	for _, a := range lastActors {
		if a == "did:plc:fresh" {
			t.Fatalf("fresh actor must not be refetched: %v", lastActors)
		}
	}
	if got["did:plc:bob"].Handle != "bob.bsky.social" || got["did:plc:bob"].DisplayName != "Bob" || got["did:plc:bob"].Avatar != "https://cdn/bob.jpg" {
		t.Fatalf("uncached followee not hydrated: %+v", got["did:plc:bob"])
	}
	if got["did:plc:stale"].Handle != "stale.new" {
		t.Fatalf("stale followee not refreshed: %+v", got["did:plc:stale"])
	}
	if got["did:plc:fresh"].Handle != "fresh.bsky.social" {
		t.Fatalf("cached followee lost: %+v", got["did:plc:fresh"])
	}
	if got["did:plc:gone"].Handle != "" {
		t.Fatalf("unresolvable followee should stay bare: %+v", got["did:plc:gone"])
	}

	// hydrated rows are persisted, so the next listing only retries the
	// unresolvable one.
	got = call()
	if hits.Load() != 2 || len(lastActors) != 1 || lastActors[0] != "did:plc:gone" {
		t.Fatalf("second call should only retry the unresolved actor: hits=%d actors=%v", hits.Load(), lastActors)
	}
	if got["did:plc:bob"].Handle != "bob.bsky.social" {
		t.Fatalf("hydrated followee not persisted: %+v", got["did:plc:bob"])
	}

	// appview outage: cached (even stale) data is still served.
	fakeAppview.Close()
	got = call()
	if got["did:plc:bob"].Handle != "bob.bsky.social" {
		t.Fatalf("listing must survive appview failure: %+v", got["did:plc:bob"])
	}
}
