package mgmt

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/store"
)

// The resolve endpoint passes the appview post view through, and annotates
// posts this bridge exported from concrnt with the original cckv URI so
// clients can render them natively (AP bridge parity).
func TestResolveRecordAnnotatesExportedPosts(t *testing.T) {
	srv, _, db := setupServer(t)

	fakeAppview := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/app.bsky.feed.getPosts" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		posts := []map[string]any{}
		for _, u := range r.URL.Query()["uris"] {
			posts = append(posts, map[string]any{
				"uri":    u,
				"author": map[string]any{"did": "did:plc:alice", "handle": "alice.example.com"},
				"record": map[string]any{"text": "hello"},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"posts": posts})
	}))
	t.Cleanup(fakeAppview.Close)
	srv.appview = appview.NewClient(fakeAppview.URL)

	const exported = "at://did:plc:alice/app.bsky.feed.post/3kexported"
	const native = "at://did:plc:bob/app.bsky.feed.post/3knative"
	db.Create(&store.URIMap{AtURI: exported, CcURI: "cckv://con1alice/concrnt.world/profiles/main/posts/p1", DID: "did:plc:alice", RefType: "outbound-post"})
	// A row of another ref type for the same at-uri must not count as exported.
	db.Create(&store.URIMap{AtURI: native, CcURI: "cckv://svc/reply", DID: "did:plc:bob", RefType: "inbound-reply"})

	e := echo.New()
	srv.Register(e)

	call := func(uri string) map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/atproto/api/resolve?uri="+uri, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
		var view map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
			t.Fatal(err)
		}
		return view
	}

	view := call(exported)
	if view["concrntURI"] != "cckv://con1alice/concrnt.world/profiles/main/posts/p1" {
		t.Fatalf("expected concrntURI on exported post, got %v", view["concrntURI"])
	}
	if view["uri"] != exported || view["record"].(map[string]any)["text"] != "hello" {
		t.Fatalf("appview view not passed through: %v", view)
	}

	view = call(native)
	if _, ok := view["concrntURI"]; ok {
		t.Fatalf("native post must not carry concrntURI: %v", view)
	}
	if view["uri"] != native {
		t.Fatalf("appview view not passed through: %v", view)
	}
}
