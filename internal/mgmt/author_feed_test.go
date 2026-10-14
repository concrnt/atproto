package mgmt

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/store"
)

// The author-feed endpoint resolves a handle to a DID, forwards limit/cursor
// to the appview, echoes the appview cursor back untouched, keeps the repost
// reason, drops the hydrated reply context and annotates posts this bridge
// exported with their concrnt URI (resolve parity).
func TestAuthorFeedProxiesAppview(t *testing.T) {
	srv, fr, db := setupServer(t)
	fr.table["alice.example.com"] = "did:plc:alice"

	const exported = "at://did:plc:alice/app.bsky.feed.post/3kexported"
	const native = "at://did:plc:alice/app.bsky.feed.post/3knative"
	const reposted = "at://did:plc:bob/app.bsky.feed.post/3kbob"
	db.Create(&store.URIMap{AtURI: exported, CcURI: "cckv://con1alice/concrnt.world/profiles/main/posts/p1", DID: "did:plc:alice", RefType: "outbound-post"})
	// A row of another ref type for the same at-uri must not count as exported.
	db.Create(&store.URIMap{AtURI: native, CcURI: "cckv://svc/reply", DID: "did:plc:alice", RefType: "inbound-reply"})

	var lastQuery url.Values
	fakeAppview := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/app.bsky.feed.getAuthorFeed" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		lastQuery = r.URL.Query()
		if lastQuery.Get("actor") == "did:plc:fail" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"InvalidRequest"}`))
			return
		}
		post := func(uri string, did string) map[string]any {
			return map[string]any{
				"uri":    uri,
				"author": map[string]any{"did": did, "handle": did + ".test"},
				"record": map[string]any{"text": "hello"},
			}
		}
		feed := []map[string]any{
			{"post": post(exported, "did:plc:alice")},
			{"post": post(native, "did:plc:alice"), "reply": map[string]any{"parent": post("at://x/app.bsky.feed.post/p", "did:plc:x")}},
			{"post": post(reposted, "did:plc:bob"), "reason": map[string]any{"$type": "app.bsky.feed.defs#reasonRepost", "by": map[string]any{"did": "did:plc:alice"}}},
			{"notAPost": true},
		}
		resp := map[string]any{"feed": feed}
		if lastQuery.Get("cursor") == "" {
			resp["cursor"] = "next-page"
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(fakeAppview.Close)
	srv.appview = appview.NewClient(fakeAppview.URL)

	e := echo.New()
	srv.Register(e)

	type item struct {
		Post   map[string]any `json:"post"`
		Reason map[string]any `json:"reason"`
		Reply  map[string]any `json:"reply"`
	}
	call := func(query string) (int, []item, string, string) {
		t.Helper()
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/atproto/api/author-feed?"+query, nil))
		var resp struct {
			Feed   []item `json:"feed"`
			Cursor string `json:"cursor"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		return rec.Code, resp.Feed, resp.Cursor, rec.Body.String()
	}

	code, feed, cursor, body := call("actor=@alice.example.com&limit=7")
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	if lastQuery.Get("actor") != "did:plc:alice" || lastQuery.Get("limit") != "7" || lastQuery.Has("cursor") {
		t.Fatalf("appview query %v", lastQuery)
	}
	if cursor != "next-page" {
		t.Fatalf("cursor %q", cursor)
	}
	if len(feed) != 3 {
		t.Fatalf("expected 3 items, got %s", body)
	}
	if feed[0].Post["concrntURI"] != "cckv://con1alice/concrnt.world/profiles/main/posts/p1" {
		t.Errorf("exported post not annotated: %v", feed[0].Post)
	}
	if _, ok := feed[1].Post["concrntURI"]; ok {
		t.Errorf("non-exported post annotated: %v", feed[1].Post)
	}
	if feed[1].Reply != nil {
		t.Errorf("reply context should be dropped: %v", feed[1].Reply)
	}
	if feed[0].Reason != nil || feed[2].Reason["$type"] != "app.bsky.feed.defs#reasonRepost" {
		t.Errorf("reason mismatch: %v / %v", feed[0].Reason, feed[2].Reason)
	}
	if feed[2].Post["uri"] != reposted {
		t.Errorf("repost body should be the original post: %v", feed[2].Post)
	}

	// Second page: cursor forwarded, out-of-range limit falls back to the default, no cursor echoed when absent.
	code, _, cursor, body = call("actor=did:plc:alice&cursor=next-page&limit=1000")
	if code != http.StatusOK {
		t.Fatalf("status %d: %s", code, body)
	}
	if lastQuery.Get("cursor") != "next-page" || lastQuery.Get("limit") != "30" {
		t.Fatalf("appview query %v", lastQuery)
	}
	if cursor != "" {
		t.Fatalf("cursor should be absent on the last page, got %q", cursor)
	}

	if code, _, _, _ := call("actor="); code != http.StatusBadRequest {
		t.Errorf("missing actor: status %d", code)
	}
	if code, _, _, _ := call("actor=unknown.example.com"); code != http.StatusNotFound {
		t.Errorf("unresolvable handle: status %d", code)
	}
	if code, _, _, _ := call("actor=did:plc:fail"); code != http.StatusBadGateway {
		t.Errorf("appview failure: status %d", code)
	}
}
