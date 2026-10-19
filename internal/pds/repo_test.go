package pds

import (
	"context"
	"net/http"
	"testing"

	"github.com/bluesky-social/indigo/repomgr"
)

func TestDeclarationWriteAndRepoReads(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)

	var commits int
	h.srv.repos.RepoManager().SetEventHandler(func(ctx context.Context, evt *repomgr.RepoEvent) {
		commits++
	}, false)

	// putRecord of the declaration is the one allowed write.
	rec, out := h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.putRecord", map[string]any{
		"repo": h.handle, "collection": declarationCollection, "rkey": "self",
		"record": map[string]any{"$type": declarationCollection, "allowIncoming": "all"},
	}, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("putRecord: %d %s", rec.Code, rec.Body)
	}
	if out["uri"] != "at://"+h.did+"/"+declarationCollection+"/self" || out["cid"] == "" {
		t.Fatalf("putRecord body: %v", out)
	}

	rec, out = h.do(t, http.MethodGet, "/xrpc/com.atproto.repo.getRecord?repo="+h.did+"&collection="+declarationCollection+"&rkey=self", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("getRecord: %d %s", rec.Code, rec.Body)
	}
	if v := out["value"].(map[string]any); v["allowIncoming"] != "all" || v["$type"] != declarationCollection {
		t.Fatalf("getRecord value: %v", out)
	}

	rec, out = h.do(t, http.MethodGet, "/xrpc/com.atproto.repo.listRecords?repo="+h.handle+"&collection="+declarationCollection, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("listRecords: %d %s", rec.Code, rec.Body)
	}
	if recs := out["records"].([]any); len(recs) != 1 || recs[0].(map[string]any)["uri"] != "at://"+h.did+"/"+declarationCollection+"/self" {
		t.Fatalf("listRecords body: %v", out)
	}

	rec, out = h.do(t, http.MethodGet, "/xrpc/com.atproto.repo.describeRepo?repo="+h.did, nil, nil)
	if rec.Code != http.StatusOK || out["handle"] != h.handle || out["handleIsCorrect"] != true {
		t.Fatalf("describeRepo: %d %s", rec.Code, rec.Body)
	}
	found := false
	for _, c := range out["collections"].([]any) {
		if c == declarationCollection {
			found = true
		}
	}
	if !found || out["didDoc"] == nil {
		t.Fatalf("describeRepo collections/didDoc: %v", out)
	}

	// applyWrites update, then delete.
	rec, out = h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", map[string]any{
		"repo": h.did,
		"writes": []map[string]any{{
			"$type": "com.atproto.repo.applyWrites#update", "collection": declarationCollection, "rkey": "self",
			"value": map[string]any{"$type": declarationCollection, "allowIncoming": "following"},
		}},
	}, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("applyWrites: %d %s", rec.Code, rec.Body)
	}
	if res := out["results"].([]any); len(res) != 1 || res[0].(map[string]any)["$type"] != "com.atproto.repo.applyWrites#updateResult" {
		t.Fatalf("applyWrites results: %v", out)
	}
	_, got, err := h.srv.repos.GetRecordRaw(context.Background(), h.did, declarationCollection, "self")
	if err != nil || got["allowIncoming"] != "following" {
		t.Fatalf("after update: %v %v", got, err)
	}

	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", map[string]any{
		"repo": h.did, "collection": declarationCollection, "rkey": "self",
	}, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("deleteRecord: %d %s", rec.Code, rec.Body)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.repo.getRecord?repo="+h.did+"&collection="+declarationCollection+"&rkey=self", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("getRecord after delete: %d", rec.Code)
	}
	if commits < 3 {
		t.Fatalf("expected firehose commits for put/update/delete, got %d", commits)
	}
}

func TestRepoWritesAreReadOnlyExceptDeclaration(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)

	post := map[string]any{
		"repo": h.did, "collection": "app.bsky.feed.post",
		"record": map[string]any{"$type": "app.bsky.feed.post", "text": "hello", "createdAt": "2026-01-01T00:00:00Z"},
	}
	for _, m := range []string{"createRecord", "putRecord"} {
		rec, out := h.do(t, http.MethodPost, "/xrpc/com.atproto.repo."+m, post, h.authed(nil))
		if rec.Code != http.StatusForbidden || out["error"] != readOnlyErrorName {
			t.Fatalf("%s post: %d %s", m, rec.Code, rec.Body)
		}
	}
	rec, out := h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord",
		map[string]any{"repo": h.did, "collection": "app.bsky.feed.post", "rkey": "abc"}, h.authed(nil))
	if rec.Code != http.StatusForbidden || out["error"] != readOnlyErrorName {
		t.Fatalf("deleteRecord post: %d %s", rec.Code, rec.Body)
	}

	// Mixed batch: refused as a whole, nothing committed.
	rec, out = h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", map[string]any{
		"repo": h.did,
		"writes": []map[string]any{
			{"$type": "com.atproto.repo.applyWrites#create", "collection": declarationCollection, "rkey": "self",
				"value": map[string]any{"allowIncoming": "all"}},
			{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.like", "rkey": "x",
				"value": map[string]any{"subject": map[string]any{"uri": "at://x", "cid": "y"}}},
		},
	}, h.authed(nil))
	if rec.Code != http.StatusForbidden || out["error"] != readOnlyErrorName {
		t.Fatalf("mixed applyWrites: %d %s", rec.Code, rec.Body)
	}
	if _, _, err := h.srv.repos.GetRecordRaw(context.Background(), h.did, declarationCollection, "self"); err == nil {
		t.Fatalf("mixed batch must not commit the declaration")
	}

	// Blob upload and the other account-management methods.
	for _, m := range []string{"com.atproto.repo.uploadBlob", "com.atproto.server.createAccount", "com.atproto.identity.updateHandle"} {
		rec, out := h.do(t, http.MethodPost, "/xrpc/"+m, "{}", h.authed(nil))
		if rec.Code != http.StatusForbidden || out["error"] != readOnlyErrorName {
			t.Fatalf("%s: %d %s", m, rec.Code, rec.Body)
		}
	}
	// ...also without authentication.
	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.uploadBlob", "{}", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("uploadBlob unauthenticated: %d", rec.Code)
	}

	// Declaration writes still need a session and the caller's own repo.
	decl := map[string]any{"repo": h.did, "collection": declarationCollection, "rkey": "self",
		"record": map[string]any{"allowIncoming": "all"}}
	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.putRecord", decl, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated declaration: %d", rec.Code)
	}
	decl["repo"] = "did:plc:someoneelse"
	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.putRecord", decl, h.authed(nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("other repo declaration: %d", rec.Code)
	}
	decl["repo"] = h.did
	decl["record"] = map[string]any{"allowIncoming": "everyone"}
	rec, out = h.do(t, http.MethodPost, "/xrpc/com.atproto.repo.putRecord", decl, h.authed(nil))
	if rec.Code != http.StatusBadRequest || out["error"] != "InvalidRecord" {
		t.Fatalf("invalid allowIncoming: %d %s", rec.Code, rec.Body)
	}
}

func TestPreferencesRoundTrip(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)

	rec, out := h.do(t, http.MethodGet, "/xrpc/app.bsky.actor.getPreferences", nil,
		h.authed(map[string]string{"atproto-proxy": "did:web:appview.test#bsky_appview"}))
	if rec.Code != http.StatusOK {
		t.Fatalf("getPreferences: %d %s", rec.Code, rec.Body)
	}
	if prefs := out["preferences"].([]any); len(prefs) != 0 {
		t.Fatalf("initial prefs: %v", out)
	}

	rec, _ = h.do(t, http.MethodPost, "/xrpc/app.bsky.actor.putPreferences",
		`{"preferences":[{"$type":"app.bsky.actor.defs#adultContentPref","enabled":true}]}`,
		h.authed(map[string]string{"atproto-proxy": "did:web:appview.test#bsky_appview"}))
	if rec.Code != http.StatusOK {
		t.Fatalf("putPreferences: %d %s", rec.Code, rec.Body)
	}
	rec, out = h.do(t, http.MethodGet, "/xrpc/app.bsky.actor.getPreferences", nil, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("getPreferences 2: %d %s", rec.Code, rec.Body)
	}
	prefs := out["preferences"].([]any)
	if len(prefs) != 1 || prefs[0].(map[string]any)["enabled"] != true {
		t.Fatalf("stored prefs: %v", out)
	}

	rec, _ = h.do(t, http.MethodPost, "/xrpc/app.bsky.actor.putPreferences", `{"preferences":{"not":"array"}}`, h.authed(nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("non-array prefs: %d", rec.Code)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/app.bsky.actor.getPreferences", nil, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated prefs: %d", rec.Code)
	}
}
