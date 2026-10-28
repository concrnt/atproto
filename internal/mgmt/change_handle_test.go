package mgmt

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/store"
)

// activateAccount runs setup+verify for ccid/handle and returns the row.
func activateAccount(t *testing.T, e *echo.Echo, fr *fakeResolver, ccid, handle string) store.Entity {
	t.Helper()
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/setup", `{"handle":"`+handle+`"}`, ccid))
	if rec.Code != http.StatusOK {
		t.Fatalf("setup status %d: %s", rec.Code, rec.Body)
	}
	var resp struct {
		DID string `json:"did"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	fr.table[handle] = resp.DID
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/verify", ``, ccid))
	if rec.Code != http.StatusOK {
		t.Fatalf("verify status %d: %s", rec.Code, rec.Body)
	}
	return store.Entity{CCID: ccid, DID: resp.DID, Handle: handle}
}

func TestChangeHandleActive(t *testing.T) {
	srv, fr, db := setupServer(t)
	e := echo.New()
	srv.Register(e)
	ctx := context.Background()

	// Not bridged → 404; pending → 409.
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"x.example.com"}`, "con1nobody"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unbridged change-handle status %d: %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/setup", `{"handle":"pend.example.com"}`, "con1pend"))
	if rec.Code != http.StatusOK {
		t.Fatalf("setup status %d: %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"x.example.com"}`, "con1pend"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("pending change-handle status %d: %s", rec.Code, rec.Body)
	}

	alice := activateAccount(t, e, fr, "con1alice", "alice.example.com")
	activateAccount(t, e, fr, "con1bob", "bob.example.com")

	// A record in alice's repo must survive the handle change untouched.
	text := "hello"
	uri, _, err := srv.repos.CreateRecord(ctx, alice.DID, "app.bsky.feed.post", &bsky.FeedPost{Text: text, CreatedAt: time.Now().UTC().Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	rkey := uri[strings.LastIndex(uri, "/")+1:]
	var before store.Entity
	db.Where("ccid = ?", "con1alice").First(&before)
	revBefore, err := srv.repos.Rev(ctx, alice.DID)
	if err != nil {
		t.Fatal(err)
	}

	// Taken by another account → 409, before any DNS lookup.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"bob.example.com"}`, "con1alice"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("taken handle status %d: %s", rec.Code, rec.Body)
	}
	// Not a domain → 400.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"alice"}`, "con1alice"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bare label status %d: %s", rec.Code, rec.Body)
	}
	// Same handle → idempotent 200, no PLC op.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"Alice.example.com"}`, "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("same handle status %d: %s", rec.Code, rec.Body)
	}
	// New domain without DNS → handle_not_resolvable, nothing moves.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"new.example.com"}`, "con1alice"))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "handle_not_resolvable") {
		t.Fatalf("expected handle_not_resolvable, got %d: %s", rec.Code, rec.Body)
	}
	// DNS pointing at someone else → did_mismatch.
	fr.table["new.example.com"] = "did:plc:someoneelse"
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"new.example.com"}`, "con1alice"))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "did_mismatch") {
		t.Fatalf("expected did_mismatch, got %d: %s", rec.Code, rec.Body)
	}
	var unchanged store.Entity
	db.Where("ccid = ?", "con1alice").First(&unchanged)
	if unchanged.Handle != "alice.example.com" || unchanged.PLCLastOpCID != before.PLCLastOpCID {
		t.Fatalf("failed attempts must not touch the row: %+v", unchanged)
	}

	evts, cancel, err := srv.repos.Events().Subscribe(ctx, "test", func(*events.XRPCStreamEvent) bool { return true }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	// Correct DNS → handle swapped on the same DID, new PLC op, #identity.
	fr.table["new.example.com"] = alice.DID
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/change-handle", `{"handle":"new.example.com"}`, "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("change-handle status %d: %s", rec.Code, rec.Body)
	}
	var resp struct {
		DID    string `json:"did"`
		Handle string `json:"handle"`
		Status string `json:"status"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.DID != alice.DID || resp.Handle != "new.example.com" || resp.Status != "active" {
		t.Fatalf("unexpected resp %+v", resp)
	}
	var after store.Entity
	db.Where("ccid = ?", "con1alice").First(&after)
	if after.DID != alice.DID || after.Status != "active" || after.Handle != "new.example.com" {
		t.Fatalf("row not updated: %+v", after)
	}
	if after.PLCLastOpCID == before.PLCLastOpCID || after.PLCLastOpCID == "" {
		t.Fatalf("expected a new PLC op, got %q (was %q)", after.PLCLastOpCID, before.PLCLastOpCID)
	}
	select {
	case evt := <-evts:
		if evt.RepoIdentity == nil || evt.RepoIdentity.Did != alice.DID || evt.RepoIdentity.Handle == nil || *evt.RepoIdentity.Handle != "new.example.com" {
			t.Fatalf("expected #identity for the new handle, got %+v", evt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no #identity emitted")
	}

	// Repo untouched: same rev, record still readable.
	revAfter, err := srv.repos.Rev(ctx, alice.DID)
	if err != nil {
		t.Fatal(err)
	}
	if revAfter != revBefore {
		t.Fatalf("repo rev changed across handle change: %s → %s", revBefore, revAfter)
	}
	raw, err := srv.repos.GetRecordJSON(ctx, alice.DID, "app.bsky.feed.post", rkey)
	if err != nil || !strings.Contains(string(raw), text) {
		t.Fatalf("record lost: %v %s", err, raw)
	}

	// The old handle is free for others; the new one is reserved.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/setup", `{"handle":"alice.example.com"}`, "con1carol"))
	if rec.Code != http.StatusOK {
		t.Fatalf("released handle should be takeable, got %d: %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/setup", `{"handle":"new.example.com"}`, "con1dave"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("new handle should be reserved, got %d: %s", rec.Code, rec.Body)
	}
}

// An account flipped back to pending by hand must not lose its repo when
// verify re-activates it.
func TestReactivateKeepsRepo(t *testing.T) {
	srv, fr, db := setupServer(t)
	e := echo.New()
	srv.Register(e)
	ctx := context.Background()

	alice := activateAccount(t, e, fr, "con1alice", "alice.example.com")
	uri, _, err := srv.repos.CreateRecord(ctx, alice.DID, "app.bsky.feed.post", &bsky.FeedPost{Text: "keep me", CreatedAt: time.Now().UTC().Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	rkey := uri[strings.LastIndex(uri, "/")+1:]
	revBefore, _ := srv.repos.Rev(ctx, alice.DID)

	if err := db.Model(&store.Entity{}).Where("ccid = ?", "con1alice").Update("status", "pending").Error; err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/verify", ``, "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("re-verify status %d: %s", rec.Code, rec.Body)
	}
	var ent store.Entity
	db.Where("ccid = ?", "con1alice").First(&ent)
	if ent.Status != "active" {
		t.Fatalf("expected active, got %q", ent.Status)
	}
	revAfter, _ := srv.repos.Rev(ctx, alice.DID)
	if revAfter != revBefore {
		t.Fatalf("re-activation rewrote the repo: %s → %s", revBefore, revAfter)
	}
	if raw, err := srv.repos.GetRecordJSON(ctx, alice.DID, "app.bsky.feed.post", rkey); err != nil || !strings.Contains(string(raw), "keep me") {
		t.Fatalf("record lost on re-activation: %v %s", err, raw)
	}
}
