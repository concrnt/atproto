package core_test

// Devnet E2E for follow notifications: documents the dedup semantics the
// bridge relies on. A byte-identical re-commit (jetstream rewind replay) must
// not duplicate the notify-timeline entry. A refollow (same key, newer
// createdAt → new documentID) upserts the record and — since distribution
// keys are derived from the href — replaces the timeline entry in place, so
// it must not re-notify either.
//
//	DEVNET_E2E=1 DEVNET_URL=http://localhost:8000 \
//	DEVNET_CCID=con1... DEVNET_PRIVKEY=<hex> go test ./internal/core/

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/concrnt/concrnt/client"

	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/world"
)

func TestDevnetFollowNotifyDedup(t *testing.T) {
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

	ctx := context.Background()
	svc := core.NewService(ccid, url, privkey)

	// A throwaway destination instead of the real notify timeline: the real
	// one carries a restrict-readers policy, which distributions inherit as a
	// virtual parent, so an unauthenticated client (all this bridge has)
	// could not observe the entries under test.
	notify := fmt.Sprintf("cckv://%s/atproto.concrnt.world/e2e-notify-%d", ccid, time.Now().Unix())
	countEntries := func() int {
		docs, err := svc.Client.Query(ctx, ccid, client.QueryParams{
			Prefix: notify + "/",
			Schema: world.SchemaAtprotoFollowNotify,
			Limit:  100,
		})
		if err != nil {
			t.Fatalf("timeline query failed: %v", err)
		}
		return len(docs.Items)
	}
	// Distribution into the timeline is asynchronous on the server; poll
	// until the expected count appears (or times out).
	waitForEntries := func(want int, label string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		got := countEntries()
		for got != want && time.Now().Before(deadline) {
			time.Sleep(300 * time.Millisecond)
			got = countEntries()
		}
		if got != want {
			t.Errorf("%s: expected %d entries, got %d", label, want, got)
		}
	}
	baseline := countEntries()

	followerDID := fmt.Sprintf("did:plc:e2efollower%d", time.Now().Unix())
	createdAt := time.Now().UTC().Truncate(time.Second)
	override := &world.ProfileOverride{Username: "E2E Follower"}
	doc := toconcrnt.BuildFollowNotifyDoc(ccid, followerDID, ccid, override, []string{notify}, createdAt)

	if _, err := core.Commit(ctx, svc, doc); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	waitForEntries(baseline+1, "first follow")

	// Byte-identical replay (jetstream cursor rewind) → same documentID →
	// the timeline entry upserts, no duplicate.
	if _, err := core.Commit(ctx, svc, doc); err != nil {
		t.Fatalf("replay commit failed: %v", err)
	}
	time.Sleep(2 * time.Second) // give a would-be duplicate time to appear
	waitForEntries(baseline+1, "replay must not duplicate")

	// Refollow: same deterministic key, newer createdAt → record upserts and
	// the timeline entry (keyed by hash of the href) is replaced, not added.
	refollow := toconcrnt.BuildFollowNotifyDoc(ccid, followerDID, ccid, override, []string{notify}, createdAt.Add(time.Second))
	if _, err := core.Commit(ctx, svc, refollow); err != nil {
		t.Fatalf("refollow commit failed: %v", err)
	}
	time.Sleep(2 * time.Second) // give a would-be duplicate time to appear
	waitForEntries(baseline+1, "refollow must not re-notify")

	// The record itself upserted at one deterministic key.
	key := toconcrnt.FollowNotifyKey(ccid, followerDID, ccid)
	got, err := core.GetDocument[world.AtprotoFollowNotify](ctx, svc, key)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if got.Value.DID != followerDID || !strings.HasPrefix(got.Key, "cckv://"+ccid+"/"+world.FollowNotifyKeyPrefix) {
		t.Errorf("resolved wrong document: %+v", got)
	}

	// Cleanup the record (timeline残骸はdevnet使い捨てアカウントなので許容).
	if _, err := core.Commit(ctx, svc, toconcrnt.BuildDeleteDoc(ccid, key)); err != nil {
		t.Fatalf("cleanup delete failed: %v", err)
	}
}
