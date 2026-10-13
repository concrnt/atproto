package outbound_test

// End-to-end test of the inbox existence cache against a running concrnt
// devnet (same environment contract as devnet_settings_e2e_test.go). The
// devnet account's own inbox timeline is deleted and recreated here, so use
// a throwaway account.

import (
	"context"
	"fmt"
	"testing"
	"time"

	concrnt "github.com/concrnt/concrnt"

	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/outbound"
	"github.com/concrnt/atproto/internal/world"
)

const schemaUserTimeline = "https://schema.concrnt.world/t/user.json"

func (h *devnetHarness) commitInbox(t *testing.T, ctx context.Context) {
	t.Helper()
	doc := concrnt.Document[map[string]any]{
		Kind:      "record",
		Key:       toconcrnt.InboxTimeline(h.ccid),
		Schema:    schemaUserTimeline,
		Value:     map[string]any{},
		Author:    h.ccid,
		CreatedAt: time.Now().UTC(),
	}
	if _, err := core.Commit(ctx, h.core, doc); err != nil {
		t.Fatalf("inbox commit failed: %v", err)
	}
}

func waitHas(store *outbound.InboxStore, ccid string, want bool) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if store.Has(ccid) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// TestDevnetInboxStore pins the cache against the real event stream: the
// inbox record's own create/delete flips it, while a mirrored post removed
// from the inbox (a deleted event on the same channel) leaves it alone.
func TestDevnetInboxStore(t *testing.T) {
	h := newDevnetHarness(t, "did:plc:inboxe2etest")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inbox := toconcrnt.InboxTimeline(h.ccid)
	// Start from a clean slate whatever a previous run left behind.
	h.deleteRecord(t, ctx, inbox)
	time.Sleep(500 * time.Millisecond)

	daemon := h.startDaemon(t, ctx)
	inboxes := daemon.Inboxes()
	time.Sleep(500 * time.Millisecond)

	if err := inboxes.EnsureLoaded(ctx, h.ccid); err != nil {
		t.Fatalf("EnsureLoaded (absent) failed: %v", err)
	}
	if inboxes.Has(h.ccid) {
		t.Fatal("inbox should be absent before it is created")
	}

	h.commitInbox(t, ctx)
	defer h.deleteRecord(t, context.Background(), inbox)
	if !waitHas(inboxes, h.ccid, true) {
		t.Fatal("created event for the inbox record did not mark it present")
	}

	// A fresh store sees the record on the network path too.
	fresh := outbound.NewInboxStore(h.core)
	if err := fresh.EnsureLoaded(ctx, h.ccid); err != nil {
		t.Fatalf("EnsureLoaded (present) failed: %v", err)
	}
	if !fresh.Has(h.ccid) {
		t.Fatal("fresh store must find the existing inbox")
	}

	// A post distributed into the inbox and then deleted fires deleted on the
	// inbox channel with the post's own key; the inbox must stay present.
	child := fmt.Sprintf("cckv://%s/e2e-inbox-child/%d", h.ccid, time.Now().UnixNano())
	doc := concrnt.Document[world.MessageValue]{
		Kind: "record", Key: child, Schema: world.SchemaMarkdown,
		Value: world.MessageValue{Body: "inbox e2e child"}, Author: h.ccid,
		CreatedAt: time.Now().UTC(), Distributes: &[]string{inbox},
	}
	if _, err := core.Commit(ctx, h.core, doc); err != nil {
		t.Fatalf("child post commit failed: %v", err)
	}
	time.Sleep(time.Second)
	h.deleteRecord(t, ctx, child)
	time.Sleep(2 * time.Second)
	if !inboxes.Has(h.ccid) {
		t.Fatal("deleted event for a child post cleared the inbox")
	}

	h.deleteRecord(t, ctx, inbox)
	if !waitHas(inboxes, h.ccid, false) {
		t.Fatal("deleted event for the inbox record did not mark it absent")
	}
}
