package repoman

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
)

func activate(t *testing.T, m *Manager, ccid, handle string) string {
	t.Helper()
	ctx := context.Background()
	ent, err := m.CreatePending(ctx, ccid, handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Activate(ctx, ent.DID); err != nil {
		t.Fatal(err)
	}
	return ent.DID
}

func TestPutRecordReplacesExistingRecord(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManager(t)
	did := activate(t, m, "con1put", "put.test.example")

	evts, cancel, err := m.Events().Subscribe(ctx, "test", func(*events.XRPCStreamEvent) bool { return true }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	// InitNewActor already wrote app.bsky.actor.profile/self, so this is
	// the "value already set" path that broke profile re-sync.
	for _, name := range []string{"first", "second"} {
		p := &appbsky.ActorProfile{LexiconTypeID: "app.bsky.actor.profile", DisplayName: strPtr(name)}
		if _, _, err := m.PutRecord(ctx, did, "app.bsky.actor.profile", "self", p); err != nil {
			t.Fatalf("PutRecord(%s): %v", name, err)
		}
		raw, err := m.GetRecordJSON(ctx, did, "app.bsky.actor.profile", "self")
		if err != nil {
			t.Fatal(err)
		}
		var got appbsky.ActorProfile
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.DisplayName == nil || *got.DisplayName != name {
			t.Fatalf("profile after put %s: %s", name, raw)
		}
	}

	// Each replacement is one #commit whose ops read delete then update.
	deadline := time.After(5 * time.Second)
	commits := 0
	for commits < 2 {
		select {
		case evt := <-evts:
			if evt.RepoCommit == nil {
				continue
			}
			commits++
			ops := evt.RepoCommit.Ops
			if len(ops) != 2 || ops[0].Action != "delete" || ops[1].Action != "update" || ops[1].Path != "app.bsky.actor.profile/self" {
				t.Fatalf("unexpected ops %+v", ops)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for commits, got %d", commits)
		}
	}
}

func TestAnnounceActiveAccounts(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManager(t)
	activeDID := activate(t, m, "con1active", "active.test.example")
	if _, err := m.CreatePending(ctx, "con1pending", "pending.test.example"); err != nil {
		t.Fatal(err)
	}

	evts, cancel, err := m.Events().Subscribe(ctx, "test", func(*events.XRPCStreamEvent) bool { return true }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	n, err := m.AnnounceActiveAccounts(ctx)
	if err != nil || n != 1 {
		t.Fatalf("announce: n=%d err=%v", n, err)
	}
	var kinds []string
	deadline := time.After(5 * time.Second)
	for len(kinds) < 2 {
		select {
		case evt := <-evts:
			switch {
			case evt.RepoIdentity != nil:
				if evt.RepoIdentity.Did != activeDID || evt.RepoIdentity.Handle == nil || *evt.RepoIdentity.Handle != "active.test.example" {
					t.Fatalf("unexpected identity %+v", evt.RepoIdentity)
				}
				kinds = append(kinds, "identity")
			case evt.RepoAccount != nil:
				if evt.RepoAccount.Did != activeDID || !evt.RepoAccount.Active {
					t.Fatalf("unexpected account %+v", evt.RepoAccount)
				}
				kinds = append(kinds, "account")
			}
		case <-deadline:
			t.Fatalf("timed out, got %v", kinds)
		}
	}
	if kinds[0] != "identity" || kinds[1] != "account" {
		t.Fatalf("order %v", kinds)
	}

	// Throttled: a second cursor-less consumer within the interval gets nothing.
	if n, err := m.AnnounceActiveAccounts(ctx); err != nil || n != 0 {
		t.Fatalf("expected throttled announce, n=%d err=%v", n, err)
	}

	// Pending accounts are never announced.
	if err := m.AnnounceAccount(ctx, activeDID); err != nil {
		t.Fatal(err)
	}
	var pending string
	m.db.Raw("SELECT did FROM at_entities WHERE ccid = ?", "con1pending").Scan(&pending)
	if err := m.AnnounceAccount(ctx, pending); err == nil {
		t.Fatal("announcing a pending account should fail")
	}
}

func strPtr(s string) *string { return &s }

// Every commit after the first must carry prevData = the previous commit's
// MST root; strict relays drop commits without it once they know the repo.
func TestCommitEventsCarryPrevData(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManager(t)

	evts, cancel, err := m.Events().Subscribe(ctx, "test", func(*events.XRPCStreamEvent) bool { return true }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	did := activate(t, m, "con1prev", "prev.test.example")
	for i := 0; i < 2; i++ {
		post := &appbsky.FeedPost{LexiconTypeID: "app.bsky.feed.post", Text: "p", CreatedAt: "2026-07-20T00:00:00.000Z"}
		if _, _, err := m.CreateRecord(ctx, did, "app.bsky.feed.post", post); err != nil {
			t.Fatal(err)
		}
	}
	p := &appbsky.ActorProfile{LexiconTypeID: "app.bsky.actor.profile", DisplayName: strPtr("x")}
	if _, _, err := m.PutRecord(ctx, did, "app.bsky.actor.profile", "self", p); err != nil {
		t.Fatal(err)
	}

	// init commit + 2 posts + profile replacement = 4 commits
	var prevDataCID string
	seen := 0
	deadline := time.After(5 * time.Second)
	for seen < 4 {
		select {
		case evt := <-evts:
			if evt.RepoCommit == nil {
				continue
			}
			c := evt.RepoCommit
			if seen == 0 {
				if c.PrevData != nil {
					t.Fatalf("first commit should not carry prevData, got %s", c.PrevData)
				}
			} else {
				if c.PrevData == nil {
					t.Fatalf("commit %d (rev %s) lacks prevData", seen, c.Rev)
				}
				if c.PrevData.String() != prevDataCID {
					t.Fatalf("commit %d prevData %s, want previous data root %s", seen, c.PrevData, prevDataCID)
				}
			}
			r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(c.Blocks))
			if err != nil {
				t.Fatalf("commit %d blocks: %v", seen, err)
			}
			prevDataCID = r.DataCid().String()
			seen++
		case <-deadline:
			t.Fatalf("timed out, saw %d commits", seen)
		}
	}
}
