package toconcrnt

import (
	"strings"
	"testing"
	"time"

	"github.com/concrnt/atproto/internal/world"
)

// The expected hash was produced by the TypeScript implementation
// (@concrnt/client CDID.makeHash) — the two sides must derive identical keys
// or deletes will orphan mirrored records.
func TestInboxKeyMatchesTSImplementation(t *testing.T) {
	got := InboxKey("con1svc", "at://did:plc:abc123/app.bsky.feed.post/3k2yihcrl6a2c")
	want := "cckv://con1svc/atproto.concrnt.world/inbox/xffpn7fg5rm89uhd0rpjt42fh"
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestPostRefs(t *testing.T) {
	record := map[string]any{
		"reply": map[string]any{
			"parent": map[string]any{"uri": "at://did:plc:a/app.bsky.feed.post/1"},
			"root":   map[string]any{"uri": "at://did:plc:b/app.bsky.feed.post/2"},
		},
		"embed": map[string]any{
			"$type":  "app.bsky.embed.record",
			"record": map[string]any{"uri": "at://did:plc:c/app.bsky.feed.post/3"},
		},
	}
	refs := PostRefs(record)
	if len(refs) != 3 {
		t.Fatalf("got %v", refs)
	}
}

func TestPostRefsRecordWithMedia(t *testing.T) {
	record := map[string]any{
		"embed": map[string]any{
			"$type": "app.bsky.embed.recordWithMedia",
			"record": map[string]any{
				"record": map[string]any{"uri": "at://did:plc:q/app.bsky.feed.post/9"},
			},
		},
	}
	refs := PostRefs(record)
	if len(refs) != 1 || refs[0] != "at://did:plc:q/app.bsky.feed.post/9" {
		t.Fatalf("got %v", refs)
	}
}

func TestSubjectExtraction(t *testing.T) {
	record := map[string]any{
		"subject": map[string]any{"uri": "at://did:plc:x/app.bsky.feed.post/5", "cid": "bafyfoo"},
	}
	if SubjectURI(record) != "at://did:plc:x/app.bsky.feed.post/5" || SubjectCID(record) != "bafyfoo" {
		t.Fatal("subject extraction failed")
	}
}

func TestFollowNotifyKeyDeterminism(t *testing.T) {
	a := FollowNotifyKey("con1svc", "did:plc:follower", "did:plc:subject")
	b := FollowNotifyKey("con1svc", "did:plc:follower", "did:plc:subject")
	if a != b {
		t.Errorf("same pair must derive the same key: %s vs %s", a, b)
	}
	if !strings.HasPrefix(a, "cckv://con1svc/atproto.concrnt.world/follow-notify/") {
		t.Errorf("unexpected key namespace: %s", a)
	}
	if c := FollowNotifyKey("con1svc", "did:plc:other", "did:plc:subject"); c == a {
		t.Errorf("different follower must derive a different key")
	}
	if c := FollowNotifyKey("con1svc", "did:plc:follower", "did:plc:other"); c == a {
		t.Errorf("different subject must derive a different key")
	}
}

func TestBuildFollowNotifyDoc(t *testing.T) {
	now := time.Now()
	override := &world.ProfileOverride{Username: "alice"}
	doc := BuildFollowNotifyDoc("con1svc", "did:plc:follower", "did:plc:subject", override,
		[]string{"cckv://con1u/concrnt.world/profiles/main/notify-timeline"}, now)
	if doc.Kind != "record" || doc.Schema != world.SchemaAtprotoFollowNotify {
		t.Errorf("bad doc %+v", doc)
	}
	if doc.Key != FollowNotifyKey("con1svc", "did:plc:follower", "did:plc:subject") {
		t.Errorf("key mismatch: %s", doc.Key)
	}
	if doc.Value.DID != "did:plc:follower" || doc.Value.ProfileOverride != override {
		t.Errorf("bad value %+v", doc.Value)
	}
	if doc.Author != "con1svc" || doc.Distributes == nil || len(*doc.Distributes) != 1 {
		t.Errorf("bad author/distributes %+v", doc)
	}
}

func TestDocShapes(t *testing.T) {
	now := time.Now()
	doc := BuildRecordDoc("con1svc", "did:plc:a", "at://did:plc:a/app.bsky.feed.post/1", "bafy", nil,
		[]string{"cckv://con1u/atproto.concrnt.world/inbox"}, now)
	if doc.Kind != "record" || doc.Schema != "https://schema.concrnt.world/atproto/record.json" {
		t.Errorf("bad doc %+v", doc)
	}
	if doc.Distributes == nil || len(*doc.Distributes) != 1 {
		t.Error("distributes missing")
	}

	del := BuildDeleteDoc("con1svc", "cckv://con1svc/atproto.concrnt.world/inbox/abc")
	if del.Kind != "delete" || del.Value != "cckv://con1svc/atproto.concrnt.world/inbox/abc" {
		t.Errorf("bad delete doc %+v", del)
	}

	like := BuildLikeAssociation("con1svc", "cckv://target", nil, nil, now)
	if like.Kind != "association" || like.Associate == nil || *like.Associate != "cckv://target" {
		t.Errorf("bad like doc %+v", like)
	}
}
