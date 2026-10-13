package toconcrnt

import (
	"encoding/json"
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

func TestParsePostRefsSeparatesParentRootQuote(t *testing.T) {
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
	refs := ParsePostRefs(record)
	if refs.Parent != "at://did:plc:a/app.bsky.feed.post/1" || refs.Root != "at://did:plc:b/app.bsky.feed.post/2" || refs.Quote != "at://did:plc:c/app.bsky.feed.post/3" {
		t.Fatalf("got %+v", refs)
	}
	if plain := ParsePostRefs(map[string]any{"text": "hi"}); plain != (PostRefSet{}) {
		t.Fatalf("plain post must have no refs: %+v", plain)
	}
}

// Notification associations must be shaped like the ActivityPub bridge's:
// no key, no variant, associate = the bridged user's resource, and a value
// restricted to what the a/reply.json / a/reroute.json schemas allow.
func TestNotificationAssociationShapes(t *testing.T) {
	now := time.Now()
	override := &world.ProfileOverride{Username: "alice", Link: "https://bsky.app/profile/alice.test"}
	notify := []string{"cckv://con1u/concrnt.world/profiles/main/notify-timeline"}

	reply := BuildReplyAssociation("con1svc", "cckv://con1u/concrnt.world/profiles/main/posts/p1", "cckv://con1svc/atproto.concrnt.world/inbox/x", override, notify, now)
	if reply.Kind != "association" || reply.Schema != world.SchemaReplyAssociation || reply.Key != "" {
		t.Errorf("bad reply doc %+v", reply)
	}
	if reply.Associate == nil || *reply.Associate != "cckv://con1u/concrnt.world/profiles/main/posts/p1" {
		t.Errorf("bad associate %+v", reply.Associate)
	}
	if reply.AssociationVariant != nil {
		t.Errorf("reply must carry no variant")
	}
	if reply.Value.TargetURI != "cckv://con1svc/atproto.concrnt.world/inbox/x" || reply.Value.ProfileOverride != override {
		t.Errorf("bad value %+v", reply.Value)
	}
	if reply.Distributes == nil || len(*reply.Distributes) != 1 || (*reply.Distributes)[0] != notify[0] {
		t.Errorf("bad distributes %+v", reply.Distributes)
	}
	var raw map[string]any
	b, _ := json.Marshal(reply)
	_ = json.Unmarshal(b, &raw)
	if _, has := raw["key"]; !has {
		// Document always serializes key; the server requires it empty
		t.Errorf("unexpected serialization: %s", b)
	}
	value := raw["value"].(map[string]any)
	for k := range value {
		if k != "targetURI" && k != "profileOverride" {
			t.Errorf("value carries field %q not allowed by a/reply.json: %s", k, b)
		}
	}

	reroute := BuildRerouteAssociation("con1svc", "cckv://con1u/concrnt.world/profiles/main/posts/p1", "cckv://con1svc/atproto.concrnt.world/inbox/y", nil, notify, now)
	if reroute.Schema != world.SchemaRerouteAssociation || reroute.Kind != "association" {
		t.Errorf("bad reroute doc %+v", reroute)
	}
	if reroute.Value.ProfileOverride != nil {
		t.Errorf("nil override must stay nil")
	}

	follow := BuildFollowNotifyAssociation("con1svc", "did:plc:follower", MainProfile("con1u"), override, notify, now)
	if follow.Kind != "association" || follow.Schema != world.SchemaAtprotoFollowNotify || follow.Key != "" {
		t.Errorf("bad follow doc %+v", follow)
	}
	if follow.Associate == nil || *follow.Associate != "cckv://con1u/concrnt.world/profiles/main" {
		t.Errorf("bad associate %+v", follow.Associate)
	}
	if follow.Value.DID != "did:plc:follower" || follow.Value.ProfileOverride != override {
		t.Errorf("bad value %+v", follow.Value)
	}
	if follow.Author != "con1svc" || follow.Distributes == nil || len(*follow.Distributes) != 1 {
		t.Errorf("bad author/distributes %+v", follow)
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
