package outbound

// The inbox channel carries events for the mirrored posts distributed into
// it as well as for the inbox record itself; only the latter may flip the
// cached existence.

import (
	"testing"

	concrnt "github.com/concrnt/concrnt"

	"github.com/concrnt/atproto/internal/convert/toconcrnt"
)

func TestInboxStoreApplyEvent(t *testing.T) {
	const ccid = "con1user"
	inbox := toconcrnt.InboxTimeline(ccid)
	s := NewInboxStore(nil)

	if s.Has(ccid) {
		t.Fatal("unloaded entry must read as absent")
	}

	s.ApplyEvent(ccid, concrnt.Event{Type: "created", URI: inbox})
	if !s.Has(ccid) {
		t.Fatal("created event on the inbox key must mark it present")
	}

	// A mirrored post removed from the inbox is not the inbox going away.
	s.ApplyEvent(ccid, concrnt.Event{Type: "deleted", URI: "cckv://con1service/atproto.concrnt.world/inbox/xhash"})
	if !s.Has(ccid) {
		t.Fatal("deleted event for a child record must not clear the inbox")
	}
	s.ApplyEvent(ccid, concrnt.Event{Type: "created", URI: "cckv://con1service/atproto.concrnt.world/inbox/xhash"})

	s.ApplyEvent(ccid, concrnt.Event{Type: "deleted", URI: inbox})
	if s.Has(ccid) {
		t.Fatal("deleted event on the inbox key must mark it absent")
	}

	// An event for another user's inbox is not ours.
	s.ApplyEvent(ccid, concrnt.Event{Type: "created", URI: toconcrnt.InboxTimeline("con1other")})
	if s.Has(ccid) {
		t.Fatal("another user's inbox event must be ignored")
	}
}
