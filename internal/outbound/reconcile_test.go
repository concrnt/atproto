package outbound

import (
	"testing"

	"github.com/concrnt/atproto/internal/store"
)

func TestComputeFollowDiff(t *testing.T) {
	desired := map[string]desiredFollow{
		"did:plc:kept": {key: "cckv://con1u/atproto.concrnt.world/follows/a"},
		"did:plc:new":  {key: "cckv://con1u/atproto.concrnt.world/follows/b"},
	}
	current := []store.Follow{
		{CCID: "con1u", SubjectDID: "did:plc:kept", FollowRkey: "r1"},
		{CCID: "con1u", SubjectDID: "did:plc:stale", FollowRkey: "r2"},
	}

	add, remove := computeFollowDiff(desired, current)
	if len(add) != 1 || add[0] != "did:plc:new" {
		t.Fatalf("expected only did:plc:new to be added, got %v", add)
	}
	if len(remove) != 1 || remove[0].SubjectDID != "did:plc:stale" {
		t.Fatalf("expected only did:plc:stale to be removed, got %v", remove)
	}

	add, remove = computeFollowDiff(nil, nil)
	if len(add) != 0 || len(remove) != 0 {
		t.Fatalf("empty inputs must diff to nothing, got %v %v", add, remove)
	}
}
