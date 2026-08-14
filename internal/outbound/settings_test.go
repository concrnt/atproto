package outbound

// Settings live in the user's own cckv space, so the store must only trust a
// record the user themselves authored under the settings schema; anything
// else degrades to the defaults (no extra timelines, enabled).

import (
	"context"
	"encoding/json"
	"testing"

	concrnt "github.com/concrnt/concrnt"

	"github.com/concrnt/atproto/internal/world"
)

func boolPtr(b bool) *bool { return &b }

func TestSanitizeSettings(t *testing.T) {
	ccid := "con1user"
	base := concrnt.Document[world.AtprotoSettings]{
		Author: ccid,
		Schema: world.SchemaAtprotoSettings,
		Value: world.AtprotoSettings{
			ListenTimelines: []string{"cckv://con1user/foo", "  ", "cckv://con1user/bar"},
			Enabled:         boolPtr(false),
		},
	}

	v := sanitizeSettings(&base, ccid)
	if len(v.listenTimelines) != 2 || v.listenTimelines[0] != "cckv://con1user/foo" {
		t.Fatalf("expected blank timelines dropped, got %v", v.listenTimelines)
	}
	if v.enabled {
		t.Fatal("expected enabled=false to be honored")
	}

	spoofed := base
	spoofed.Author = "con1attacker"
	if v := sanitizeSettings(&spoofed, ccid); len(v.listenTimelines) != 0 || !v.enabled {
		t.Fatal("foreign-author record must degrade to defaults")
	}

	wrongSchema := base
	wrongSchema.Schema = world.SchemaAtprotoFollow
	if v := sanitizeSettings(&wrongSchema, ccid); len(v.listenTimelines) != 0 || !v.enabled {
		t.Fatal("wrong-schema record must degrade to defaults")
	}

	if v := sanitizeSettings(nil, ccid); len(v.listenTimelines) != 0 || !v.enabled {
		t.Fatal("missing record must mean defaults")
	}

	noEnabled := base
	noEnabled.Value.Enabled = nil
	if v := sanitizeSettings(&noEnabled, ccid); !v.enabled {
		t.Fatal("nil enabled must mean enabled")
	}
}

func TestSettingsStoreApplyEvent(t *testing.T) {
	d := newGateTestDaemon(t)
	s := d.settings
	ctx := context.Background()
	ccid := "con1user"
	key := settingsKey(ccid)

	// Unloaded ccid: defaults.
	if tl, enabled := s.Get(ccid); len(tl) != 0 || !enabled {
		t.Fatal("unloaded ccid must return defaults")
	}

	doc := concrnt.Document[world.AtprotoSettings]{
		Key:    key,
		Author: ccid,
		Schema: world.SchemaAtprotoSettings,
		Value: world.AtprotoSettings{
			ListenTimelines: []string{"cckv://con1user/some-timeline"},
			Enabled:         boolPtr(false),
		},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	s.ApplyEvent(ctx, ccid, concrnt.Event{
		Type: "created",
		References: map[string]concrnt.SignedDocument{
			key: {Document: string(b)},
		},
	})
	tl, enabled := s.Get(ccid)
	if len(tl) != 1 || tl[0] != "cckv://con1user/some-timeline" || enabled {
		t.Fatalf("event-embedded record not applied: %v %v", tl, enabled)
	}

	s.ApplyEvent(ctx, ccid, concrnt.Event{Type: "deleted"})
	if tl, enabled := s.Get(ccid); len(tl) != 0 || !enabled {
		t.Fatal("deleted settings record must restore defaults")
	}
}
