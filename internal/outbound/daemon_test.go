package outbound

// The Redis event feed is the server-internal view: it embeds documents an
// anonymous reader may not see, flagged per SignedDocument via isPublic.
// These tests pin the fail-closed gate on the embedded-document fast path.
// The core service points at an unreachable host so a fallback fetch surfaces
// as a connection error, distinguishable from errNotPublic.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	concrnt "github.com/concrnt/concrnt"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

func newGateTestDaemon(t *testing.T) *Daemon {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "gate.db")), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := store.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	// Unreachable API base: well-known resolution fails with a warning and
	// any fetch fails fast with a connection error.
	svc := core.NewService("con1service", "http://127.0.0.1:1", strings.Repeat("11", 32))
	return NewDaemon(db, svc, nil, nil, "", time.Hour)
}

func mustMarshalDoc(t *testing.T, doc Doc) string {
	t.Helper()
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestResolveEventDocumentPublicGate(t *testing.T) {
	d := newGateTestDaemon(t)
	ctx := context.Background()

	uri := "cckv://con1user/e2e/post1"
	doc := Doc{Kind: "record", Key: uri, Schema: world.SchemaMarkdown, Author: "con1user"}
	yes, no := true, false

	// Embedded and marked public → returned without any fetch.
	got, err := d.resolveEventDocument(ctx, concrnt.Event{
		References: map[string]concrnt.SignedDocument{
			uri: {Document: mustMarshalDoc(t, doc), IsPublic: &yes},
		},
	}, uri)
	if err != nil || got.Key != uri {
		t.Fatalf("public document should resolve, got %+v, %v", got, err)
	}

	// Marked non-public or unevaluated → errNotPublic, with no fallback
	// fetch (a fetch against the unreachable host would be a different
	// error).
	for name, flag := range map[string]*bool{"false": &no, "nil": nil} {
		_, err := d.resolveEventDocument(ctx, concrnt.Event{
			References: map[string]concrnt.SignedDocument{
				uri: {Document: mustMarshalDoc(t, doc), IsPublic: flag},
			},
		}, uri)
		if !errors.Is(err, errNotPublic) {
			t.Errorf("isPublic=%s: expected errNotPublic, got %v", name, err)
		}
	}

	// Not embedded at all → the anonymous fetch is attempted (and fails
	// against the unreachable host with something other than errNotPublic).
	_, err = d.resolveEventDocument(ctx, concrnt.Event{}, uri)
	if err == nil || errors.Is(err, errNotPublic) {
		t.Errorf("expected a fetch error for a non-embedded document, got %v", err)
	}
}

func TestHandleTimelineEventNonPublicNested(t *testing.T) {
	d := newGateTestDaemon(t)
	ctx := context.Background()

	entity := &store.Entity{CCID: "con1user", DID: "did:plc:gatetest", Enabled: true}
	inner := "cckv://con1user/e2e/private-post"
	channel := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/home-timeline/x0123", entity.CCID)
	yes, no := true, false

	refDoc := Doc{
		Kind:   "record",
		Key:    channel,
		Schema: world.SchemaReference,
		Author: entity.CCID,
		Value:  json.RawMessage(fmt.Sprintf(`{"href":%q}`, inner)),
	}
	innerDoc := Doc{Kind: "record", Key: inner, Schema: world.SchemaMarkdown, Author: entity.CCID}

	// A public timeline reference whose distributed original is non-public
	// must not be mirrored.
	d.handleTimelineEvent(ctx, entity, channel, concrnt.Event{
		Type: "created",
		References: map[string]concrnt.SignedDocument{
			channel: {
				Document: mustMarshalDoc(t, refDoc),
				IsPublic: &yes,
				References: map[string]concrnt.SignedDocument{
					inner: {Document: mustMarshalDoc(t, innerDoc), IsPublic: &no},
				},
			},
		},
	})

	var count int64
	d.db.Model(&store.URIMap{}).Count(&count)
	if count != 0 {
		t.Errorf("non-public nested document was mirrored (%d URIMap rows)", count)
	}
}
