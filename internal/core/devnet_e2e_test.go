package core_test

// End-to-end test against a running concrnt devnet. Skipped unless
// DEVNET_E2E=1. Exercises the full concrnt integration path this bridge
// depends on: master-key signing, commit (and its response body, which the
// upstream Go client discards), resolve, timeline distribution and delete
// via deterministic keys.
//
//	DEVNET_E2E=1 DEVNET_URL=http://localhost:8000 \
//	DEVNET_CCID=con1... DEVNET_PRIVKEY=<hex> go test ./internal/core/

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/concrnt/concrnt/client"

	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/world"
)

func TestDevnetCommitRoundtrip(t *testing.T) {
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

	atURI := fmt.Sprintf("at://did:plc:e2etest/app.bsky.feed.post/3ktest%d", time.Now().Unix())
	override := &world.ProfileOverride{Username: "E2E Tester", Link: "https://bsky.app/profile/e2e.test"}
	distributes := []string{toconcrnt.InboxTimeline(ccid)}

	doc := toconcrnt.BuildRecordDoc(ccid, "did:plc:e2etest", atURI, "bafyfake", override, distributes, time.Now().UTC())
	result, err := core.Commit(ctx, svc, doc)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if result.CCKV == nil {
		t.Errorf("commit response carried no cckv uri: %+v", result)
	}

	key := toconcrnt.InboxKey(ccid, atURI)
	got, err := core.GetDocument[world.AtprotoRecord](ctx, svc, key)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if got.Schema != world.SchemaAtprotoRecord || got.Value.AtURI != atURI {
		t.Errorf("resolved wrong document: %+v", got)
	}
	if got.Value.ProfileOverride == nil || got.Value.ProfileOverride.Username != "E2E Tester" {
		t.Errorf("profileOverride lost: %+v", got.Value)
	}

	del := toconcrnt.BuildDeleteDoc(ccid, key)
	if _, err := core.Commit(ctx, svc, del); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	// Bypass the client's 10-minute resource cache when checking deletion.
	var gone world.AtprotoRecord
	if err := svc.Client.GetRecord(ctx, key, &client.Options{NoCache: true}, &gone); err == nil {
		t.Error("document still resolvable after delete")
	}
}
