package core_test

// Pins QueryAllByPrefix's cursor handling against a running devnet (same
// DEVNET_* environment as devnet_e2e_test.go). The server's cursor is a
// string (RFC3339Nano createdAt) that is parsed back into Since; a listing
// larger than one page must come back whole, and a page full of rows sharing
// one createdAt must be reported as incomplete rather than looping.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	concrnt "github.com/concrnt/concrnt"

	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/world"
)

func seedFollows(t *testing.T, ctx context.Context, svc *core.Service, ccid, prefix string, n int, createdAt func(i int) time.Time) []string {
	t.Helper()
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s%03d", prefix, i)
		doc := concrnt.Document[world.AtprotoFollow]{
			Kind: "record", Key: key, Schema: world.SchemaAtprotoFollow,
			Value:  world.AtprotoFollow{DID: fmt.Sprintf("did:plc:paging%03d", i)},
			Author: ccid, CreatedAt: createdAt(i),
		}
		if _, err := core.Commit(ctx, svc, doc); err != nil {
			t.Fatalf("seed commit %s failed: %v", key, err)
		}
		keys = append(keys, key)
	}
	t.Cleanup(func() {
		for _, key := range keys {
			del := concrnt.Document[string]{Kind: "delete", Schema: world.SchemaDelete, Value: key, Author: ccid, CreatedAt: time.Now().UTC()}
			if _, err := core.Commit(context.Background(), svc, del); err != nil {
				t.Logf("cleanup delete of %s failed: %v", key, err)
			}
		}
	})
	return keys
}

func TestDevnetQueryPaging(t *testing.T) {
	if os.Getenv("DEVNET_E2E") != "1" {
		t.Skip("set DEVNET_E2E=1 to run against a devnet")
	}
	ccid, privkey, url := os.Getenv("DEVNET_CCID"), os.Getenv("DEVNET_PRIVKEY"), os.Getenv("DEVNET_URL")
	if url == "" {
		url = "http://localhost:8000"
	}
	if ccid == "" || privkey == "" {
		t.Fatal("DEVNET_CCID and DEVNET_PRIVKEY are required")
	}
	ctx := context.Background()
	svc := core.NewService(ccid, url, privkey)
	base := time.Now().UTC().Add(-time.Hour)

	t.Run("multi-page listing is complete", func(t *testing.T) {
		prefix := fmt.Sprintf("cckv://%s/e2e-paging/%d/", ccid, time.Now().UnixNano())
		const n = 120 // > one page of 100
		seedFollows(t, ctx, svc, ccid, prefix, n, func(i int) time.Time { return base.Add(time.Duration(i) * time.Millisecond) })

		docs, complete, err := core.QueryAllByPrefix(ctx, svc, prefix, world.SchemaAtprotoFollow)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if !complete {
			t.Fatal("listing with distinct createdAt must be complete")
		}
		if len(docs) != n {
			t.Fatalf("expected %d docs across pages, got %d", n, len(docs))
		}
	})

	t.Run("stalled cursor is reported incomplete", func(t *testing.T) {
		prefix := fmt.Sprintf("cckv://%s/e2e-stall/%d/", ccid, time.Now().UnixNano())
		same := base.Add(30 * time.Minute)
		seedFollows(t, ctx, svc, ccid, prefix, 101, func(int) time.Time { return same })

		docs, complete, err := core.QueryAllByPrefix(ctx, svc, prefix, world.SchemaAtprotoFollow)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if complete {
			t.Fatal("a full page sharing one createdAt cannot advance the cursor; must be incomplete")
		}
		if len(docs) == 0 {
			t.Fatal("stalled listing should still return the first page")
		}
	})
}
