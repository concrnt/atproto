package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

// desiredFollow is one follow declared in the user's cckv space.
type desiredFollow struct {
	key       string
	createdAt time.Time
}

// computeFollowDiff compares the cckv truth against the at_follows cache and
// returns the subject DIDs to newly mirror and the cached rows to tear down.
func computeFollowDiff(desired map[string]desiredFollow, current []store.Follow) (add []string, remove []store.Follow) {
	seen := map[string]bool{}
	for _, f := range current {
		seen[f.SubjectDID] = true
		if _, ok := desired[f.SubjectDID]; !ok {
			remove = append(remove, f)
		}
	}
	for did := range desired {
		if !seen[did] {
			add = append(add, did)
		}
	}
	return add, remove
}

// ensureFollowsReconciled runs reconcileFollows once per entity, the same way
// the ActivityPub bridge's followStore ensures each entity's follow records
// are bulk-loaded: success is remembered, failure retries on the next reload.
func (d *Daemon) ensureFollowsReconciled(ctx context.Context, entity *store.Entity) {
	d.mu.RLock()
	done := d.reconciled[entity.CCID]
	d.mu.RUnlock()
	if done {
		return
	}
	if err := d.reconcileFollows(ctx, entity); err != nil {
		slog.Warn("follow reconcile failed", "ccid", entity.CCID, "error", err)
		return
	}
	d.mu.Lock()
	d.reconciled[entity.CCID] = true
	d.mu.Unlock()
}

// reconcileFollows backfills follows committed while the bridge was down:
// the user's follow documents are the source of truth, so anything they
// declare and we don't mirror gets created, and anything we mirror that they
// no longer declare gets removed. Runs concurrently with the event path;
// both sides are idempotent per subject DID.
func (d *Daemon) reconcileFollows(ctx context.Context, entity *store.Entity) error {
	prefix := "cckv://" + entity.CCID + "/" + world.FollowsKeyPrefix
	docs, complete, err := core.QueryAllByPrefix(ctx, d.core, prefix, world.SchemaAtprotoFollow)
	if err != nil {
		return err
	}

	desired := map[string]desiredFollow{}
	for _, doc := range docs {
		if doc.Author != entity.CCID || doc.Schema != world.SchemaAtprotoFollow {
			continue
		}
		var follow world.AtprotoFollow
		if err := json.Unmarshal(doc.Value, &follow); err != nil || !strings.HasPrefix(follow.DID, "did:") {
			continue
		}
		// Multiple keys can declare the same subject (e.g. a record from the
		// raw-DID key era plus its hashed successor); the newest wins.
		if prev, ok := desired[follow.DID]; ok && !doc.CreatedAt.After(prev.createdAt) {
			continue
		}
		desired[follow.DID] = desiredFollow{key: doc.Key, createdAt: doc.CreatedAt}
	}

	var current []store.Follow
	if err := d.db.Where("ccid = ?", entity.CCID).Find(&current).Error; err != nil {
		return err
	}

	add, remove := computeFollowDiff(desired, current)
	var firstErr error
	for _, did := range add {
		f := desired[did]
		if err := d.applyFollow(ctx, entity, did, f.key, f.createdAt); err != nil {
			slog.Error("failed to backfill follow", "ccid", entity.CCID, "did", did, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// An incomplete listing can only prove presence, never absence — an
	// unfollow sweep over it would tear down valid follows en masse.
	if !complete {
		slog.Warn("follow listing incomplete; skipping unfollow sweep", "ccid", entity.CCID)
		return errIncompleteListing
	}
	for _, f := range remove {
		d.removeFollow(ctx, entity, f)
	}
	return firstErr
}

var errIncompleteListing = errors.New("follow listing incomplete")
