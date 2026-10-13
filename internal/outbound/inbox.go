package outbound

// InboxStore caches whether each bridged user's inbox timeline
// (cckv://<ccid>/atproto.concrnt.world/inbox) exists. The user creates it
// from the app with a policy that lets the service account write to it; a
// user who enabled the bridge but never visited that screen (or deleted the
// record) has none, and a mirrored post distributed there is refused by
// policy and lands in the core's dead-letter queue. Delivery consults this
// store to drop such recipients up front. Same shape as SettingsStore: one
// fetch per ccid, then kept current from the Redis event stream.

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	concrnt "github.com/concrnt/concrnt"

	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
)

type InboxStore struct {
	core *core.Service

	mu     sync.RWMutex
	exists map[string]bool
	loaded map[string]bool
}

func NewInboxStore(c *core.Service) *InboxStore {
	return &InboxStore{
		core:   c,
		exists: map[string]bool{},
		loaded: map[string]bool{},
	}
}

// Has reports whether the user's inbox timeline is known to exist. An
// unloaded entry reads as absent: delivery must never guess.
func (s *InboxStore) Has(ccid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exists[ccid]
}

// EnsureLoaded resolves the inbox record once per ccid. A 404 is cached as
// absent; any other error leaves the entry unloaded so the next sync (or the
// next delivery) retries.
func (s *InboxStore) EnsureLoaded(ctx context.Context, ccid string) error {
	s.mu.RLock()
	done := s.loaded[ccid]
	s.mu.RUnlock()
	if done {
		return nil
	}

	_, err := core.GetDocument[json.RawMessage](ctx, s.core, toconcrnt.InboxTimeline(ccid))
	if err != nil {
		if !core.IsNotFound(err) {
			return err
		}
		slog.Info("inbox timeline missing; mirrored posts will be skipped until it is created", "ccid", ccid)
		s.set(ccid, false)
		return nil
	}
	s.set(ccid, true)
	return nil
}

// ApplyEvent follows a create/delete of the inbox record itself. The inbox
// channel also carries created/deleted events for the mirrored posts
// distributed into it (their uri is the post key), so only an event whose
// uri is the inbox key counts — otherwise one deleted post would mark the
// whole inbox as gone.
func (s *InboxStore) ApplyEvent(ccid string, ev concrnt.Event) {
	if ev.URI != toconcrnt.InboxTimeline(ccid) {
		return
	}
	switch ev.Type {
	case "created":
		slog.Info("inbox timeline created", "ccid", ccid)
		s.set(ccid, true)
	case "deleted":
		slog.Info("inbox timeline deleted", "ccid", ccid)
		s.set(ccid, false)
	}
}

func (s *InboxStore) set(ccid string, exists bool) {
	s.mu.Lock()
	s.exists[ccid] = exists
	s.loaded[ccid] = true
	s.mu.Unlock()
}
