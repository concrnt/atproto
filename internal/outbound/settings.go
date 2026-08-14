package outbound

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"

	concrnt "github.com/concrnt/concrnt"

	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/world"
)

// settingsKey is the user-owned record holding per-user bridge settings,
// mirroring the ActivityPub bridge's activitypub.concrnt.world/settings.
func settingsKey(ccid string) string {
	return "cckv://" + ccid + "/" + world.SettingsKeySuffix
}

// settingsValue is a sanitized settings record. The zero value is NOT the
// default — absence of a record means defaultSettings().
type settingsValue struct {
	listenTimelines []string
	enabled         bool
}

func defaultSettings() settingsValue {
	return settingsValue{enabled: true}
}

// sanitizeSettings validates a settings document against its owner: only a
// record authored by the user themselves under the right schema counts;
// anything else degrades to the defaults. A nil enabled means enabled.
func sanitizeSettings(doc *concrnt.Document[world.AtprotoSettings], ccid string) settingsValue {
	v := defaultSettings()
	if doc == nil || doc.Author != ccid || doc.Schema != world.SchemaAtprotoSettings {
		return v
	}
	for _, tl := range doc.Value.ListenTimelines {
		if strings.TrimSpace(tl) != "" {
			v.listenTimelines = append(v.listenTimelines, tl)
		}
	}
	if doc.Value.Enabled != nil {
		v.enabled = *doc.Value.Enabled
	}
	return v
}

// SettingsStore mirrors each bridged user's settings record. It is the Go
// port of the ActivityPub bridge's settingsStore.ts: the records live in each
// user's own cckv space, so there is no prefix to bulk-query — they are
// fetched per entity and kept fresh via Redis events.
type SettingsStore struct {
	core *core.Service

	mu     sync.RWMutex
	values map[string]settingsValue
	loaded map[string]bool
}

func NewSettingsStore(c *core.Service) *SettingsStore {
	return &SettingsStore{
		core:   c,
		values: map[string]settingsValue{},
		loaded: map[string]bool{},
	}
}

// Get returns the user's listen timelines and enabled flag. Before the record
// has been loaded it returns the defaults (no extra timelines, enabled).
func (s *SettingsStore) Get(ccid string) (timelines []string, enabled bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.values[ccid]
	if !ok {
		v = defaultSettings()
	}
	return v.listenTimelines, v.enabled
}

// EnsureLoaded fetches the settings record once per ccid; loaded entries are
// a no-op, and errors leave the entry unloaded so the periodic reload retries.
func (s *SettingsStore) EnsureLoaded(ctx context.Context, ccid string) error {
	s.mu.RLock()
	done := s.loaded[ccid]
	s.mu.RUnlock()
	if done {
		return nil
	}

	doc, err := core.GetDocument[world.AtprotoSettings](ctx, s.core, settingsKey(ccid))
	if err != nil {
		if !core.IsNotFound(err) {
			return err
		}
		doc = nil // no record = defaults
	}
	s.set(ccid, sanitizeSettings(doc, ccid))
	return nil
}

// ApplyEvent applies a realtime create/delete of the settings record,
// preferring the signed copy embedded in the event over a fetch. The record
// only steers the bridge's behavior for its own author, so a non-public
// record is honored as long as the event carries it.
func (s *SettingsStore) ApplyEvent(ctx context.Context, ccid string, ev concrnt.Event) {
	key := settingsKey(ccid)
	switch ev.Type {
	case "created":
		var doc *concrnt.Document[world.AtprotoSettings]
		if sd, ok := ev.References[key]; ok {
			var parsed concrnt.Document[world.AtprotoSettings]
			if err := json.Unmarshal([]byte(sd.Document), &parsed); err == nil {
				doc = &parsed
			}
		}
		if doc == nil {
			fetched, err := core.GetDocument[world.AtprotoSettings](ctx, s.core, key)
			if err != nil {
				if !core.IsNotFound(err) {
					slog.Error("failed to resolve settings record", "ccid", ccid, "error", err)
					return
				}
			} else {
				doc = fetched
			}
		}
		s.set(ccid, sanitizeSettings(doc, ccid))
	case "deleted":
		s.set(ccid, defaultSettings())
	}
}

func (s *SettingsStore) set(ccid string, v settingsValue) {
	s.mu.Lock()
	s.values[ccid] = v
	s.loaded[ccid] = true
	s.mu.Unlock()
}
