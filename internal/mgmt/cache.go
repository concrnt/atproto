package mgmt

import (
	"sync"
	"time"

	"github.com/concrnt/atproto/internal/appview"
)

const graphCacheTTL = 2 * time.Minute

// graphCache is a small in-process TTL cache for appview graph pages, keyed
// by did+cursor+limit. It exists so follower lists stay on-demand (never
// materialized in the DB) without hammering the appview on every reload.
type graphCache struct {
	mu      sync.Mutex
	entries map[string]graphCacheEntry
}

type graphCacheEntry struct {
	page *appview.GraphPage
	at   time.Time
}

func (c *graphCache) get(key string) *appview.GraphPage {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if time.Since(e.at) > graphCacheTTL {
			delete(c.entries, k)
		}
	}
	if e, ok := c.entries[key]; ok {
		return e.page
	}
	return nil
}

func (c *graphCache) put(key string, page *appview.GraphPage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]graphCacheEntry{}
	}
	c.entries[key] = graphCacheEntry{page: page, at: time.Now()}
}
