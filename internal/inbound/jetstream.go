// Package inbound consumes the Bluesky network (via Jetstream) and mirrors
// relevant activity into concrnt.
//
// Subscription strategy: one connection, filtered by collection only, with
// local DID filtering. Replies/likes/quotes targeting bridged accounts can
// come from any DID, so a full (collection-filtered) stream is needed
// anyway; this also avoids Jetstream's wantedDids limits and reconnection
// churn on follow changes.
package inbound

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/store"
)

const cursorName = "jetstream"

// cursorRewind replays a little history on reconnect; ingestion is
// idempotent (deterministic keys / unique at-uris) so overlap is harmless.
const cursorRewind = 5 * time.Second

var wantedCollections = []string{
	"app.bsky.feed.post",
	"app.bsky.feed.like",
	"app.bsky.feed.repost",
	"app.bsky.graph.follow",
}

type Consumer struct {
	db       *gorm.DB
	host     string
	ingester *Ingester

	reloadInterval time.Duration

	mu       sync.RWMutex
	followed map[string]bool // DIDs any bridged user follows
	bridged  map[string]bool // DIDs of our own bridged accounts
}

func NewConsumer(db *gorm.DB, host string, ingester *Ingester, reloadInterval time.Duration) *Consumer {
	return &Consumer{
		db:             db,
		host:           host,
		ingester:       ingester,
		reloadInterval: reloadInterval,
		followed:       map[string]bool{},
		bridged:        map[string]bool{},
	}
}

func (c *Consumer) reload() {
	var follows []store.Follow
	if err := c.db.Find(&follows).Error; err != nil {
		slog.Error("failed to load follows", "error", err)
		return
	}
	var ents []store.Entity
	if err := c.db.Where("did <> ''").Find(&ents).Error; err != nil {
		slog.Error("failed to load entities", "error", err)
		return
	}

	followed := make(map[string]bool, len(follows))
	for _, f := range follows {
		followed[f.SubjectDID] = true
	}
	bridged := make(map[string]bool, len(ents))
	for _, e := range ents {
		bridged[e.DID] = true
	}

	c.mu.Lock()
	c.followed = followed
	c.bridged = bridged
	c.mu.Unlock()

	if err := c.ingester.index.reload(c.db); err != nil {
		slog.Error("failed to reload mirrored index", "error", err)
	}
}

func (c *Consumer) loadCursor() int64 {
	var cur store.Cursor
	if err := c.db.Where("name = ?", cursorName).First(&cur).Error; err != nil {
		return 0
	}
	return cur.Value
}

func (c *Consumer) saveCursor(timeUS int64) {
	c.db.Save(&store.Cursor{Name: cursorName, Value: timeUS})
}

// wsURL builds the /subscribe URL from the configured host (hostname, or a
// http(s)/ws(s) URL).
func (c *Consumer) wsURL() (string, error) {
	raw := c.host
	if !strings.Contains(raw, "://") {
		raw = "wss://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "ws":
		u.Scheme = "ws"
	default:
		u.Scheme = "wss"
	}
	u.Path = "/subscribe"

	q := url.Values{}
	for _, col := range wantedCollections {
		q.Add("wantedCollections", col)
	}
	if cursor := c.loadCursor(); cursor > 0 {
		q.Set("cursor", strconv.FormatInt(cursor-cursorRewind.Microseconds(), 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Run consumes the stream until ctx is done, reconnecting on failure.
func (c *Consumer) Run(ctx context.Context) {
	c.reload()
	go func() {
		ticker := time.NewTicker(c.reloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.reload()
			case <-ctx.Done():
				return
			}
		}
	}()

	for ctx.Err() == nil {
		if err := c.consumeOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("jetstream consumer failed, reconnecting", "error", err)
		}
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
		}
	}
}

func (c *Consumer) consumeOnce(ctx context.Context) error {
	wsURL, err := c.wsURL()
	if err != nil {
		return err
	}

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Tear the connection down when ctx ends so ReadMessage unblocks.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	slog.Info("jetstream connected", "host", c.host)

	lastSave := time.Now()
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var ev Event
		if err := json.Unmarshal(msg, &ev); err != nil {
			slog.Debug("unparsable jetstream frame", "error", err)
			continue
		}
		c.handleEvent(ctx, &ev)

		if ev.TimeUS > 0 && time.Since(lastSave) > 5*time.Second {
			c.saveCursor(ev.TimeUS)
			lastSave = time.Now()
		}
	}
}

func (c *Consumer) handleEvent(ctx context.Context, ev *Event) {
	if ev.Kind != KindCommit || ev.Commit == nil {
		return
	}

	c.mu.RLock()
	isFollowed := c.followed[ev.DID]
	isBridged := c.bridged[ev.DID]
	bridged := c.bridged
	c.mu.RUnlock()

	// Never re-import our own bridged accounts' activity (echo loop).
	if isBridged {
		return
	}

	commit := ev.Commit
	switch commit.Operation {
	case OpCreate, OpUpdate:
		switch commit.Collection {
		case "app.bsky.feed.post":
			targets := c.bridgedRefs(commit.Record, bridged)
			if isFollowed || len(targets) > 0 {
				c.ingester.IngestPost(ctx, ev, isFollowed, targets)
			}
		case "app.bsky.feed.repost":
			subjectDID := didOfATURI(subjectURI(commit.Record))
			if isFollowed || bridged[subjectDID] {
				c.ingester.IngestRepost(ctx, ev, isFollowed, bridged[subjectDID])
			}
		case "app.bsky.feed.like":
			if bridged[didOfATURI(subjectURI(commit.Record))] {
				c.ingester.IngestLike(ctx, ev)
			}
		case "app.bsky.graph.follow":
			// A follow record's subject is a plain DID, not a StrongRef.
			subject, _ := commit.Record["subject"].(string)
			if bridged[subject] {
				c.ingester.IngestFollow(ctx, ev, subject)
			}
		}
	case OpDelete:
		// Gate the network-wide delete stream on the in-memory index of
		// records we actually mirrored.
		uri := atURI(ev)
		if c.ingester.index.hasPost(uri) || c.ingester.index.hasLike(uri) {
			c.ingester.IngestDelete(ctx, ev)
		}
	}
}

// bridgedRefs returns the at-uris in the post's reply/quote refs whose DIDs
// belong to bridged accounts.
func (c *Consumer) bridgedRefs(record map[string]any, bridged map[string]bool) []string {
	var out []string
	for _, ref := range postRefs(record) {
		if bridged[didOfATURI(ref)] {
			out = append(out, ref)
		}
	}
	return out
}

// didOfATURI extracts the authority (DID) from at://did/collection/rkey.
func didOfATURI(uri string) string {
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return ""
	}
	if idx := strings.IndexByte(rest, '/'); idx > 0 {
		return rest[:idx]
	}
	return rest
}
