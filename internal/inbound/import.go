package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm/clause"

	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

// ErrPostNotFound is returned by ImportPost when the appview knows no post
// at the requested at-uri (deleted, never indexed, or not a post).
var ErrPostNotFound = errors.New("post not found")

// ImportPost mirrors a single Bluesky post into concrnt on demand, the way
// the ActivityPub bridge's import endpoint materializes a remote note: a
// client that opened a post the bridge never delivered (nobody bridged
// follows the author) asks for it so the native detail view (replies,
// reactions) can hang off a real concrnt record. The record goes under the
// deterministic inbox key and is distributed to no timeline; it is committed
// through the repository import path so posts older than the server's
// backdate window are accepted. Already-mirrored posts are left untouched
// (re-committing without distributes would strip the delivered record of its
// timelines) and their key is returned. The post is also remembered as a
// remote post and indexed, so replies to it resolve their thread root and a
// later delete on the firehose removes the mirror like any other.
func (g *Ingester) ImportPost(ctx context.Context, atURI string) (string, error) {
	key := toconcrnt.InboxKey(g.core.CCID(), atURI)
	_, err := core.GetDocument[world.AtprotoRecord](ctx, g.core, key)
	if err == nil {
		return key, nil
	}
	if !core.IsNotFound(err) {
		return "", fmt.Errorf("failed to check existing mirror: %w", err)
	}

	posts, err := g.appview.GetPosts(ctx, []string{atURI})
	if err != nil {
		return "", fmt.Errorf("appview lookup failed: %w", err)
	}
	if len(posts) == 0 {
		return "", ErrPostNotFound
	}
	var view struct {
		URI    string `json:"uri"`
		CID    string `json:"cid"`
		Author struct {
			DID string `json:"did"`
		} `json:"author"`
		Record map[string]any `json:"record"`
	}
	if err := json.Unmarshal(posts[0], &view); err != nil {
		return "", fmt.Errorf("unparsable post view: %w", err)
	}
	if view.URI != "" && view.URI != atURI {
		return "", ErrPostNotFound
	}
	did := view.Author.DID
	if did == "" {
		did = didOfATURI(atURI)
	}
	createdAt := time.Now().UTC()
	if s, ok := view.Record["createdAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			createdAt = t.UTC()
		}
	}

	doc := toconcrnt.BuildRecordDoc(g.core.CCID(), atURI, view.CID, nil, createdAt)
	if err := core.Import(ctx, g.core, doc); err != nil {
		return "", fmt.Errorf("failed to commit mirrored post: %w", err)
	}

	if record, err := json.Marshal(view.Record); err == nil {
		g.db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "at_uri"}},
			UpdateAll: true,
		}).Create(&store.RemotePost{
			AtURI:     atURI,
			DID:       did,
			CID:       view.CID,
			Record:    datatypes.JSON(record),
			IndexedAt: time.Now(),
		})
	}
	g.index.addPost(atURI)
	slog.Info("imported post on demand", "atUri", atURI, "key", doc.Key)
	return doc.Key, nil
}
