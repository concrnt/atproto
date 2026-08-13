package inbound

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

func postRefs(record map[string]any) []string { return toconcrnt.PostRefs(record) }
func subjectURI(record map[string]any) string { return toconcrnt.SubjectURI(record) }

// Ingester turns filtered Jetstream events into concrnt commits.
type Ingester struct {
	db       *gorm.DB
	core     *core.Service
	appview  *appview.Client
	actorTTL time.Duration
	index    *liveIndex
}

func NewIngester(db *gorm.DB, coreSvc *core.Service, av *appview.Client, actorTTL time.Duration) *Ingester {
	return &Ingester{db: db, core: coreSvc, appview: av, actorTTL: actorTTL, index: newLiveIndex()}
}

func atURI(ev *Event) string {
	return fmt.Sprintf("at://%s/%s/%s", ev.DID, ev.Commit.Collection, ev.Commit.Rkey)
}

func createdAt(ev *Event) time.Time {
	if ev.Commit != nil && ev.Commit.Record != nil {
		if s, ok := ev.Commit.Record["createdAt"].(string); ok {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				return t.UTC()
			}
		}
	}
	return time.UnixMicro(ev.TimeUS).UTC()
}

// profileOverride returns cached display info for a bsky actor, refreshing
// from the appview when stale. Failures degrade to a bare handle-less
// override rather than blocking ingestion.
func (g *Ingester) profileOverride(ctx context.Context, did string) *world.ProfileOverride {
	var actor store.Actor
	err := g.db.Where("did = ?", did).First(&actor).Error
	if err != nil || time.Since(actor.FetchedAt) > g.actorTTL {
		if profile, perr := g.appview.GetProfile(ctx, did); perr == nil {
			actor = store.Actor{
				DID:         did,
				Handle:      profile.Handle,
				DisplayName: profile.DisplayName,
				AvatarURL:   profile.Avatar,
				FetchedAt:   time.Now(),
			}
			g.db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "did"}},
				UpdateAll: true,
			}).Create(&actor)
		} else if err != nil {
			return nil
		}
	}

	username := actor.DisplayName
	if username == "" {
		username = actor.Handle
	}
	link := "https://bsky.app/profile/" + did
	if actor.Handle != "" {
		link = "https://bsky.app/profile/" + actor.Handle
	}
	return &world.ProfileOverride{
		Username: username,
		Avatar:   actor.AvatarURL,
		Link:     link,
	}
}

// followerInboxes returns the inbox timelines of every bridged user
// following did.
//
// TODO: with enough bridged followers of one actor (~400) the resulting
// distributes list pushes the signed document past the server's 32KiB
// document limit and the whole commit fails; needs batched commits.
func (g *Ingester) followerInboxes(did string) []string {
	var follows []store.Follow
	if err := g.db.Where("subject_did = ?", did).Find(&follows).Error; err != nil {
		return nil
	}
	var inboxes []string
	for _, f := range follows {
		inboxes = append(inboxes, toconcrnt.InboxTimeline(f.CCID))
	}
	return inboxes
}

// notifyTimelineOfDID resolves a bridged DID to its owner's notify timeline.
func (g *Ingester) notifyTimelineOfDID(did string) string {
	var ent store.Entity
	if err := g.db.Where("did = ?", did).First(&ent).Error; err != nil {
		return ""
	}
	return toconcrnt.NotifyTimeline(ent.CCID)
}

// IngestPost mirrors a post from a followed actor and/or a reply/quote
// aimed at bridged accounts.
func (g *Ingester) IngestPost(ctx context.Context, ev *Event, isFollowed bool, bridgedTargets []string) {
	uri := atURI(ev)

	var distributes []string
	if isFollowed {
		distributes = append(distributes, g.followerInboxes(ev.DID)...)
	}
	for _, target := range bridgedTargets {
		if nt := g.notifyTimelineOfDID(didOfATURI(target)); nt != "" {
			distributes = append(distributes, nt)
		}
	}
	if len(distributes) == 0 {
		return
	}

	override := g.profileOverride(ctx, ev.DID)
	doc := toconcrnt.BuildRecordDoc(g.core.CCID(), ev.DID, uri, ev.Commit.CID, override, dedupe(distributes), createdAt(ev))
	if _, err := core.Commit(ctx, g.core, doc); err != nil {
		slog.Error("failed to commit mirrored post", "atUri", uri, "error", err)
		return
	}
	g.cacheRemotePost(ev, uri)
	g.index.addPost(uri)
	slog.Info("mirrored post", "atUri", uri, "distributes", len(distributes))
}

// IngestFollow delivers a follow notification to the bridged subject's notify
// timeline. No follower state is materialized: unfollows are not tracked and
// the document upserts at a key derived from the (follower, subject) pair.
func (g *Ingester) IngestFollow(ctx context.Context, ev *Event, subjectDID string) {
	notify := g.notifyTimelineOfDID(subjectDID)
	if notify == "" {
		return
	}
	override := g.profileOverride(ctx, ev.DID)
	doc := toconcrnt.BuildFollowNotifyDoc(g.core.CCID(), ev.DID, subjectDID, override, []string{notify}, createdAt(ev))
	if _, err := core.Commit(ctx, g.core, doc); err != nil {
		slog.Error("failed to commit follow notification", "follower", ev.DID, "subject", subjectDID, "error", err)
		return
	}
	slog.Info("follow notification", "follower", ev.DID, "subject", subjectDID)
}

// IngestRepost mirrors a repost by a followed actor, and/or notifies a
// bridged author their post was reposted.
func (g *Ingester) IngestRepost(ctx context.Context, ev *Event, isFollowed, subjectBridged bool) {
	uri := atURI(ev)
	subjURI := toconcrnt.SubjectURI(ev.Commit.Record)
	subjCID := toconcrnt.SubjectCID(ev.Commit.Record)
	if subjURI == "" {
		return
	}

	// Resolve the repost subject to a concrnt URI: an exported local post's
	// original, or a mirrored remote record (created on demand, undistributed).
	var targetCcURI string
	var m store.URIMap
	if err := g.db.Where("at_uri = ? AND ref_type = ?", subjURI, "outbound-post").First(&m).Error; err == nil {
		targetCcURI = m.CcURI
	} else {
		subjDID := didOfATURI(subjURI)
		subjOverride := g.profileOverride(ctx, subjDID)
		subjDoc := toconcrnt.BuildRecordDoc(g.core.CCID(), subjDID, subjURI, subjCID, subjOverride, nil, createdAt(ev))
		if _, err := core.Commit(ctx, g.core, subjDoc); err != nil {
			slog.Error("failed to mirror repost subject", "atUri", subjURI, "error", err)
			return
		}
		targetCcURI = toconcrnt.InboxKey(g.core.CCID(), subjURI)
		g.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&store.RemotePost{
			AtURI: subjURI, DID: subjDID, CID: subjCID, IndexedAt: time.Now(),
		})
		g.index.addPost(subjURI)
	}

	var distributes []string
	if isFollowed {
		distributes = append(distributes, g.followerInboxes(ev.DID)...)
	}
	if subjectBridged {
		if nt := g.notifyTimelineOfDID(didOfATURI(subjURI)); nt != "" {
			distributes = append(distributes, nt)
		}
	}
	if len(distributes) == 0 {
		return
	}

	override := g.profileOverride(ctx, ev.DID)
	doc := toconcrnt.BuildRerouteDoc(g.core.CCID(), uri, targetCcURI, override, dedupe(distributes), createdAt(ev))
	if _, err := core.Commit(ctx, g.core, doc); err != nil {
		slog.Error("failed to commit mirrored repost", "atUri", uri, "error", err)
		return
	}
	g.cacheRemotePost(ev, uri)
	g.index.addPost(uri)
}

// IngestLike attaches a like association to an exported local post.
func (g *Ingester) IngestLike(ctx context.Context, ev *Event) {
	subjURI := toconcrnt.SubjectURI(ev.Commit.Record)
	var m store.URIMap
	if err := g.db.Where("at_uri = ? AND ref_type = ?", subjURI, "outbound-post").First(&m).Error; err != nil {
		return // subject is not an exported concrnt post
	}

	override := g.profileOverride(ctx, ev.DID)
	notify := g.notifyTimelineOfDID(didOfATURI(subjURI))
	var distributes []string
	if notify != "" {
		distributes = append(distributes, notify)
	}

	doc := toconcrnt.BuildLikeAssociation(g.core.CCID(), m.CcURI, override, distributes, createdAt(ev))
	result, err := core.Commit(ctx, g.core, doc)
	if err != nil {
		slog.Error("failed to commit like association", "subject", subjURI, "error", err)
		return
	}
	if result.CCFS == nil {
		slog.Error("like commit response carried no ccfs uri", "subject", subjURI)
		return
	}

	// Remember the association so an unlike can delete it.
	g.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&store.URIMap{
		AtURI:      atURI(ev),
		CcURI:      *result.CCFS,
		DID:        ev.DID,
		Collection: ev.Commit.Collection,
		Rkey:       ev.Commit.Rkey,
		RefType:    "inbound-like",
	})
	g.index.addLike(atURI(ev))
}

// IngestDelete undoes a previously mirrored record.
func (g *Ingester) IngestDelete(ctx context.Context, ev *Event) {
	uri := atURI(ev)

	switch ev.Commit.Collection {
	case "app.bsky.feed.post", "app.bsky.feed.repost":
		// The liveIndex gate (checked by the consumer) already established
		// this record was mirrored; the deterministic key makes the delete
		// resolvable without any lookup.
		doc := toconcrnt.BuildDeleteDoc(g.core.CCID(), toconcrnt.InboxKey(g.core.CCID(), uri))
		if _, err := core.Commit(ctx, g.core, doc); err != nil {
			slog.Warn("failed to delete mirrored record", "atUri", uri, "error", err)
		}
		g.db.Where("at_uri = ?", uri).Delete(&store.RemotePost{})
		g.index.remove(uri)

	case "app.bsky.feed.like":
		var m store.URIMap
		if err := g.db.Where("at_uri = ? AND ref_type = ?", uri, "inbound-like").First(&m).Error; err != nil {
			return
		}
		doc := toconcrnt.BuildDeleteDoc(g.core.CCID(), m.CcURI)
		if _, err := core.Commit(ctx, g.core, doc); err != nil {
			slog.Warn("failed to delete like association", "atUri", uri, "error", err)
			return
		}
		g.db.Delete(&m)
		g.index.remove(uri)
	}
}

func (g *Ingester) cacheRemotePost(ev *Event, uri string) {
	b, err := json.Marshal(ev.Commit.Record)
	if err != nil {
		return
	}
	g.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "at_uri"}},
		UpdateAll: true,
	}).Create(&store.RemotePost{
		AtURI:     uri,
		DID:       ev.DID,
		CID:       ev.Commit.CID,
		Record:    datatypes.JSON(b),
		IndexedAt: time.Now(),
	})
}

func dedupe(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := in[:0]
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
