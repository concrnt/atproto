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

	concrnt "github.com/concrnt/concrnt"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

func postRefs(record map[string]any) toconcrnt.PostRefSet { return toconcrnt.ParsePostRefs(record) }
func subjectURI(record map[string]any) string             { return toconcrnt.SubjectURI(record) }

// InboxChecker answers whether a bridged user's inbox timeline exists (the
// outbound daemon's InboxStore; an interface here so inbound does not import
// outbound). Delivery to a missing inbox is refused by policy on the core
// and only ends up in its dead-letter queue, so such recipients are dropped.
type InboxChecker interface {
	Has(ccid string) bool
	EnsureLoaded(ctx context.Context, ccid string) error
}

// Ingester turns filtered Jetstream events into concrnt commits.
type Ingester struct {
	db       *gorm.DB
	core     *core.Service
	appview  *appview.Client
	actorTTL time.Duration
	index    *liveIndex
	inboxes  InboxChecker
}

func NewIngester(db *gorm.DB, coreSvc *core.Service, av *appview.Client, actorTTL time.Duration, inboxes InboxChecker) *Ingester {
	return &Ingester{db: db, core: coreSvc, appview: av, actorTTL: actorTTL, index: newLiveIndex(), inboxes: inboxes}
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
// following did, leaving out users whose inbox timeline does not exist yet
// (lazily loaded, so a follower picked up before the daemon's sync tick is
// still checked).
//
// TODO: with enough bridged followers of one actor (~400) the resulting
// distributes list pushes the signed document past the server's 32KiB
// document limit and the whole commit fails; needs batched commits.
func (g *Ingester) followerInboxes(ctx context.Context, did string) []string {
	var follows []store.Follow
	if err := g.db.Where("subject_did = ?", did).Find(&follows).Error; err != nil {
		return nil
	}
	var inboxes []string
	for _, f := range follows {
		if err := g.inboxes.EnsureLoaded(ctx, f.CCID); err != nil {
			slog.Error("failed to load inbox state", "ccid", f.CCID, "error", err)
		}
		if !g.inboxes.Has(f.CCID) {
			slog.Debug("skipping delivery: no inbox timeline", "ccid", f.CCID, "subject", did)
			continue
		}
		inboxes = append(inboxes, toconcrnt.InboxTimeline(f.CCID))
	}
	return inboxes
}

// ccidOfDID resolves a bridged DID to its owner's CCID ("" when not bridged).
func (g *Ingester) ccidOfDID(did string) string {
	var ent store.Entity
	if err := g.db.Where("did = ?", did).First(&ent).Error; err != nil {
		return ""
	}
	return ent.CCID
}

// notifyTimelineOfDID resolves a bridged DID to its owner's notify timeline.
func (g *Ingester) notifyTimelineOfDID(did string) string {
	ccid := g.ccidOfDID(did)
	if ccid == "" {
		return ""
	}
	return toconcrnt.NotifyTimeline(ccid)
}

// exportedPostCcURI resolves the at-uri of a bridged user's exported post to
// the concrnt post it was exported from ("" when the bridge did not export
// it; such posts are not notified about, as the AP bridge ignores inReplyTo
// outside its management).
func (g *Ingester) exportedPostCcURI(atURI string) string {
	if atURI == "" {
		return ""
	}
	var m store.URIMap
	if err := g.db.Where("at_uri = ? AND ref_type = ?", atURI, "outbound-post").First(&m).Error; err != nil {
		return ""
	}
	return m.CcURI
}

// notifyInteraction attaches a reply/reroute association to the bridged
// user's post targetCcURI, delivered only to their notify timeline (the body
// record stays in the service namespace: a record distributed to the
// restrict-readers notify timeline would inherit that policy and become
// unreadable to everyone else). The association is remembered under refType
// so a delete of the bsky record can remove it; a replayed event finds the
// row and does not commit twice.
func (g *Ingester) notifyInteraction(ctx context.Context, ev *Event, refType, schema, targetAtURI, targetCcURI, bodyKey string, override *world.ProfileOverride) {
	uri := atURI(ev)
	var existing store.URIMap
	if err := g.db.Where("at_uri = ? AND ref_type = ?", uri, refType).First(&existing).Error; err == nil {
		return
	}
	notify := g.notifyTimelineOfDID(didOfATURI(targetAtURI))
	if notify == "" {
		return
	}

	var doc concrnt.Document[world.TargetAssociationValue]
	switch schema {
	case world.SchemaReplyAssociation:
		doc = toconcrnt.BuildReplyAssociation(g.core.CCID(), targetCcURI, bodyKey, override, []string{notify}, createdAt(ev))
	case world.SchemaRerouteAssociation:
		doc = toconcrnt.BuildRerouteAssociation(g.core.CCID(), targetCcURI, bodyKey, override, []string{notify}, createdAt(ev))
	default:
		return
	}
	result, err := core.Commit(ctx, g.core, doc)
	if err != nil {
		slog.Error("failed to commit notification association", "schema", schema, "atUri", uri, "target", targetCcURI, "error", err)
		return
	}
	if result.CCFS == nil {
		slog.Error("notification commit response carried no ccfs uri", "schema", schema, "atUri", uri)
		return
	}
	g.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&store.URIMap{
		AtURI:      uri,
		CcURI:      *result.CCFS,
		DID:        ev.DID,
		Collection: ev.Commit.Collection,
		Rkey:       ev.Commit.Rkey,
		RefType:    refType,
	})
	slog.Info("notified interaction", "schema", schema, "atUri", uri, "target", targetCcURI)
}

// IngestPost mirrors a post from a followed actor and/or a reply/quote
// aimed at bridged accounts. The body record goes to followers' inboxes
// only; replies and quotes of bridged users' exported posts are notified
// with reply/reroute associations on those posts.
func (g *Ingester) IngestPost(ctx context.Context, ev *Event, isFollowed bool, targets toconcrnt.PostRefSet) {
	uri := atURI(ev)

	var distributes []string
	if isFollowed {
		distributes = append(distributes, g.followerInboxes(ctx, ev.DID)...)
	}
	replyTarget := g.exportedPostCcURI(targets.Parent)
	quoteTarget := g.exportedPostCcURI(targets.Quote)
	if len(distributes) == 0 && replyTarget == "" && quoteTarget == "" {
		return
	}

	// The body is committed even with no inbox to deliver to: the
	// notification associations below point at it.
	override := g.profileOverride(ctx, ev.DID)
	doc := toconcrnt.BuildRecordDoc(g.core.CCID(), ev.DID, uri, ev.Commit.CID, override, dedupe(distributes), createdAt(ev))
	if _, err := core.Commit(ctx, g.core, doc); err != nil {
		slog.Error("failed to commit mirrored post", "atUri", uri, "error", err)
		return
	}
	g.cacheRemotePost(ev, uri)
	g.index.addPost(uri)
	slog.Info("mirrored post", "atUri", uri, "distributes", len(distributes))

	if replyTarget != "" {
		g.notifyInteraction(ctx, ev, "inbound-reply", world.SchemaReplyAssociation, targets.Parent, replyTarget, doc.Key, override)
	}
	if quoteTarget != "" {
		g.notifyInteraction(ctx, ev, "inbound-reroute", world.SchemaRerouteAssociation, targets.Quote, quoteTarget, doc.Key, override)
	}
}

// IngestFollow delivers a follow notification to the bridged subject's notify
// timeline as an association on their main profile. The association is
// remembered under inbound-follow so an unfollow (record delete) removes it
// and a replayed event does not commit twice.
func (g *Ingester) IngestFollow(ctx context.Context, ev *Event, subjectDID string) {
	uri := atURI(ev)
	var existing store.URIMap
	if err := g.db.Where("at_uri = ? AND ref_type = ?", uri, "inbound-follow").First(&existing).Error; err == nil {
		g.index.addFollow(uri)
		return
	}
	subjectCCID := g.ccidOfDID(subjectDID)
	if subjectCCID == "" {
		return
	}
	notify := toconcrnt.NotifyTimeline(subjectCCID)
	override := g.profileOverride(ctx, ev.DID)
	doc := toconcrnt.BuildFollowNotifyAssociation(g.core.CCID(), ev.DID, toconcrnt.MainProfile(subjectCCID), override, []string{notify}, createdAt(ev))
	result, err := core.Commit(ctx, g.core, doc)
	if err != nil {
		slog.Error("failed to commit follow notification", "follower", ev.DID, "subject", subjectDID, "error", err)
		return
	}
	if result.CCFS == nil {
		slog.Error("follow notification commit response carried no ccfs uri", "follower", ev.DID, "subject", subjectDID)
		return
	}
	g.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&store.URIMap{
		AtURI:      uri,
		CcURI:      *result.CCFS,
		DID:        ev.DID,
		Collection: ev.Commit.Collection,
		Rkey:       ev.Commit.Rkey,
		RefType:    "inbound-follow",
	})
	g.index.addFollow(uri)
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
	exported := false
	var m store.URIMap
	if err := g.db.Where("at_uri = ? AND ref_type = ?", subjURI, "outbound-post").First(&m).Error; err == nil {
		targetCcURI = m.CcURI
		exported = true
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
		distributes = append(distributes, g.followerInboxes(ctx, ev.DID)...)
	}
	// Only an exported post of a bridged user is notified about (the reroute
	// association must attach to the user's own post, not a mirror).
	notifyTarget := ""
	if subjectBridged && exported {
		notifyTarget = targetCcURI
	}
	if len(distributes) == 0 && notifyTarget == "" {
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
	slog.Info("mirrored repost", "atUri", uri, "distributes", len(distributes))

	if notifyTarget != "" {
		g.notifyInteraction(ctx, ev, "inbound-reroute", world.SchemaRerouteAssociation, subjURI, notifyTarget, doc.Key, override)
	}
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
		// Notification associations attached to bridged users' posts for
		// this record go with it.
		var refs []store.URIMap
		g.db.Where("at_uri = ? AND ref_type IN ?", uri, []string{"inbound-reply", "inbound-reroute"}).Find(&refs)
		for _, ref := range refs {
			if _, err := core.Commit(ctx, g.core, toconcrnt.BuildDeleteDoc(g.core.CCID(), ref.CcURI)); err != nil {
				slog.Warn("failed to delete notification association", "atUri", uri, "refType", ref.RefType, "error", err)
				continue
			}
			g.db.Delete(&ref)
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

	case "app.bsky.graph.follow":
		// An unfollow withdraws the follow notification.
		var m store.URIMap
		if err := g.db.Where("at_uri = ? AND ref_type = ?", uri, "inbound-follow").First(&m).Error; err != nil {
			return
		}
		doc := toconcrnt.BuildDeleteDoc(g.core.CCID(), m.CcURI)
		if _, err := core.Commit(ctx, g.core, doc); err != nil {
			slog.Warn("failed to delete follow notification", "atUri", uri, "error", err)
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
