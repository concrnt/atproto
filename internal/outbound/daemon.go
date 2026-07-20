// Package outbound watches concrnt core events (via the shared Redis) and
// exports bridged users' activity into their Bluesky repos. It is the Go
// port of the ActivityPub bridge's daemon.ts, with delivery replaced by repo
// writes (the firehose does the actual fan-out).
package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	concrnt "github.com/concrnt/concrnt"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/atproto/internal/blobs"
	"github.com/concrnt/atproto/internal/convert/tobsky"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
	"github.com/concrnt/atproto/internal/world"
)

// Doc is a concrnt document with an unparsed value.
type Doc = concrnt.Document[json.RawMessage]

type Daemon struct {
	db      *gorm.DB
	core    *core.Service
	repos   *repoman.Manager
	blobs   *blobs.Service
	postURL string // template for permalinks, {uri} placeholder

	reloadInterval time.Duration

	mu       sync.RWMutex
	entities []store.Entity
	// ccfs URIs of likes we exported; filters the firehose of unrelated
	// "deleted" events without hitting the DB for each one.
	outboundLikes map[string]bool
}

func NewDaemon(db *gorm.DB, coreSvc *core.Service, repos *repoman.Manager, blobSvc *blobs.Service, postURLTemplate string, reloadInterval time.Duration) *Daemon {
	return &Daemon{
		db:             db,
		core:           coreSvc,
		repos:          repos,
		blobs:          blobSvc,
		postURL:        postURLTemplate,
		reloadInterval: reloadInterval,
		outboundLikes:  map[string]bool{},
	}
}

func (d *Daemon) reload() {
	var ents []store.Entity
	if err := d.db.Find(&ents).Error; err != nil {
		slog.Error("failed to load entities", "error", err)
		return
	}
	var likes []store.URIMap
	if err := d.db.Where("ref_type = ?", "outbound-like").Find(&likes).Error; err != nil {
		slog.Error("failed to load outbound likes", "error", err)
		return
	}
	likeSet := make(map[string]bool, len(likes))
	for _, l := range likes {
		likeSet[l.CcURI] = true
	}

	d.mu.Lock()
	d.entities = ents
	d.outboundLikes = likeSet
	d.mu.Unlock()
}

// Run consumes events until ctx is done.
func (d *Daemon) Run(ctx context.Context, events <-chan core.ChannelEvent) {
	d.reload()
	go func() {
		ticker := time.NewTicker(d.reloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.reload()
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			d.dispatch(ctx, ev)
		}
	}
}

func (d *Daemon) dispatch(ctx context.Context, ev core.ChannelEvent) {
	channel := ev.Channel
	if !strings.HasPrefix(channel, "ccfs://") && !strings.HasPrefix(channel, "cckv://") {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic while processing event", "channel", channel, "panic", r)
		}
	}()

	d.mu.RLock()
	entities := d.entities
	d.mu.RUnlock()

	for i := range entities {
		entity := &entities[i]
		if !entity.Enabled || entity.DID == "" {
			continue
		}

		timelines := []string(entity.ListenTimelines)
		if len(timelines) == 0 {
			timelines = []string{fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/home-timeline", entity.CCID)}
		}
		for _, prefix := range timelines {
			if strings.HasPrefix(channel, prefix) {
				d.handleTimelineEvent(ctx, entity, channel, ev.Event)
				break
			}
		}

		profileKey := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main", entity.CCID)
		if channel == profileKey && ev.Event.Type == "created" {
			d.handleProfileUpdate(ctx, entity)
		}

		followPrefix := fmt.Sprintf("cckv://%s/%s", entity.CCID, world.FollowsKeyPrefix)
		if strings.HasPrefix(channel, followPrefix) {
			d.handleFollowEvent(ctx, entity, channel, ev.Event)
		}
	}

	assocPrefix := fmt.Sprintf("cckv://%s/%s", d.core.CCID(), world.InboxKeyPrefix)
	if ev.Event.Type == "associated" && strings.HasPrefix(channel, assocPrefix) {
		d.handleAssociationEvent(ctx, ev.Event)
	}

	if ev.Event.Type == "deleted" && strings.HasPrefix(ev.Event.URI, "ccfs://") {
		d.handleAssociationDeleted(ctx, ev.Event)
	}
}

// resolveEventDocument returns the document for uri, preferring the signed
// copy embedded in the event over a fetch.
func (d *Daemon) resolveEventDocument(ctx context.Context, ev concrnt.Event, uri string) (*Doc, error) {
	if sd, ok := ev.References[uri]; ok {
		var doc Doc
		if err := json.Unmarshal([]byte(sd.Document), &doc); err == nil {
			return &doc, nil
		}
	}
	return core.GetDocument[json.RawMessage](ctx, d.core, uri)
}

func (d *Daemon) handleTimelineEvent(ctx context.Context, entity *store.Entity, channel string, ev concrnt.Event) {
	switch ev.Type {
	case "created":
		doc, err := d.resolveEventDocument(ctx, ev, channel)
		if err != nil {
			slog.Error("failed to resolve timeline document", "channel", channel, "error", err)
			return
		}
		cckv := doc.Key

		if doc.Author != entity.CCID {
			return
		}

		// Timelines carry reference documents; follow them to the message.
		if doc.Schema == world.SchemaReference {
			var ref struct {
				Href string `json:"href"`
			}
			if err := json.Unmarshal(doc.Value, &ref); err != nil || ref.Href == "" {
				return
			}
			cckv = ref.Href
			if sd, ok := ev.References[channel]; ok {
				if inner, ok := sd.References[cckv]; ok {
					var innerDoc Doc
					if err := json.Unmarshal([]byte(inner.Document), &innerDoc); err == nil {
						doc = &innerDoc
					}
				}
			}
			if doc.Key != cckv {
				doc, err = core.GetDocument[json.RawMessage](ctx, d.core, cckv)
				if err != nil {
					slog.Error("failed to resolve referenced document", "uri", cckv, "error", err)
					return
				}
			}
			if doc.Author != entity.CCID {
				return
			}
		}

		if world.IsBridgedSchema(doc.Schema) || !world.IsMessageSchema(doc.Schema) {
			return
		}

		// Deduplicate: multiple listen timelines can surface the same post.
		var count int64
		d.db.Model(&store.URIMap{}).Where("cc_uri = ?", cckv).Count(&count)
		if count > 0 {
			return
		}

		d.exportMessage(ctx, entity, cckv, doc)

	case "deleted":
		cckv := ev.URI
		if !strings.HasPrefix(cckv, "cckv://"+entity.CCID+"/") {
			return
		}
		var ref store.URIMap
		if err := d.db.Where("cc_uri = ? AND did = ?", cckv, entity.DID).First(&ref).Error; err != nil {
			return
		}
		if err := d.repos.DeleteRecord(ctx, entity.DID, ref.Collection, ref.Rkey); err != nil {
			slog.Error("failed to delete record", "atUri", ref.AtURI, "error", err)
			return
		}
		d.db.Delete(&ref)
	}
}

// exportMessage converts one concrnt message document into a bsky record.
func (d *Daemon) exportMessage(ctx context.Context, entity *store.Entity, cckv string, doc *Doc) {
	var value world.MessageValue
	if err := json.Unmarshal(doc.Value, &value); err != nil {
		slog.Error("failed to parse message value", "uri", cckv, "error", err)
		return
	}

	// Plain reroute → repost.
	if world.IsPlainReroute(doc.Schema, value.Body) {
		if value.TargetURI == "" {
			return
		}
		subject, _ := d.resolveSubject(ctx, value.TargetURI)
		if subject == nil {
			slog.Info("reroute target not bridgeable, skipping", "target", value.TargetURI)
			return
		}
		repost := tobsky.BuildRepost(*subject, doc.CreatedAt)
		atURI, cid, err := d.repos.CreateRecord(ctx, entity.DID, "app.bsky.feed.repost", repost)
		if err != nil {
			slog.Error("failed to create repost", "uri", cckv, "error", err)
			return
		}
		d.saveURIMap(atURI, cckv, entity.DID, cid, "outbound-repost", nil)
		return
	}

	parts := tobsky.BuildPostParts(value.Body, doc.Schema == world.SchemaPlaintext)

	opts := tobsky.PostOptions{CreatedAt: doc.CreatedAt}

	// Reply target resolution; unresolvable reply targets are skipped
	// entirely (a contextless reply would be worse than none).
	if doc.Schema == world.SchemaReply {
		if value.TargetURI == "" {
			return
		}
		parent, root := d.resolveReply(ctx, value.TargetURI)
		if parent == nil {
			slog.Info("reply target not bridgeable, skipping", "target", value.TargetURI)
			return
		}
		opts.Reply = &tobsky.ReplyRefs{Parent: *parent, Root: *root}
	}

	// Quote reroute (reroute with body).
	if doc.Schema == world.SchemaReroute {
		if value.TargetURI == "" {
			return
		}
		quote, _ := d.resolveSubject(ctx, value.TargetURI)
		if quote == nil {
			slog.Info("quote target not bridgeable, skipping", "target", value.TargetURI)
			return
		}
		opts.Quote = quote
	}

	// Collect images: media schema attachments first, then inline markdown
	// images, capped at 4.
	type imgSrc struct{ url, alt string }
	var sources []imgSrc
	for _, m := range value.Medias {
		if strings.HasPrefix(m.MediaType, "image/") {
			sources = append(sources, imgSrc{url: m.MediaURL})
		}
	}
	for _, img := range parts.InlineImages {
		sources = append(sources, imgSrc{url: img.URL, alt: img.Alt})
	}
	for _, src := range sources {
		if len(opts.Images) >= 4 {
			break
		}
		blob, err := d.blobs.Ingest(ctx, entity.DID, src.url)
		if err != nil {
			slog.Warn("failed to ingest image", "url", src.url, "error", err)
			continue
		}
		opts.Images = append(opts.Images, &appbsky.EmbedImages_Image{
			Image: blob,
			Alt:   src.alt,
		})
	}

	tobsky.EnforceLimit(&parts)
	if parts.Truncated && d.postURL != "" {
		opts.TruncatedLink = &tobsky.ExternalLink{
			URI:         strings.ReplaceAll(d.postURL, "{uri}", cckv),
			Title:       "concrnt",
			Description: "Read the full post on concrnt",
		}
	}

	if strings.TrimSpace(parts.Text) == "" && len(opts.Images) == 0 && opts.Quote == nil {
		return
	}

	post := tobsky.AssemblePost(parts, opts)
	atURI, cid, err := d.repos.CreateRecord(ctx, entity.DID, "app.bsky.feed.post", post)
	if err != nil {
		slog.Error("failed to create post", "uri", cckv, "error", err)
		return
	}

	// Remember the thread root so replies to this post can find it.
	meta := map[string]string{}
	if opts.Reply != nil {
		meta["rootUri"] = opts.Reply.Root.URI
		meta["rootCid"] = opts.Reply.Root.CID
	} else {
		meta["rootUri"] = atURI
		meta["rootCid"] = cid
	}
	d.saveURIMap(atURI, cckv, entity.DID, cid, "outbound-post", meta)
	slog.Info("exported post", "ccUri", cckv, "atUri", atURI)
}

func (d *Daemon) saveURIMap(atURI, ccURI, did, cid, refType string, meta map[string]string) {
	rec := store.URIMap{
		AtURI:   atURI,
		CcURI:   ccURI,
		DID:     did,
		CID:     cid,
		RefType: refType,
	}
	if parts := strings.SplitN(strings.TrimPrefix(atURI, "at://"), "/", 3); len(parts) == 3 {
		rec.Collection = parts[1]
		rec.Rkey = parts[2]
	}
	if meta != nil {
		if b, err := json.Marshal(meta); err == nil {
			rec.Meta = datatypes.JSON(b)
		}
	}
	if err := d.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&rec).Error; err != nil {
		slog.Error("failed to save uri map", "atUri", atURI, "error", err)
	}
}

// resolveSubject maps a concrnt URI to a bsky record reference. It handles
// both remote records mirrored into concrnt (atproto/record.json) and local
// posts we already exported.
func (d *Daemon) resolveSubject(ctx context.Context, ccURI string) (*tobsky.StrongRef, *Doc) {
	doc, err := core.GetDocument[json.RawMessage](ctx, d.core, ccURI)
	if err == nil && doc.Schema == world.SchemaAtprotoRecord {
		var rec world.AtprotoRecord
		if err := json.Unmarshal(doc.Value, &rec); err == nil && rec.AtURI != "" {
			return &tobsky.StrongRef{URI: rec.AtURI, CID: rec.CID}, doc
		}
	}

	var m store.URIMap
	if err := d.db.Where("cc_uri = ? AND ref_type = ?", ccURI, "outbound-post").First(&m).Error; err == nil {
		return &tobsky.StrongRef{URI: m.AtURI, CID: m.CID}, doc
	}
	return nil, doc
}

// resolveReply returns (parent, root) refs for a reply target.
func (d *Daemon) resolveReply(ctx context.Context, ccURI string) (*tobsky.StrongRef, *tobsky.StrongRef) {
	parent, _ := d.resolveSubject(ctx, ccURI)
	if parent == nil {
		return nil, nil
	}
	root := *parent

	// Local exported post: root ref is stored in the map's meta.
	var m store.URIMap
	if err := d.db.Where("at_uri = ?", parent.URI).First(&m).Error; err == nil && len(m.Meta) > 0 {
		var meta map[string]string
		if err := json.Unmarshal(m.Meta, &meta); err == nil && meta["rootUri"] != "" {
			root = tobsky.StrongRef{URI: meta["rootUri"], CID: meta["rootCid"]}
		}
	}

	// Remote post: its cached record may carry a reply.root.
	var rp store.RemotePost
	if err := d.db.Where("at_uri = ?", parent.URI).First(&rp).Error; err == nil && len(rp.Record) > 0 {
		var rec struct {
			Reply *struct {
				Root struct {
					URI string `json:"uri"`
					CID string `json:"cid"`
				} `json:"root"`
			} `json:"reply"`
		}
		if err := json.Unmarshal(rp.Record, &rec); err == nil && rec.Reply != nil && rec.Reply.Root.URI != "" {
			root = tobsky.StrongRef{URI: rec.Reply.Root.URI, CID: rec.Reply.Root.CID}
		}
	}

	return parent, &root
}

// handleProfileUpdate pushes the concrnt profile into the bsky repo.
func (d *Daemon) handleProfileUpdate(ctx context.Context, entity *store.Entity) {
	if err := d.SyncProfile(ctx, entity); err != nil {
		slog.Error("failed to sync profile", "ccid", entity.CCID, "error", err)
	}
}

// SyncProfile fetches the user's concrnt profile and writes it as
// app.bsky.actor.profile. Also used right after setup.
func (d *Daemon) SyncProfile(ctx context.Context, entity *store.Entity) error {
	uri := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main", entity.CCID)
	doc, err := core.GetDocument[world.ProfileValue](ctx, d.core, uri)
	if err != nil {
		return fmt.Errorf("failed to fetch profile %s: %w", uri, err)
	}

	profile := tobsky.BuildProfile(doc.Value.Username, doc.Value.Description, nil)
	if doc.Value.Avatar != "" {
		if blob, err := d.blobs.Ingest(ctx, entity.DID, doc.Value.Avatar); err == nil {
			profile.Avatar = blob
		} else {
			slog.Warn("failed to ingest avatar", "url", doc.Value.Avatar, "error", err)
		}
	}
	if doc.Value.Banner != "" {
		if blob, err := d.blobs.Ingest(ctx, entity.DID, doc.Value.Banner); err == nil {
			profile.Banner = blob
		}
	}

	_, _, err = d.repos.PutRecord(ctx, entity.DID, "app.bsky.actor.profile", "self", profile)
	return err
}

// handleFollowEvent processes follow documents in the user's cckv space
// (the source of truth for concrnt→bsky follows).
func (d *Daemon) handleFollowEvent(ctx context.Context, entity *store.Entity, channel string, ev concrnt.Event) {
	switch ev.Type {
	case "created":
		doc, err := d.resolveEventDocument(ctx, ev, channel)
		if err != nil || doc.Author != entity.CCID || doc.Schema != world.SchemaAtprotoFollow {
			return
		}
		var follow world.AtprotoFollow
		if err := json.Unmarshal(doc.Value, &follow); err != nil || !strings.HasPrefix(follow.DID, "did:") {
			return
		}

		var count int64
		d.db.Model(&store.Follow{}).Where("ccid = ? AND subject_did = ?", entity.CCID, follow.DID).Count(&count)
		if count > 0 {
			return
		}

		rec := tobsky.BuildFollow(follow.DID, doc.CreatedAt)
		atURI, cid, err := d.repos.CreateRecord(ctx, entity.DID, "app.bsky.graph.follow", rec)
		if err != nil {
			slog.Error("failed to create follow record", "did", follow.DID, "error", err)
			return
		}
		rkey := atURI[strings.LastIndex(atURI, "/")+1:]
		d.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&store.Follow{
			CCID:       entity.CCID,
			SubjectDID: follow.DID,
			FollowRkey: rkey,
		})
		d.saveURIMap(atURI, doc.Key, entity.DID, cid, "outbound-follow", nil)
		slog.Info("followed", "ccid", entity.CCID, "subject", follow.DID)

	case "deleted":
		var ref store.URIMap
		if err := d.db.Where("cc_uri = ? AND ref_type = ?", ev.URI, "outbound-follow").First(&ref).Error; err != nil {
			return
		}
		if err := d.repos.DeleteRecord(ctx, entity.DID, ref.Collection, ref.Rkey); err != nil {
			slog.Error("failed to delete follow record", "atUri", ref.AtURI, "error", err)
		}
		d.db.Where("ccid = ? AND follow_rkey = ?", entity.CCID, ref.Rkey).Delete(&store.Follow{})
		d.db.Delete(&ref)
		slog.Info("unfollowed", "ccid", entity.CCID, "atUri", ref.AtURI)
	}
}

// handleAssociationEvent exports likes/reactions on mirrored bsky posts.
func (d *Daemon) handleAssociationEvent(ctx context.Context, ev concrnt.Event) {
	if ev.Association == nil {
		return
	}
	ccfs := *ev.Association

	assoc, err := d.resolveEventDocument(ctx, ev, ccfs)
	if err != nil {
		slog.Error("failed to resolve association", "uri", ccfs, "error", err)
		return
	}
	if assoc.Schema != world.SchemaLike && assoc.Schema != world.SchemaReaction {
		return
	}

	var liker store.Entity
	if err := d.db.Where("ccid = ?", assoc.Author).First(&liker).Error; err != nil {
		return // liker is not bridged
	}
	if !liker.Enabled {
		return
	}

	// The like target must be a mirrored bsky record.
	target, err := core.GetDocument[world.AtprotoRecord](ctx, d.core, ev.URI)
	if err != nil || target.Schema != world.SchemaAtprotoRecord || target.Value.AtURI == "" {
		return
	}

	// Reactions degrade to likes; skip if this user already liked the post.
	var count int64
	d.db.Model(&store.URIMap{}).
		Where("ref_type = ? AND did = ? AND meta->>'subject' = ?", "outbound-like", liker.DID, target.Value.AtURI).
		Count(&count)
	if count > 0 {
		return
	}

	like := tobsky.BuildLike(tobsky.StrongRef{URI: target.Value.AtURI, CID: target.Value.CID}, assoc.CreatedAt)
	atURI, cid, err := d.repos.CreateRecord(ctx, liker.DID, "app.bsky.feed.like", like)
	if err != nil {
		slog.Error("failed to create like", "subject", target.Value.AtURI, "error", err)
		return
	}
	d.saveURIMap(atURI, ccfs, liker.DID, cid, "outbound-like", map[string]string{"subject": target.Value.AtURI})

	d.mu.Lock()
	d.outboundLikes[ccfs] = true
	d.mu.Unlock()
}

// handleAssociationDeleted undoes an exported like.
func (d *Daemon) handleAssociationDeleted(ctx context.Context, ev concrnt.Event) {
	d.mu.RLock()
	known := d.outboundLikes[ev.URI]
	d.mu.RUnlock()
	if !known {
		return
	}

	var ref store.URIMap
	if err := d.db.Where("cc_uri = ? AND ref_type = ?", ev.URI, "outbound-like").First(&ref).Error; err != nil {
		d.mu.Lock()
		delete(d.outboundLikes, ev.URI)
		d.mu.Unlock()
		return
	}
	if err := d.repos.DeleteRecord(ctx, ref.DID, ref.Collection, ref.Rkey); err != nil {
		slog.Error("failed to delete like record", "atUri", ref.AtURI, "error", err)
		return
	}
	d.db.Delete(&ref)
	d.mu.Lock()
	delete(d.outboundLikes, ev.URI)
	d.mu.Unlock()
}
