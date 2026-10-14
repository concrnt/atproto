// Package toconcrnt builds concrnt documents from Bluesky records. Pure
// functions only: DB lookups, network and commits happen in the inbound
// daemon.
package toconcrnt

import (
	"encoding/json"
	"time"

	concrnt "github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"

	"github.com/concrnt/atproto/internal/world"
)

// InboxKey deterministically derives the concrnt storage key for a bsky
// record, mirroring the ActivityPub bridge's URL-hash scheme. Deletes can
// re-derive the same key from the at-uri without a lookup table.
func InboxKey(serviceCCID, atURI string) string {
	return "cckv://" + serviceCCID + "/" + world.InboxKeyPrefix + cdid.MakeHash([]byte(atURI)).String()
}

// InboxTimeline is the per-user timeline that receives mirrored bsky posts.
func InboxTimeline(ccid string) string {
	return "cckv://" + ccid + "/" + world.InboxTimelineKey
}

// NotifyTimeline is the standard notification timeline of a concrnt user.
func NotifyTimeline(ccid string) string {
	return "cckv://" + ccid + "/concrnt.world/profiles/main/notify-timeline"
}

// BuildRecordDoc mirrors a bsky record as a lightweight reference
// (atproto/record.json: at-uri + cid only, like the AP bridge's ap/note.json),
// distributed to the given timelines.
func BuildRecordDoc(serviceCCID, atURI, cid string, distributes []string, createdAt time.Time) concrnt.Document[world.AtprotoRecord] {
	doc := concrnt.Document[world.AtprotoRecord]{
		Kind:   "record",
		Key:    InboxKey(serviceCCID, atURI),
		Schema: world.SchemaAtprotoRecord,
		Value: world.AtprotoRecord{
			AtURI: atURI,
			CID:   cid,
		},
		Author:    serviceCCID,
		CreatedAt: createdAt,
	}
	if len(distributes) > 0 {
		doc.Distributes = &distributes
	}
	return doc
}

// BuildRerouteDoc mirrors a bsky repost as m/reroute.json pointing at the
// mirrored subject record.
func BuildRerouteDoc(serviceCCID, repostAtURI, targetCcURI string, override *world.ProfileOverride, distributes []string, createdAt time.Time) concrnt.Document[world.MessageValue] {
	doc := concrnt.Document[world.MessageValue]{
		Kind:   "record",
		Key:    InboxKey(serviceCCID, repostAtURI),
		Schema: world.SchemaReroute,
		Value: world.MessageValue{
			TargetURI:       targetCcURI,
			ProfileOverride: override,
		},
		Author:    serviceCCID,
		CreatedAt: createdAt,
	}
	if len(distributes) > 0 {
		doc.Distributes = &distributes
	}
	return doc
}

// MainProfile is the main profile document of a concrnt user; follow
// notifications associate to it (there is no post to attach them to).
func MainProfile(ccid string) string {
	return "cckv://" + ccid + "/concrnt.world/profiles/main"
}

// buildTargetAssociation attaches a reply/reroute notification to a bridged
// user's post (targetCcURI); bodyKey is the mirrored record the notification
// points at. Associations have no key and, like the ActivityPub bridge's,
// no variant: the server dedupes them by (associate, author, schema, value).
func buildTargetAssociation(schema, serviceCCID, targetCcURI, bodyKey string, override *world.ProfileOverride, distributes []string, createdAt time.Time) concrnt.Document[world.TargetAssociationValue] {
	doc := concrnt.Document[world.TargetAssociationValue]{
		Kind:   "association",
		Schema: schema,
		Value: world.TargetAssociationValue{
			TargetURI:       bodyKey,
			ProfileOverride: override,
		},
		Author:    serviceCCID,
		CreatedAt: createdAt,
		Associate: &targetCcURI,
	}
	if len(distributes) > 0 {
		doc.Distributes = &distributes
	}
	return doc
}

// BuildReplyAssociation notifies the author of targetCcURI that a mirrored
// bsky post (bodyKey) replied to it (a/reply.json, as the AP bridge does).
func BuildReplyAssociation(serviceCCID, targetCcURI, bodyKey string, override *world.ProfileOverride, distributes []string, createdAt time.Time) concrnt.Document[world.TargetAssociationValue] {
	return buildTargetAssociation(world.SchemaReplyAssociation, serviceCCID, targetCcURI, bodyKey, override, distributes, createdAt)
}

// BuildRerouteAssociation notifies the author of targetCcURI that a mirrored
// bsky repost or quote (bodyKey) rerouted it (a/reroute.json).
func BuildRerouteAssociation(serviceCCID, targetCcURI, bodyKey string, override *world.ProfileOverride, distributes []string, createdAt time.Time) concrnt.Document[world.TargetAssociationValue] {
	return buildTargetAssociation(world.SchemaRerouteAssociation, serviceCCID, targetCcURI, bodyKey, override, distributes, createdAt)
}

// BuildFollowNotifyAssociation notifies a bridged user that a Bluesky account
// followed them: an atproto/follow-notify.json association on their main
// profile (profileURI), distributed to their notify timeline. No follower
// state is materialized anywhere; this document is the whole representation.
func BuildFollowNotifyAssociation(serviceCCID, followerDID, profileURI string, distributes []string, createdAt time.Time) concrnt.Document[world.AtprotoFollowNotify] {
	doc := concrnt.Document[world.AtprotoFollowNotify]{
		Kind:   "association",
		Schema: world.SchemaAtprotoFollowNotify,
		Value: world.AtprotoFollowNotify{
			DID: followerDID,
		},
		Author:    serviceCCID,
		CreatedAt: createdAt,
		Associate: &profileURI,
	}
	if len(distributes) > 0 {
		doc.Distributes = &distributes
	}
	return doc
}

// BuildLikeAssociation mirrors a bsky like as an a/like.json association on
// the target concrnt resource.
func BuildLikeAssociation(serviceCCID, targetCcURI string, override *world.ProfileOverride, distributes []string, createdAt time.Time) concrnt.Document[world.LikeValue] {
	variant := ""
	doc := concrnt.Document[world.LikeValue]{
		Kind:   "association",
		Schema: world.SchemaLike,
		Value: world.LikeValue{
			ProfileOverride: override,
		},
		Author:             serviceCCID,
		CreatedAt:          createdAt,
		Associate:          &targetCcURI,
		AssociationVariant: &variant,
	}
	if len(distributes) > 0 {
		doc.Distributes = &distributes
	}
	return doc
}

// BuildDeleteDoc removes a previously mirrored resource; value is the target
// URI, matching the ActivityPub bridge's delete commits.
func BuildDeleteDoc(serviceCCID, targetURI string) concrnt.Document[string] {
	return concrnt.Document[string]{
		Kind:      "delete",
		Schema:    world.SchemaDelete,
		Value:     targetURI,
		Author:    serviceCCID,
		CreatedAt: time.Now().UTC(),
	}
}

// PostRefSet is the structured form of a post's interactions: the reply
// parent and root, and the quoted record (embed.record / recordWithMedia).
// Each is an at-uri or empty. Notifications use Parent (the post actually
// replied to, as the ActivityPub bridge uses inReplyTo) and Quote; Root only
// feeds the flat PostRefs listing.
type PostRefSet struct {
	Parent string
	Root   string
	Quote  string
}

// ParsePostRefs extracts the reply parent/root and quoted record at-uris of
// a post record.
func ParsePostRefs(record map[string]any) PostRefSet {
	var refs PostRefSet
	b, err := json.Marshal(record)
	if err != nil {
		return refs
	}
	var post struct {
		Reply *struct {
			Parent struct {
				URI string `json:"uri"`
			} `json:"parent"`
			Root struct {
				URI string `json:"uri"`
			} `json:"root"`
		} `json:"reply"`
		Embed *struct {
			Type   string `json:"$type"`
			Record *struct {
				URI string `json:"uri"`
				// recordWithMedia nests one level deeper
				Record *struct {
					URI string `json:"uri"`
				} `json:"record"`
			} `json:"record"`
		} `json:"embed"`
	}
	if err := json.Unmarshal(b, &post); err != nil {
		return refs
	}
	if post.Reply != nil {
		refs.Parent = post.Reply.Parent.URI
		refs.Root = post.Reply.Root.URI
	}
	if post.Embed != nil && post.Embed.Record != nil {
		if post.Embed.Record.URI != "" {
			refs.Quote = post.Embed.Record.URI
		} else if post.Embed.Record.Record != nil {
			refs.Quote = post.Embed.Record.Record.URI
		}
	}
	return refs
}

// PostRefs extracts the at-uris a post interacts with: reply parent/root and
// quoted records. Used to detect interactions with bridged accounts.
func PostRefs(record map[string]any) []string {
	var refs []string
	set := ParsePostRefs(record)
	if set.Parent != "" {
		refs = append(refs, set.Parent)
	}
	if set.Root != "" && set.Root != set.Parent {
		refs = append(refs, set.Root)
	}
	if set.Quote != "" {
		refs = append(refs, set.Quote)
	}
	return refs
}

// SubjectURI extracts the subject at-uri of a like/repost record.
func SubjectURI(record map[string]any) string {
	subj, ok := record["subject"].(map[string]any)
	if !ok {
		return ""
	}
	uri, _ := subj["uri"].(string)
	return uri
}

// SubjectCID extracts the subject cid of a like/repost record.
func SubjectCID(record map[string]any) string {
	subj, ok := record["subject"].(map[string]any)
	if !ok {
		return ""
	}
	c, _ := subj["cid"].(string)
	return c
}
