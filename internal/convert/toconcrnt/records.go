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
// (atproto/record.json), distributed to the given timelines.
func BuildRecordDoc(serviceCCID, did, atURI, cid string, override *world.ProfileOverride, distributes []string, createdAt time.Time) concrnt.Document[world.AtprotoRecord] {
	doc := concrnt.Document[world.AtprotoRecord]{
		Kind:   "record",
		Key:    InboxKey(serviceCCID, atURI),
		Schema: world.SchemaAtprotoRecord,
		Value: world.AtprotoRecord{
			DID:             did,
			AtURI:           atURI,
			CID:             cid,
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

// FollowNotifyKey derives a stable key per (follower, subject) pair so a
// refollow upserts the same record instead of accumulating documents.
func FollowNotifyKey(serviceCCID, followerDID, subjectDID string) string {
	return "cckv://" + serviceCCID + "/" + world.FollowNotifyKeyPrefix + cdid.MakeHash([]byte(followerDID+"->"+subjectDID)).String()
}

// BuildFollowNotifyDoc notifies a bridged user that a Bluesky account followed
// them. No follower state is materialized anywhere; this document is the whole
// representation.
func BuildFollowNotifyDoc(serviceCCID, followerDID, subjectDID string, override *world.ProfileOverride, distributes []string, createdAt time.Time) concrnt.Document[world.AtprotoFollowNotify] {
	doc := concrnt.Document[world.AtprotoFollowNotify]{
		Kind:   "record",
		Key:    FollowNotifyKey(serviceCCID, followerDID, subjectDID),
		Schema: world.SchemaAtprotoFollowNotify,
		Value: world.AtprotoFollowNotify{
			DID:             followerDID,
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

// PostRefs extracts the at-uris a post interacts with: reply parent/root and
// quoted records. Used to detect interactions with bridged accounts.
func PostRefs(record map[string]any) []string {
	var refs []string
	b, err := json.Marshal(record)
	if err != nil {
		return nil
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
		return nil
	}
	if post.Reply != nil {
		if post.Reply.Parent.URI != "" {
			refs = append(refs, post.Reply.Parent.URI)
		}
		if post.Reply.Root.URI != "" && post.Reply.Root.URI != post.Reply.Parent.URI {
			refs = append(refs, post.Reply.Root.URI)
		}
	}
	if post.Embed != nil && post.Embed.Record != nil {
		if post.Embed.Record.URI != "" {
			refs = append(refs, post.Embed.Record.URI)
		} else if post.Embed.Record.Record != nil && post.Embed.Record.Record.URI != "" {
			refs = append(refs, post.Embed.Record.Record.URI)
		}
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
