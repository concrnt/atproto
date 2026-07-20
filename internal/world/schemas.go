// Package world holds concrnt schema URL constants and pure predicates over
// documents. It must stay free of config, DB and network dependencies so the
// conversion logic on top of it is unit-testable.
package world

import "strings"

const (
	SchemaMarkdown  = "https://schema.concrnt.world/m/markdown.json"
	SchemaGFM       = "https://schema.concrnt.world/m/gfm.json"
	SchemaMFM       = "https://schema.concrnt.world/m/mfm.json"
	SchemaPlaintext = "https://schema.concrnt.world/m/plaintext.json"
	SchemaMedia     = "https://schema.concrnt.world/m/media.json"
	SchemaReply     = "https://schema.concrnt.world/m/reply.json"
	SchemaReroute   = "https://schema.concrnt.world/m/reroute.json"
	SchemaAPNote    = "https://schema.concrnt.world/ap/note.json"
	SchemaLike      = "https://schema.concrnt.world/a/like.json"
	SchemaReaction  = "https://schema.concrnt.world/a/reaction.json"
	SchemaReference = "https://schema.concrnt.net/reference.json"
	SchemaDelete    = "https://schema.concrnt.net/delete.json"

	// Schemas introduced by this bridge.
	SchemaAtprotoRecord       = "https://schema.concrnt.world/atproto/record.json"
	SchemaAtprotoFollow       = "https://schema.concrnt.world/atproto/follow.json"
	SchemaAtprotoFollowNotify = "https://schema.concrnt.world/atproto/follow-notify.json"
)

// Namespace used for keys this bridge writes into concrnt.
const (
	InboxTimelineKey      = "atproto.concrnt.world/inbox"
	InboxKeyPrefix        = InboxTimelineKey + "/"
	FollowsKeyPrefix      = "atproto.concrnt.world/follows/"
	FollowNotifyKeyPrefix = "atproto.concrnt.world/follow-notify/"
)

// Body-bearing message value shared by markdown/media/reply/reroute schemas.
type MessageValue struct {
	Body     string            `json:"body"`
	Title    *string           `json:"title,omitempty"`
	Details  *string           `json:"details,omitempty"`
	Emojis   map[string]string `json:"emojis,omitempty"`
	Medias   []Media           `json:"medias,omitempty"`
	Mentions []string          `json:"mentions,omitempty"`
	// reply / reroute
	TargetURI       string           `json:"targetURI,omitempty"`
	ProfileOverride *ProfileOverride `json:"profileOverride,omitempty"`
}

type Media struct {
	MediaURL     string `json:"mediaURL"`
	MediaType    string `json:"mediaType"`
	ThumbnailURL string `json:"thumbnailURL,omitempty"`
	Blurhash     string `json:"blurhash,omitempty"`
	Flag         string `json:"flag,omitempty"`
}

type ProfileOverride struct {
	Username        string `json:"username,omitempty"`
	Avatar          string `json:"avatar,omitempty"`
	Description     string `json:"description,omitempty"`
	Link            string `json:"link,omitempty"`
	CharacterID     string `json:"characterID,omitempty"`
	ProfileOverride string `json:"profileOverride,omitempty"`
}

// AtprotoRecord is the value of SchemaAtprotoRecord: a lightweight reference
// to a Bluesky record, resolved by clients at render time (mirrors ap/note.json).
type AtprotoRecord struct {
	DID             string           `json:"did"`
	AtURI           string           `json:"atUri"`
	CID             string           `json:"cid"`
	ProfileOverride *ProfileOverride `json:"profileOverride,omitempty"`
}

// AtprotoFollow is the value of SchemaAtprotoFollow, committed by a user into
// their own cckv space to declare a follow of a Bluesky account.
type AtprotoFollow struct {
	DID string `json:"did"`
}

// AtprotoFollowNotify is the value of SchemaAtprotoFollowNotify, delivered to
// a bridged user's notify timeline when a Bluesky account follows them.
type AtprotoFollowNotify struct {
	DID             string           `json:"did"`
	ProfileOverride *ProfileOverride `json:"profileOverride,omitempty"`
}

// LikeValue is the value of a/like.json and a/reaction.json associations.
type LikeValue struct {
	Shortcode       string           `json:"shortcode,omitempty"`
	ImageURL        string           `json:"imageUrl,omitempty"`
	ProfileOverride *ProfileOverride `json:"profileOverride,omitempty"`
}

// ProfileValue mirrors concrnt.world/profiles/main (p/main.json).
type ProfileValue struct {
	Username    string `json:"username,omitempty"`
	Description string `json:"description,omitempty"`
	Avatar      string `json:"avatar,omitempty"`
	Banner      string `json:"banner,omitempty"`
}

// IsPlainReroute reports whether a reroute document carries no body,
// i.e. it federates as a plain repost rather than a quote post.
func IsPlainReroute(schema string, body string) bool {
	return schema == SchemaReroute && strings.TrimSpace(body) == ""
}

// IsMessageSchema reports whether the schema is a body-bearing post schema
// this bridge exports to Bluesky.
func IsMessageSchema(schema string) bool {
	switch schema {
	case SchemaMarkdown, SchemaGFM, SchemaMFM, SchemaPlaintext,
		SchemaMedia, SchemaReply, SchemaReroute:
		return true
	}
	return false
}

// IsBridgedSchema reports whether the document originates from a bridge
// (this one or the ActivityPub one) and must never be re-exported.
func IsBridgedSchema(schema string) bool {
	return schema == SchemaAtprotoRecord || schema == SchemaAPNote
}
