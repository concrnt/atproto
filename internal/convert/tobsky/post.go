package tobsky

import (
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/util"
)

// StrongRef points at a bsky record (uri + cid).
type StrongRef struct {
	URI string
	CID string
}

func (r StrongRef) lex() *comatproto.RepoStrongRef {
	return &comatproto.RepoStrongRef{Uri: r.URI, Cid: r.CID}
}

// PostOptions carries everything the caller resolved externally: ingested
// image blobs, reply refs, the quoted record, and the fallback external link
// used when the text was truncated.
type PostOptions struct {
	CreatedAt time.Time
	Images    []*appbsky.EmbedImages_Image
	Reply     *ReplyRefs
	Quote     *StrongRef
	// TruncatedLink renders an external card linking back to the full post on
	// concrnt. Only used when parts.Truncated and no other embed competes.
	TruncatedLink *ExternalLink
}

type ReplyRefs struct {
	Root   StrongRef
	Parent StrongRef
}

type ExternalLink struct {
	URI         string
	Title       string
	Description string
}

// AssemblePost builds the final app.bsky.feed.post record. Embed precedence:
// images > quote > truncation link (images and quotes are the post's own
// content; the truncation link is only a courtesy pointer).
func AssemblePost(parts PostParts, opts PostOptions) *appbsky.FeedPost {
	post := &appbsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          parts.Text,
		Facets:        parts.Facets,
		CreatedAt:     opts.CreatedAt.UTC().Format(util.ISO8601),
	}

	if opts.Reply != nil {
		post.Reply = &appbsky.FeedPost_ReplyRef{
			Root:   opts.Reply.Root.lex(),
			Parent: opts.Reply.Parent.lex(),
		}
	}

	switch {
	case len(opts.Images) > 0 && opts.Quote != nil:
		post.Embed = &appbsky.FeedPost_Embed{
			EmbedRecordWithMedia: &appbsky.EmbedRecordWithMedia{
				LexiconTypeID: "app.bsky.embed.recordWithMedia",
				Record: &appbsky.EmbedRecord{
					LexiconTypeID: "app.bsky.embed.record",
					Record:        opts.Quote.lex(),
				},
				Media: &appbsky.EmbedRecordWithMedia_Media{
					EmbedImages: &appbsky.EmbedImages{
						LexiconTypeID: "app.bsky.embed.images",
						Images:        opts.Images,
					},
				},
			},
		}
	case len(opts.Images) > 0:
		post.Embed = &appbsky.FeedPost_Embed{
			EmbedImages: &appbsky.EmbedImages{
				LexiconTypeID: "app.bsky.embed.images",
				Images:        opts.Images,
			},
		}
	case opts.Quote != nil:
		post.Embed = &appbsky.FeedPost_Embed{
			EmbedRecord: &appbsky.EmbedRecord{
				LexiconTypeID: "app.bsky.embed.record",
				Record:        opts.Quote.lex(),
			},
		}
	case parts.Truncated && opts.TruncatedLink != nil:
		post.Embed = &appbsky.FeedPost_Embed{
			EmbedExternal: &appbsky.EmbedExternal{
				LexiconTypeID: "app.bsky.embed.external",
				External: &appbsky.EmbedExternal_External{
					Uri:         opts.TruncatedLink.URI,
					Title:       opts.TruncatedLink.Title,
					Description: opts.TruncatedLink.Description,
				},
			},
		}
	}

	return post
}

// BuildLike builds an app.bsky.feed.like record.
func BuildLike(subject StrongRef, createdAt time.Time) *appbsky.FeedLike {
	return &appbsky.FeedLike{
		LexiconTypeID: "app.bsky.feed.like",
		Subject:       subject.lex(),
		CreatedAt:     createdAt.UTC().Format(util.ISO8601),
	}
}

// BuildRepost builds an app.bsky.feed.repost record.
func BuildRepost(subject StrongRef, createdAt time.Time) *appbsky.FeedRepost {
	return &appbsky.FeedRepost{
		LexiconTypeID: "app.bsky.feed.repost",
		Subject:       subject.lex(),
		CreatedAt:     createdAt.UTC().Format(util.ISO8601),
	}
}

// BuildFollow builds an app.bsky.graph.follow record.
func BuildFollow(subjectDID string, createdAt time.Time) *appbsky.GraphFollow {
	return &appbsky.GraphFollow{
		LexiconTypeID: "app.bsky.graph.follow",
		Subject:       subjectDID,
		CreatedAt:     createdAt.UTC().Format(util.ISO8601),
	}
}

// BuildProfile builds an app.bsky.actor.profile record from concrnt profile
// fields. avatar may be nil.
func BuildProfile(displayName, description string, avatar *lexutil.LexBlob) *appbsky.ActorProfile {
	p := &appbsky.ActorProfile{
		LexiconTypeID: "app.bsky.actor.profile",
	}
	if displayName != "" {
		p.DisplayName = &displayName
	}
	if description != "" {
		p.Description = &description
	}
	if avatar != nil {
		p.Avatar = avatar
	}
	return p
}
