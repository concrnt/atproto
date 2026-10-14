package tobsky

import (
	"testing"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
)

func testBlob(t *testing.T, mime string) *lexutil.LexBlob {
	t.Helper()
	c, err := cid.Parse("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")
	if err != nil {
		t.Fatal(err)
	}
	return &lexutil.LexBlob{Ref: lexutil.LexLink(c), MimeType: mime, Size: 1}
}

func TestAssemblePostVideoEmbed(t *testing.T) {
	video := testBlob(t, "video/mp4")
	post := AssemblePost(PostParts{Text: "clip"}, PostOptions{CreatedAt: time.Now(), Video: video})
	if post.Embed == nil || post.Embed.EmbedVideo == nil {
		t.Fatalf("embed = %+v, want app.bsky.embed.video", post.Embed)
	}
	if post.Embed.EmbedVideo.LexiconTypeID != "app.bsky.embed.video" || post.Embed.EmbedVideo.Video != video {
		t.Fatalf("video embed = %+v", post.Embed.EmbedVideo)
	}
}

func TestAssemblePostVideoWithQuote(t *testing.T) {
	video := testBlob(t, "video/mp4")
	quote := &StrongRef{URI: "at://did:plc:q/app.bsky.feed.post/1", CID: "bafyq"}
	post := AssemblePost(PostParts{Text: "clip"}, PostOptions{CreatedAt: time.Now(), Video: video, Quote: quote})
	rwm := post.Embed.EmbedRecordWithMedia
	if rwm == nil || rwm.Media == nil || rwm.Media.EmbedVideo == nil {
		t.Fatalf("embed = %+v, want recordWithMedia carrying a video", post.Embed)
	}
	if rwm.Record.Record.Uri != quote.URI {
		t.Fatalf("quote uri = %q", rwm.Record.Record.Uri)
	}
}

func TestAssemblePostVideoBeatsImages(t *testing.T) {
	video := testBlob(t, "video/mp4")
	img := &appbsky.EmbedImages_Image{Image: testBlob(t, "image/png")}
	post := AssemblePost(PostParts{Text: "both"}, PostOptions{CreatedAt: time.Now(), Video: video, Images: []*appbsky.EmbedImages_Image{img}})
	if post.Embed.EmbedVideo == nil || post.Embed.EmbedImages != nil {
		t.Fatalf("embed = %+v, want video only", post.Embed)
	}
}

func TestAssemblePostImagesUnchanged(t *testing.T) {
	img := &appbsky.EmbedImages_Image{Image: testBlob(t, "image/png")}
	post := AssemblePost(PostParts{Text: "pic"}, PostOptions{CreatedAt: time.Now(), Images: []*appbsky.EmbedImages_Image{img}})
	if post.Embed == nil || post.Embed.EmbedImages == nil || len(post.Embed.EmbedImages.Images) != 1 {
		t.Fatalf("embed = %+v, want images", post.Embed)
	}
}
