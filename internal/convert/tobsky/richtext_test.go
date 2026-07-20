package tobsky

import (
	"strings"
	"testing"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/rivo/uniseg"
)

func facetText(t *testing.T, text string, f *appbsky.RichtextFacet) string {
	t.Helper()
	if f.Index.ByteStart < 0 || f.Index.ByteEnd > int64(len(text)) || f.Index.ByteStart >= f.Index.ByteEnd {
		t.Fatalf("facet range out of bounds: [%d, %d) in %q", f.Index.ByteStart, f.Index.ByteEnd, text)
	}
	return text[f.Index.ByteStart:f.Index.ByteEnd]
}

func TestPlainText(t *testing.T) {
	parts := BuildPostParts("hello world", false)
	if parts.Text != "hello world" {
		t.Errorf("got %q", parts.Text)
	}
	if len(parts.Facets) != 0 {
		t.Errorf("unexpected facets: %v", parts.Facets)
	}
}

func TestMarkdownLinkFacet(t *testing.T) {
	parts := BuildPostParts("check [my site](https://example.com) out", false)
	if parts.Text != "check my site out" {
		t.Errorf("got %q", parts.Text)
	}
	if len(parts.Facets) != 1 {
		t.Fatalf("expected 1 facet, got %d", len(parts.Facets))
	}
	if got := facetText(t, parts.Text, parts.Facets[0]); got != "my site" {
		t.Errorf("facet covers %q", got)
	}
	if uri := parts.Facets[0].Features[0].RichtextFacet_Link.Uri; uri != "https://example.com" {
		t.Errorf("facet uri %q", uri)
	}
}

// Facet offsets must be UTF-8 byte offsets even with emoji/CJK before the link.
func TestLinkFacetAfterEmoji(t *testing.T) {
	parts := BuildPostParts("絵文字🎉のあと [リンク](https://example.jp) です", false)
	if len(parts.Facets) != 1 {
		t.Fatalf("expected 1 facet, got %d (%q)", len(parts.Facets), parts.Text)
	}
	if got := facetText(t, parts.Text, parts.Facets[0]); got != "リンク" {
		t.Errorf("facet covers %q", got)
	}
}

func TestAutolink(t *testing.T) {
	parts := BuildPostParts("see https://example.com/path please", false)
	if len(parts.Facets) != 1 {
		t.Fatalf("expected 1 facet, got %d (%q)", len(parts.Facets), parts.Text)
	}
	if got := facetText(t, parts.Text, parts.Facets[0]); got != "https://example.com/path" {
		t.Errorf("facet covers %q", got)
	}
}

func TestHashtagFacet(t *testing.T) {
	parts := BuildPostParts("こんにちは #concrnt と #テスト🎉 です", false)
	var tags []string
	for _, f := range parts.Facets {
		if f.Features[0].RichtextFacet_Tag != nil {
			tags = append(tags, f.Features[0].RichtextFacet_Tag.Tag)
			cover := facetText(t, parts.Text, f)
			if !strings.HasPrefix(cover, "#") {
				t.Errorf("tag facet should cover the # sign, covers %q", cover)
			}
		}
	}
	if len(tags) != 2 || tags[0] != "concrnt" {
		t.Errorf("got tags %v", tags)
	}
}

func TestInlineImageExtraction(t *testing.T) {
	parts := BuildPostParts("look ![alt text](https://example.com/img.png) nice", false)
	if len(parts.InlineImages) != 1 {
		t.Fatalf("expected 1 image, got %d", len(parts.InlineImages))
	}
	if parts.InlineImages[0].URL != "https://example.com/img.png" || parts.InlineImages[0].Alt != "alt text" {
		t.Errorf("got %+v", parts.InlineImages[0])
	}
	if strings.Contains(parts.Text, "img.png") {
		t.Errorf("image url leaked into text: %q", parts.Text)
	}
}

func TestContentWarning(t *testing.T) {
	parts := BuildPostParts("<details><summary>spoiler</summary>hidden body</details>", false)
	if parts.Summary != "spoiler" {
		t.Errorf("summary %q", parts.Summary)
	}
	if !strings.HasPrefix(parts.Text, "[CW: spoiler]") {
		t.Errorf("text should carry CW prefix: %q", parts.Text)
	}
	if !strings.Contains(parts.Text, "hidden body") {
		t.Errorf("body missing: %q", parts.Text)
	}
}

func TestCWShiftsFacets(t *testing.T) {
	parts := BuildPostParts("<details><summary>cw</summary>[link](https://example.com)</details>", false)
	if len(parts.Facets) != 1 {
		t.Fatalf("expected 1 facet, got %d (%q)", len(parts.Facets), parts.Text)
	}
	if got := facetText(t, parts.Text, parts.Facets[0]); got != "link" {
		t.Errorf("facet covers %q", got)
	}
}

func TestTruncationOnGraphemeBoundary(t *testing.T) {
	// 400 flag emoji (regional indicator pairs) — grapheme-safe cutting
	// must not split a pair.
	flag := "🇯🇵"
	parts := BuildPostParts(strings.Repeat(flag, 400), false)
	EnforceLimit(&parts)
	if !parts.Truncated {
		t.Fatal("expected truncation")
	}
	if n := uniseg.GraphemeClusterCount(parts.Text); n > MaxGraphemes {
		t.Errorf("still %d graphemes", n)
	}
	if !strings.HasSuffix(parts.Text, "…") {
		t.Errorf("missing ellipsis: %q", parts.Text[len(parts.Text)-8:])
	}
	// All remaining flags must be intact (each is 8 bytes).
	body := strings.TrimSuffix(parts.Text, "…")
	if len(body)%len(flag) != 0 {
		t.Errorf("flag emoji split by truncation")
	}
}

func TestNoTruncationUnderLimit(t *testing.T) {
	parts := BuildPostParts(strings.Repeat("あ", 300), false)
	EnforceLimit(&parts)
	if parts.Truncated {
		t.Error("300 graphemes must not be truncated")
	}
}

func TestTruncationDropsOutOfRangeFacets(t *testing.T) {
	body := strings.Repeat("x", 400) + " [tail](https://example.com/tail)"
	parts := BuildPostParts(body, false)
	EnforceLimit(&parts)
	for _, f := range parts.Facets {
		if f.Index.ByteEnd > int64(len(parts.Text)) {
			t.Errorf("facet exceeds text after truncation")
		}
	}
}

func TestListRendering(t *testing.T) {
	parts := BuildPostParts("intro\n\n- one\n- two", false)
	if !strings.Contains(parts.Text, "- one\n- two") {
		t.Errorf("got %q", parts.Text)
	}
}

func TestPlaintextSchemaSkipsMarkdown(t *testing.T) {
	parts := BuildPostParts("*not emphasis*", true)
	if parts.Text != "*not emphasis*" {
		t.Errorf("plaintext must be untouched, got %q", parts.Text)
	}
}
