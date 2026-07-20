// Package tobsky converts concrnt documents into Bluesky records. Everything
// in this package is a pure function over its inputs (no config, DB or
// network) so it can be unit-tested exhaustively.
package tobsky

import (
	"regexp"
	"strings"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/rivo/uniseg"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	extast "github.com/yuin/goldmark/extension/ast"

	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/text"
)

// MaxGraphemes is Bluesky's post length limit.
const MaxGraphemes = 300

type InlineImage struct {
	URL string
	Alt string
}

// PostParts is the flattened representation of a concrnt message body.
type PostParts struct {
	Text         string
	Facets       []*appbsky.RichtextFacet
	InlineImages []InlineImage
	Summary      string // content warning text, "" if none
	Truncated    bool
}

var (
	imageMdRe = regexp.MustCompile(`!\[([^\]]*)\]\(([^)\s]+)\)`)
	// Mirrors the ActivityPub bridge: only a leading <details> block counts
	// as a content warning.
	detailsRe = regexp.MustCompile(`(?s)^\s*<details>\s*(?:<summary>(.*?)</summary>)?(.*?)</details>(.*)$`)
	hashtagRe = regexp.MustCompile(`(^|\s)#([^\s#:;,.!?'"()\[\]]+)`)
)

var markdown = goldmark.New(
	goldmark.WithExtensions(extension.Linkify, extension.Strikethrough),
)

// BuildPostParts converts a message body into plain text plus facets.
// Byte offsets for facets are recorded while the text is being built, never
// searched for afterwards, so emoji and repeated substrings stay correct.
func BuildPostParts(body string, plaintext bool) PostParts {
	textSrc := body

	summary := ""
	if m := detailsRe.FindStringSubmatch(textSrc); m != nil {
		summary = strings.TrimSpace(m[1])
		if summary == "" {
			summary = "CW"
		}
		textSrc = strings.TrimSpace(m[2])
		if rest := strings.TrimSpace(m[3]); rest != "" {
			textSrc += "\n" + rest
		}
	}

	var images []InlineImage
	textSrc = imageMdRe.ReplaceAllStringFunc(textSrc, func(match string) string {
		sub := imageMdRe.FindStringSubmatch(match)
		images = append(images, InlineImage{Alt: sub[1], URL: sub[2]})
		return ""
	})
	textSrc = strings.TrimSpace(textSrc)

	var out string
	var facets []*appbsky.RichtextFacet
	if plaintext {
		out = textSrc
	} else {
		out, facets = flattenMarkdown(textSrc)
	}

	// Content warnings have no direct Bluesky equivalent; surface the
	// summary as a text prefix.
	if summary != "" {
		prefix := "[CW: " + summary + "]\n"
		shift := int64(len(prefix))
		for _, f := range facets {
			f.Index.ByteStart += shift
			f.Index.ByteEnd += shift
		}
		out = prefix + out
	}

	facets = append(facets, hashtagFacets(out)...)

	return PostParts{
		Text:         out,
		Facets:       facets,
		InlineImages: images,
		Summary:      summary,
	}
}

// flattenMarkdown renders markdown as plain text, emitting link facets whose
// byte ranges are recorded as the text is appended.
func flattenMarkdown(src string) (string, []*appbsky.RichtextFacet) {
	source := []byte(src)
	doc := markdown.Parser().Parse(text.NewReader(source))

	var b strings.Builder
	var facets []*appbsky.RichtextFacet

	addLinkFacet := func(start, end int, uri string) {
		if end <= start || uri == "" {
			return
		}
		facets = append(facets, &appbsky.RichtextFacet{
			Index: &appbsky.RichtextFacet_ByteSlice{ByteStart: int64(start), ByteEnd: int64(end)},
			Features: []*appbsky.RichtextFacet_Features_Elem{{
				RichtextFacet_Link: &appbsky.RichtextFacet_Link{Uri: uri},
			}},
		})
	}

	var walk func(n ast.Node)
	writeChildren := func(n ast.Node) {
		for c := n.FirstChild(); c != nil; c = c.NextSibling() {
			walk(c)
		}
	}
	endBlock := func() {
		s := b.String()
		if s != "" && !strings.HasSuffix(s, "\n\n") {
			if strings.HasSuffix(s, "\n") {
				b.WriteString("\n")
			} else {
				b.WriteString("\n\n")
			}
		}
	}

	walk = func(n ast.Node) {
		switch v := n.(type) {
		case *ast.Text:
			b.Write(v.Segment.Value(source))
			if v.SoftLineBreak() || v.HardLineBreak() {
				b.WriteString("\n")
			}
		case *ast.String:
			b.Write(v.Value)
		case *ast.Link:
			start := b.Len()
			writeChildren(v)
			if b.Len() == start {
				b.WriteString(string(v.Destination))
			}
			addLinkFacet(start, b.Len(), string(v.Destination))
		case *ast.AutoLink:
			url := string(v.URL(source))
			start := b.Len()
			b.WriteString(url)
			addLinkFacet(start, b.Len(), url)
		case *ast.Image:
			// Inline images are extracted by regex beforehand; render the
			// alt text of any that survive (e.g. reference-style syntax).
			writeChildren(v)
		case *ast.Paragraph, *ast.Heading:
			writeChildren(v)
			endBlock()
		case *ast.TextBlock:
			// Tight list items wrap their content in a TextBlock; keep them
			// to a single line instead of a paragraph break.
			writeChildren(v)
			if !strings.HasSuffix(b.String(), "\n") {
				b.WriteString("\n")
			}
		case *ast.ListItem:
			b.WriteString("- ")
			writeChildren(v)
			s := b.String()
			if !strings.HasSuffix(s, "\n") {
				b.WriteString("\n")
			}
		case *ast.List:
			writeChildren(v)
			endBlock()
		case *ast.Blockquote:
			writeChildren(v)
		case *ast.FencedCodeBlock:
			for i := range v.Lines().Len() {
				line := v.Lines().At(i)
				b.Write(line.Value(source))
			}
			endBlock()
		case *ast.CodeBlock:
			for i := range v.Lines().Len() {
				line := v.Lines().At(i)
				b.Write(line.Value(source))
			}
			endBlock()
		case *ast.CodeSpan:
			writeChildren(v)
		case *ast.RawHTML, *ast.HTMLBlock:
			// strip raw html
		case *ast.ThematicBreak:
			endBlock()
		case *extast.Strikethrough:
			writeChildren(v)
		default:
			writeChildren(n)
		}
	}
	writeChildren(doc)

	return strings.TrimSpace(b.String()), clampFacets(facets, len(strings.TrimSpace(b.String())))
}

// clampFacets drops facets that fall outside the final (trimmed) text.
// Leading trim never happens in practice (goldmark output starts with
// content), so only the right boundary matters.
func clampFacets(facets []*appbsky.RichtextFacet, textLen int) []*appbsky.RichtextFacet {
	out := facets[:0]
	for _, f := range facets {
		if f.Index.ByteStart >= int64(textLen) {
			continue
		}
		if f.Index.ByteEnd > int64(textLen) {
			f.Index.ByteEnd = int64(textLen)
		}
		out = append(out, f)
	}
	return out
}

// hashtagFacets finds #tags in the final text. Go regexp returns byte
// offsets, which is exactly what facets need.
func hashtagFacets(s string) []*appbsky.RichtextFacet {
	var facets []*appbsky.RichtextFacet
	for _, m := range hashtagRe.FindAllStringSubmatchIndex(s, -1) {
		tagStart, tagEnd := m[4], m[5]
		facets = append(facets, &appbsky.RichtextFacet{
			// The facet covers "#tag" including the hash sign.
			Index: &appbsky.RichtextFacet_ByteSlice{ByteStart: int64(tagStart - 1), ByteEnd: int64(tagEnd)},
			Features: []*appbsky.RichtextFacet_Features_Elem{{
				RichtextFacet_Tag: &appbsky.RichtextFacet_Tag{Tag: s[tagStart:tagEnd]},
			}},
		})
	}
	return facets
}

// EnforceLimit truncates parts.Text to MaxGraphemes, cutting on a grapheme
// boundary and appending an ellipsis. Facets that no longer fit are dropped
// or clipped.
func EnforceLimit(parts *PostParts) {
	if uniseg.GraphemeClusterCount(parts.Text) <= MaxGraphemes {
		return
	}

	g := uniseg.NewGraphemes(parts.Text)
	count := 0
	cut := len(parts.Text)
	for g.Next() {
		count++
		if count > MaxGraphemes-3 {
			start, _ := g.Positions()
			cut = start
			break
		}
	}

	parts.Text = strings.TrimRight(parts.Text[:cut], " \n") + "…"
	parts.Facets = clampFacets(parts.Facets, cut)
	parts.Truncated = true
}
