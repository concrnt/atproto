package world

import (
	"encoding/json"
	"testing"
)

// world-app writes emojis as {shortcode: {imageURL, animURL}} (m/markdown.json);
// a value that fails to parse is silently not bridged.
func TestMessageValueParsesEmojiObjects(t *testing.T) {
	raw := `{"body":"hi :wave:","emojis":{"wave":{"imageURL":"https://example.com/wave.png","animURL":"https://example.com/wave.gif"}}}`
	var v MessageValue
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatal(err)
	}
	if v.Emojis["wave"].ImageURL != "https://example.com/wave.png" || v.Emojis["wave"].AnimURL != "https://example.com/wave.gif" {
		t.Fatalf("emojis %+v", v.Emojis)
	}
}
