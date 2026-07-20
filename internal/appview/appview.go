// Package appview is a thin client for the public Bluesky appview
// (public.api.bsky.app): profile lookups for profileOverride rendering and
// post hydration for clients resolving atproto/record.json references.
package appview

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	base string
	http *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		base: baseURL,
		http: &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *Client) get(ctx context.Context, method string, params url.Values, out any) error {
	u := fmt.Sprintf("%s/xrpc/%s?%s", c.base, method, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("appview %s: status %d: %s", method, resp.StatusCode, string(body))
	}
	return json.Unmarshal(body, out)
}

// Profile is the subset of app.bsky.actor.defs#profileViewDetailed we use.
type Profile struct {
	DID         string `json:"did"`
	Handle      string `json:"handle"`
	DisplayName string `json:"displayName"`
	Avatar      string `json:"avatar"`
	Description string `json:"description"`
}

func (c *Client) GetProfile(ctx context.Context, actor string) (*Profile, error) {
	params := url.Values{"actor": {actor}}
	var p Profile
	if err := c.get(ctx, "app.bsky.actor.getProfile", params, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// GetPosts hydrates up to 25 at-uris into full post views (raw JSON).
func (c *Client) GetPosts(ctx context.Context, uris []string) ([]json.RawMessage, error) {
	params := url.Values{}
	for _, u := range uris {
		params.Add("uris", u)
	}
	var resp struct {
		Posts []json.RawMessage `json:"posts"`
	}
	if err := c.get(ctx, "app.bsky.feed.getPosts", params, &resp); err != nil {
		return nil, err
	}
	return resp.Posts, nil
}
