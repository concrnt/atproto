package pds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/auth"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/keys"
)

// Proxy forwards authenticated XRPC calls that the bridge does not serve
// itself (app.bsky.*, chat.bsky.*, anything with an `atproto-proxy` header)
// to the named service, signed with the account's repo key as an
// inter-service JWT. This is what lets the official Bluesky app, logged in
// here as its PDS, read its timeline and its DMs.
type Proxy struct {
	dir        identity.Directory
	appviewDID string
	chatDID    string
	keys       *keys.Service
	http       *http.Client
	userAgent  string
}

const (
	proxyMaxBody    = 1 << 20 // DMs and mutes are small; blobs never reach the proxy
	proxyJWTTTL     = time.Minute
	proxyHeaderName = "atproto-proxy"
)

func NewProxy(dir identity.Directory, appviewDID, chatDID string, ks *keys.Service, version string) *Proxy {
	return &Proxy{
		dir:        dir,
		appviewDID: appviewDID,
		chatDID:    chatDID,
		keys:       ks,
		http:       &http.Client{Timeout: 30 * time.Second},
		userAgent:  "concrnt-atproto-bridge/" + version,
	}
}

// defaultTarget picks the service for a method when the client sent no
// atproto-proxy header; "" means the method is not proxied.
func (p *Proxy) defaultTarget(nsid string) string {
	switch {
	case strings.HasPrefix(nsid, "chat.bsky."):
		return p.chatDID + "#bsky_chat"
	case strings.HasPrefix(nsid, "app.bsky."):
		return p.appviewDID + "#bsky_appview"
	}
	return ""
}

var errUnknownService = errors.New("unknown proxy service")

// resolve turns "did#fragment" into (service DID, https endpoint).
func (p *Proxy) resolve(ctx context.Context, target string) (string, string, error) {
	didStr, frag, ok := strings.Cut(target, "#")
	if !ok || frag == "" {
		return "", "", fmt.Errorf("atproto-proxy must be did#service")
	}
	did, err := syntax.ParseDID(didStr)
	if err != nil {
		return "", "", fmt.Errorf("atproto-proxy: %w", err)
	}
	ident, err := p.dir.LookupDID(ctx, did)
	if err != nil {
		return "", "", fmt.Errorf("resolve %s: %w", did, err)
	}
	endpoint := ident.GetServiceEndpoint(frag)
	if endpoint == "" {
		return "", "", errUnknownService
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return "", "", fmt.Errorf("service endpoint %q is not https", endpoint)
	}
	return did.String(), strings.TrimSuffix(endpoint, "/"), nil
}

// Forward relays c's request for method nsid on behalf of did. It writes
// the response itself; the returned error is only for echo's logging.
func (p *Proxy) Forward(c echo.Context, did, nsid, target string) error {
	ctx := c.Request().Context()
	if target == "" {
		target = p.defaultTarget(nsid)
	}
	if target == "" {
		return xrpcError(c, http.StatusNotImplemented, "MethodNotImplemented", "no proxy target for "+nsid)
	}
	method, err := syntax.ParseNSID(nsid)
	if err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "invalid method NSID")
	}
	serviceDID, endpoint, err := p.resolve(ctx, target)
	if errors.Is(err, errUnknownService) {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "unknown proxy service "+target)
	}
	if err != nil {
		return xrpcError(c, http.StatusBadGateway, "UpstreamFailure", err.Error())
	}

	priv, err := p.keys.SigningKey(did)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", "signing key unavailable")
	}
	token, err := auth.SignServiceAuth(syntax.DID(did), serviceDID, proxyJWTTTL, &method, priv)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}

	in := c.Request()
	var body io.Reader
	var bodyLen int64
	if in.Body != nil && in.Method != http.MethodGet {
		buf, err := io.ReadAll(io.LimitReader(in.Body, proxyMaxBody+1))
		if err != nil {
			return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "failed to read body")
		}
		if len(buf) > proxyMaxBody {
			return xrpcError(c, http.StatusRequestEntityTooLarge, "PayloadTooLarge", "request body too large for proxying")
		}
		body = bytes.NewReader(buf)
		bodyLen = int64(len(buf))
	}

	upstream := endpoint + "/xrpc/" + nsid
	if in.URL.RawQuery != "" {
		upstream += "?" + in.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(ctx, in.Method, upstream, body)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
	req.ContentLength = bodyLen
	for _, h := range []string{"Content-Type", "Accept", "Accept-Language", "atproto-accept-labelers"} {
		if v := in.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", p.userAgent)

	resp, err := p.http.Do(req)
	if err != nil {
		return xrpcError(c, http.StatusBadGateway, "UpstreamFailure", "upstream request failed: "+err.Error())
	}
	defer resp.Body.Close()

	out := c.Response()
	for _, h := range []string{"Content-Type", "atproto-content-labelers", "atproto-repo-rev", "Retry-After"} {
		if v := resp.Header.Get(h); v != "" {
			out.Header().Set(h, v)
		}
	}
	out.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(out, resp.Body)
	return nil
}
