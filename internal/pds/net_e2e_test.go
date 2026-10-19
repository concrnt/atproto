package pds

// Network E2E against the real Bluesky services:
//
//	ATPROTO_NET_E2E=1 go test ./internal/pds/ -run NetE2E -v
//
// The test account's DID is not registered on plc.directory, so the real
// appview / chat service are expected to refuse the service token; what this
// proves is that did:web resolution of the service DIDs works, that the
// request reaches the services, and that their status/body/headers are
// relayed instead of turning into a 502.

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
)

func TestNetE2EProxyReachesRealServices(t *testing.T) {
	if os.Getenv("ATPROTO_NET_E2E") == "" {
		t.Skip("set ATPROTO_NET_E2E=1 to run against api.bsky.app / api.bsky.chat")
	}
	h := newClientHarness(t)
	h.login(t)
	base := &identity.BaseDirectory{SkipHandleVerification: true, UserAgent: "concrnt-atproto-bridge/test"}
	h.srv.Proxy = NewProxy(identity.NewCacheDirectory(base, 100, 10*time.Minute, time.Minute, time.Minute),
		"did:web:api.bsky.app", "did:web:api.bsky.chat", h.ks, "test")

	for _, tc := range []struct {
		method, path string
		headers      map[string]string
	}{
		{http.MethodGet, "/xrpc/app.bsky.actor.getProfile?actor=bsky.app", map[string]string{"atproto-proxy": "did:web:api.bsky.app#bsky_appview"}},
		{http.MethodGet, "/xrpc/app.bsky.actor.getProfile?actor=bsky.app", nil},
		{http.MethodGet, "/xrpc/chat.bsky.convo.listConvos", map[string]string{"atproto-proxy": "did:web:api.bsky.chat#bsky_chat"}},
		{http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle?handle=bsky.app", map[string]string{"atproto-proxy": "did:web:api.bsky.app#bsky_appview"}},
	} {
		rec, out := h.do(t, tc.method, tc.path, nil, h.authed(tc.headers))
		t.Logf("%s %v → %d %s (content-type %q)", tc.path, tc.headers, rec.Code, rec.Body.String(), rec.Header().Get("Content-Type"))
		// The bridge's own failures are 502 UpstreamFailure / 501 / a 500
		// carrying a Go error string; the chat service answers an
		// unresolvable issuer with its generic 500 "Internal Server Error"
		// (checked with curl), which is a relayed upstream response.
		if rec.Code == http.StatusBadGateway || rec.Code == http.StatusNotImplemented ||
			(rec.Code == http.StatusInternalServerError && out["message"] != "Internal Server Error") {
			t.Fatalf("%s: bridge-side failure %d %s", tc.path, rec.Code, rec.Body)
		}
		if _, ok := out["error"]; !ok && rec.Code >= 400 {
			t.Fatalf("%s: non-XRPC error body: %s", tc.path, rec.Body)
		}
	}
}
