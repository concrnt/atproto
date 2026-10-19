package pds

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type captured struct {
	method, path, query, body string
	header                    http.Header
}

func newUpstream(t *testing.T, status int, respHeaders map[string]string, respBody string) (*httptest.Server, *captured) {
	t.Helper()
	cap := &captured{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		cap.method, cap.path, cap.query, cap.body, cap.header = r.Method, r.URL.Path, r.URL.RawQuery, string(b), r.Header.Clone()
		for k, v := range respHeaders {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(srv.Close)
	return srv, cap
}

func TestProxyForwardsWithServiceAuth(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)
	appview, cap := newUpstream(t, http.StatusTooManyRequests,
		map[string]string{"Content-Type": "application/json", "atproto-content-labelers": "did:plc:lab;redact", "Retry-After": "7", "X-Internal": "no"},
		`{"error":"RateLimitExceeded"}`)
	chat, chatCap := newUpstream(t, http.StatusOK, map[string]string{"Content-Type": "application/json"}, `{"convos":[]}`)
	h.srv.Proxy.http = appview.Client()
	h.srv.Proxy.http.Timeout = 5 * time.Second
	h.dir.services["did:web:appview.test"] = map[string]string{"bsky_appview": appview.URL, "bsky_notif": appview.URL}
	h.dir.services["did:web:chat.test"] = map[string]string{"bsky_chat": chat.URL}

	// Default target for app.bsky.* with no header, GET with query.
	rec, out := h.do(t, http.MethodGet, "/xrpc/app.bsky.actor.getProfile?actor="+h.did, nil,
		h.authed(map[string]string{"atproto-accept-labelers": "did:plc:lab", "Accept-Encoding": "gzip"}))
	if rec.Code != http.StatusTooManyRequests || out["error"] != "RateLimitExceeded" {
		t.Fatalf("proxied status/body: %d %s", rec.Code, rec.Body)
	}
	if cap.method != http.MethodGet || cap.path != "/xrpc/app.bsky.actor.getProfile" || cap.query != "actor="+h.did {
		t.Fatalf("upstream request: %+v", cap)
	}
	if cap.header.Get("atproto-accept-labelers") != "did:plc:lab" {
		t.Fatalf("labelers header not forwarded: %v", cap.header)
	}
	if strings.Contains(cap.header.Get("Accept-Encoding"), "gzip") && cap.header.Get("Accept-Encoding") != "gzip" {
		t.Fatalf("client Accept-Encoding leaked: %v", cap.header)
	}
	tok := strings.TrimPrefix(cap.header.Get("Authorization"), "Bearer ")
	claims := jwtPayload(t, tok)
	if claims["iss"] != h.did || claims["aud"] != "did:web:appview.test" || claims["lxm"] != "app.bsky.actor.getProfile" {
		t.Fatalf("service auth claims: %v", claims)
	}
	if exp, iat := claims["exp"].(float64), claims["iat"].(float64); exp-iat < 55 || exp-iat > 65 {
		t.Fatalf("service auth ttl: %v", exp-iat)
	}
	if rec.Header().Get("atproto-content-labelers") != "did:plc:lab;redact" || rec.Header().Get("Retry-After") != "7" {
		t.Fatalf("response headers not relayed: %v", rec.Header())
	}
	if rec.Header().Get("X-Internal") != "" {
		t.Fatalf("unexpected upstream header relayed")
	}

	// Explicit header picks the chat service; POST body is forwarded.
	rec, out = h.do(t, http.MethodPost, "/xrpc/chat.bsky.convo.sendMessage",
		`{"convoId":"c1","message":{"text":"hi"}}`,
		h.authed(map[string]string{"atproto-proxy": "did:web:chat.test#bsky_chat"}))
	if rec.Code != http.StatusOK {
		t.Fatalf("chat proxied: %d %s", rec.Code, rec.Body)
	}
	if chatCap.method != http.MethodPost || !strings.Contains(chatCap.body, `"text":"hi"`) || chatCap.header.Get("Content-Type") != "application/json" {
		t.Fatalf("chat upstream request: %+v", chatCap)
	}
	if claims := jwtPayload(t, strings.TrimPrefix(chatCap.header.Get("Authorization"), "Bearer ")); claims["aud"] != "did:web:chat.test" || claims["lxm"] != "chat.bsky.convo.sendMessage" {
		t.Fatalf("chat service auth: %v", claims)
	}

	// chat.bsky.* without a header defaults to the chat service.
	rec, _ = h.do(t, http.MethodGet, "/xrpc/chat.bsky.convo.listConvos", nil, h.authed(nil))
	if rec.Code != http.StatusOK || chatCap.path != "/xrpc/chat.bsky.convo.listConvos" {
		t.Fatalf("chat default target: %d %s", rec.Code, chatCap.path)
	}

	// A com.atproto method is proxied only when the header says so.
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle?handle=bob.test", nil,
		h.authed(map[string]string{"atproto-proxy": "did:web:appview.test#bsky_appview"}))
	if rec.Code != http.StatusTooManyRequests || cap.path != "/xrpc/com.atproto.identity.resolveHandle" {
		t.Fatalf("resolveHandle via proxy: %d %s", rec.Code, cap.path)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.temp.whatever", nil, h.authed(nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("unproxied method: %d", rec.Code)
	}

	// Unknown service fragment.
	rec, _ = h.do(t, http.MethodGet, "/xrpc/app.bsky.actor.getProfile", nil,
		h.authed(map[string]string{"atproto-proxy": "did:web:appview.test#nope"}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown service: %d %s", rec.Code, rec.Body)
	}
}

func TestProxyRequiresAuthAndCapsBody(t *testing.T) {
	h := newClientHarness(t)
	rec, out := h.do(t, http.MethodGet, "/xrpc/app.bsky.actor.getProfile", nil, nil)
	if rec.Code != http.StatusUnauthorized || out["error"] != "AuthMissing" {
		t.Fatalf("unauthenticated proxy: %d %s", rec.Code, rec.Body)
	}
	// Unauthenticated, non-proxied methods keep the crawler-friendly 501.
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.temp.whatever", nil, nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("unknown method unauthenticated: %d", rec.Code)
	}

	h.login(t)
	chat, _ := newUpstream(t, http.StatusOK, nil, `{}`)
	h.srv.Proxy.http = chat.Client()
	h.dir.services["did:web:chat.test"] = map[string]string{"bsky_chat": chat.URL}
	big := strings.Repeat("x", proxyMaxBody+1)
	rec, out = h.do(t, http.MethodPost, "/xrpc/chat.bsky.convo.sendMessage", big, h.authed(nil))
	if rec.Code != http.StatusRequestEntityTooLarge || out["error"] != "PayloadTooLarge" {
		t.Fatalf("oversized body: %d %s", rec.Code, rec.Body)
	}

	// Upstream unreachable → 502.
	h.dir.services["did:web:chat.test"] = map[string]string{"bsky_chat": "https://127.0.0.1:1"}
	rec, out = h.do(t, http.MethodGet, "/xrpc/chat.bsky.convo.listConvos", nil, h.authed(nil))
	if rec.Code != http.StatusBadGateway || out["error"] != "UpstreamFailure" {
		t.Fatalf("upstream down: %d %s", rec.Code, rec.Body)
	}
}
