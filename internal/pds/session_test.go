package pds

import (
	"net/http"
	"testing"
	"time"

	"github.com/concrnt/atproto/internal/apppass"
)

func TestCreateSessionVariants(t *testing.T) {
	h := newClientHarness(t)

	for _, id := range []string{h.handle, "@" + h.handle, "ALICE.example.com", h.did} {
		rec, out := h.do(t, http.MethodPost, "/xrpc/com.atproto.server.createSession",
			map[string]string{"identifier": id, "password": h.plain}, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("login as %q: %d %s", id, rec.Code, rec.Body)
		}
		if out["did"] != h.did || out["handle"] != h.handle || out["active"] != true {
			t.Fatalf("login as %q: body %v", id, out)
		}
		doc, _ := out["didDoc"].(map[string]any)
		svcs, _ := doc["service"].([]any)
		if len(svcs) != 1 || svcs[0].(map[string]any)["serviceEndpoint"] != "https://pds.example" {
			t.Fatalf("didDoc service: %v", doc)
		}
		vm, _ := doc["verificationMethod"].([]any)
		if len(vm) != 1 || vm[0].(map[string]any)["publicKeyMultibase"] == "" {
			t.Fatalf("didDoc verificationMethod: %v", doc)
		}
		claims := jwtPayload(t, out["accessJwt"].(string))
		if claims["scope"] != scopeAccess || claims["sub"] != h.did || claims["aud"] != "did:web:pds.example" {
			t.Fatalf("access claims: %v", claims)
		}
	}

	for _, tc := range []struct{ id, pw string }{
		{h.handle, "wrong-pass"},
		{"nobody.example.com", h.plain},
		{h.handle, ""},
	} {
		rec, out := h.do(t, http.MethodPost, "/xrpc/com.atproto.server.createSession",
			map[string]string{"identifier": tc.id, "password": tc.pw}, nil)
		if tc.pw == "" {
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("empty password: %d", rec.Code)
			}
			continue
		}
		if rec.Code != http.StatusUnauthorized || out["error"] != "AuthenticationRequired" {
			t.Fatalf("bad login %v: %d %s", tc, rec.Code, rec.Body)
		}
	}
}

func TestLoginRateLimit(t *testing.T) {
	h := newClientHarness(t)
	for i := 0; i < loginFailureLimit; i++ {
		h.do(t, http.MethodPost, "/xrpc/com.atproto.server.createSession",
			map[string]string{"identifier": h.handle, "password": "nope"}, nil)
	}
	rec, out := h.do(t, http.MethodPost, "/xrpc/com.atproto.server.createSession",
		map[string]string{"identifier": h.handle, "password": h.plain}, nil)
	if rec.Code != http.StatusTooManyRequests || out["error"] != "RateLimitExceeded" {
		t.Fatalf("expected rate limit, got %d %s", rec.Code, rec.Body)
	}
}

func TestSessionLifecycle(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)

	rec, out := h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil, h.authed(nil))
	if rec.Code != http.StatusOK || out["did"] != h.did || out["emailConfirmed"] != true {
		t.Fatalf("getSession: %d %s", rec.Code, rec.Body)
	}
	if _, ok := out["accessJwt"]; ok {
		t.Fatalf("getSession must not return tokens")
	}

	// getSession with the refresh token is not an access token.
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil,
		map[string]string{"Authorization": "Bearer " + h.refresh})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("refresh token as access: %d", rec.Code)
	}

	// refresh rotates the lineage; the old refresh token dies, the old
	// access token (same lineage JTI) dies too.
	oldAccess, oldRefresh := h.access, h.refresh
	rec, out = h.do(t, http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil,
		map[string]string{"Authorization": "Bearer " + h.refresh})
	if rec.Code != http.StatusOK {
		t.Fatalf("refreshSession: %d %s", rec.Code, rec.Body)
	}
	h.access, h.refresh = out["accessJwt"].(string), out["refreshJwt"].(string)
	if _, ok := out["didDoc"]; !ok {
		t.Fatalf("refreshSession must include didDoc")
	}
	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil,
		map[string]string{"Authorization": "Bearer " + oldRefresh})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("reused refresh token: %d %s", rec.Code, rec.Body)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil,
		map[string]string{"Authorization": "Bearer " + oldAccess})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("old access after refresh: %d", rec.Code)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("new access: %d %s", rec.Code, rec.Body)
	}

	// deleteSession ends the lineage.
	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.server.deleteSession", nil,
		map[string]string{"Authorization": "Bearer " + h.refresh})
	if rec.Code != http.StatusOK {
		t.Fatalf("deleteSession: %d %s", rec.Code, rec.Body)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil, h.authed(nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("access after logout: %d", rec.Code)
	}
}

func TestExpiredAccessTokenIs400ExpiredToken(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)
	claims := jwtPayload(t, h.access)
	expired, err := h.srv.Sessions.sign(h.did, scopeAccess, claims["jti"].(string), h.apID, -time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	rec, out := h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil,
		map[string]string{"Authorization": "Bearer " + expired})
	if rec.Code != http.StatusBadRequest || out["error"] != "ExpiredToken" {
		t.Fatalf("expired access: %d %s", rec.Code, rec.Body)
	}
	// Garbage is 401 (the client logs out).
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil,
		map[string]string{"Authorization": "Bearer nope"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("garbage token: %d", rec.Code)
	}
}

func TestRevokingAppPasswordKillsSessions(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)
	if err := apppass.Revoke(h.db, h.did, "phone"); err != nil {
		t.Fatal(err)
	}
	rec, _ := h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getSession", nil, h.authed(nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("access after revoke: %d", rec.Code)
	}
	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil,
		map[string]string{"Authorization": "Bearer " + h.refresh})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("refresh after revoke: %d", rec.Code)
	}
	rec, _ = h.do(t, http.MethodPost, "/xrpc/com.atproto.server.createSession",
		map[string]string{"identifier": h.handle, "password": h.plain}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("login with revoked password: %d", rec.Code)
	}
}

func TestGetServiceAuthAndListAppPasswords(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)

	rec, out := h.do(t, http.MethodGet,
		"/xrpc/com.atproto.server.getServiceAuth?aud=did:web:video.test&lxm=com.atproto.repo.uploadBlob", nil, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("getServiceAuth: %d %s", rec.Code, rec.Body)
	}
	claims := jwtPayload(t, out["token"].(string))
	if claims["iss"] != h.did || claims["aud"] != "did:web:video.test" || claims["lxm"] != "com.atproto.repo.uploadBlob" {
		t.Fatalf("service auth claims: %v", claims)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getServiceAuth?aud=not-a-did", nil, h.authed(nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad aud: %d", rec.Code)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.getServiceAuth?aud=did:web:video.test", nil, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated getServiceAuth: %d", rec.Code)
	}

	rec, out = h.do(t, http.MethodGet, "/xrpc/com.atproto.server.listAppPasswords", nil, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("listAppPasswords: %d %s", rec.Code, rec.Body)
	}
	pws, _ := out["passwords"].([]any)
	if len(pws) != 1 || pws[0].(map[string]any)["name"] != "phone" {
		t.Fatalf("listAppPasswords body: %v", out)
	}
	rec, out = h.do(t, http.MethodPost, "/xrpc/com.atproto.server.createAppPassword",
		map[string]string{"name": "x"}, h.authed(nil))
	if rec.Code != http.StatusForbidden || out["error"] != readOnlyErrorName {
		t.Fatalf("createAppPassword must be refused: %d %s", rec.Code, rec.Body)
	}
}

func TestResolveHandleLocal(t *testing.T) {
	h := newClientHarness(t)
	rec, out := h.do(t, http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle?handle=Alice.example.com", nil, nil)
	if rec.Code != http.StatusOK || out["did"] != h.did {
		t.Fatalf("resolveHandle: %d %s", rec.Code, rec.Body)
	}
	rec, _ = h.do(t, http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle?handle=bob.example.com", nil, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown handle unauthenticated: %d", rec.Code)
	}
}
