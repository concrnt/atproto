package pds

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/auth"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// keyDirectory serves the test account's DID with its signing key, the way
// plc.directory would, so indigo's ServiceAuthValidator (what Bluesky's Go
// services use) can verify the proxy's inter-service JWT end to end.
type keyDirectory struct {
	fakeDirectory
	did string
	key string // did:key multibase
}

func (k *keyDirectory) LookupDID(ctx context.Context, did syntax.DID) (*identity.Identity, error) {
	if did.String() == k.did {
		return &identity.Identity{DID: did, Keys: map[string]identity.VerificationMethod{
			"atproto": {Type: "Multikey", PublicKeyMultibase: k.key},
		}}, nil
	}
	return k.fakeDirectory.LookupDID(ctx, did)
}

func TestProxyServiceJWTVerifiesWithIndigoValidator(t *testing.T) {
	h := newClientHarness(t)
	h.login(t)
	upstream, cap := newUpstream(t, http.StatusOK, nil, `{}`)
	h.srv.Proxy.http = upstream.Client()
	h.dir.services["did:web:chat.test"] = map[string]string{"bsky_chat": upstream.URL}

	rec, _ := h.do(t, http.MethodGet, "/xrpc/chat.bsky.convo.listConvos", nil, h.authed(nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("proxy: %d %s", rec.Code, rec.Body)
	}
	tok := strings.TrimPrefix(cap.header.Get("Authorization"), "Bearer ")

	priv, err := h.ks.SigningKey(h.did)
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := priv.PublicKey()
	dir := &keyDirectory{fakeDirectory: *h.dir, did: h.did, key: pub.Multibase()}

	v := &auth.ServiceAuthValidator{Audience: "did:web:chat.test", Dir: dir}
	lxm := syntax.NSID("chat.bsky.convo.listConvos")
	iss, err := v.Validate(context.Background(), tok, &lxm)
	if err != nil {
		t.Fatalf("validator rejected the proxy JWT: %v", err)
	}
	if iss.String() != h.did {
		t.Fatalf("iss = %s, want %s", iss, h.did)
	}
	// Wrong method or audience must fail.
	other := syntax.NSID("chat.bsky.convo.sendMessage")
	if _, err := v.Validate(context.Background(), tok, &other); err == nil {
		t.Fatalf("validator accepted the JWT for a different lxm")
	}
	v2 := &auth.ServiceAuthValidator{Audience: "did:web:appview.test", Dir: dir}
	if _, err := v2.Validate(context.Background(), tok, &lxm); err == nil {
		t.Fatalf("validator accepted the JWT for a different audience")
	}
}
