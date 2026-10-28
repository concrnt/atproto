package mgmt

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
)

// wellKnownOnly serves /.well-known/atproto-did for one host and 404s
// everything else, in particular any PLC directory lookup.
type wellKnownOnly struct{ host, did string }

func (w wellKnownOnly) RoundTrip(req *http.Request) (*http.Response, error) {
	status, body := http.StatusNotFound, ""
	if req.URL.Host == w.host && req.URL.Path == "/.well-known/atproto-did" {
		status, body = http.StatusOK, w.did+"\n"
	}
	return &http.Response{
		StatusCode:    status,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Header:        http.Header{},
		Request:       req,
	}, nil
}

// The real resolver must be one-directional: a domain that points at the DID
// resolves even though the DID document still declares the old handle (the
// state every active account is in while moving to a new domain).
func TestIndigoResolverDoesNotRequireDeclaredHandle(t *testing.T) {
	r := indigoResolver{dir: &identity.BaseDirectory{
		HTTPClient:            http.Client{Transport: wellKnownOnly{host: "new.example.com", did: "did:plc:testresolver0000000000000"}},
		SkipDNSDomainSuffixes: []string{".example.com"},
	}}
	did, err := r.ResolveHandle(context.Background(), "New.Example.com")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if did != "did:plc:testresolver0000000000000" {
		t.Fatalf("unexpected did %q", did)
	}
	if _, err := r.ResolveHandle(context.Background(), "other.example.com"); err == nil {
		t.Fatal("unpublished handle should not resolve")
	}
}
