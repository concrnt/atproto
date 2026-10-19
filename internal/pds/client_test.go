package pds

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/glebarez/sqlite"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/apppass"
	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/plcm"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

// fakeDirectory answers service DID lookups from a fixed table.
type fakeDirectory struct {
	services map[string]map[string]string // did → service id → endpoint
}

func (f *fakeDirectory) LookupHandle(ctx context.Context, h syntax.Handle) (*identity.Identity, error) {
	return nil, identity.ErrHandleNotFound
}

func (f *fakeDirectory) LookupDID(ctx context.Context, did syntax.DID) (*identity.Identity, error) {
	svcs, ok := f.services[did.String()]
	if !ok {
		return nil, identity.ErrDIDNotFound
	}
	ident := &identity.Identity{DID: did, Services: map[string]identity.ServiceEndpoint{}}
	for id, url := range svcs {
		ident.Services[id] = identity.ServiceEndpoint{Type: "Service", URL: url}
	}
	return ident, nil
}

func (f *fakeDirectory) Lookup(ctx context.Context, atid syntax.AtIdentifier) (*identity.Identity, error) {
	did, err := atid.AsDID()
	if err != nil {
		return nil, err
	}
	return f.LookupDID(ctx, did)
}

func (f *fakeDirectory) Purge(ctx context.Context, atid syntax.AtIdentifier) error { return nil }

type clientHarness struct {
	srv     *Server
	e       *echo.Echo
	db      *gorm.DB
	ks      *keys.Service
	dir     *fakeDirectory
	did     string
	handle  string
	plain   string // app password
	apID    string
	access  string
	refresh string
}

// newClientHarness builds a PDS with the client surface enabled, one active
// bridged account with a signing key and repo, and one app password.
func newClientHarness(t *testing.T) *clientHarness {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "pds.db")), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := store.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	ks, err := keys.NewService(db, strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	rot, _ := atcrypto.GeneratePrivateKeyK256()
	repos, err := repoman.New(db, t.TempDir(), ks, plcm.NewService("http://plc.invalid", rot, "pds.example"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repos.Close(context.Background()) })

	did, handle := "did:plc:clienttest", "alice.example.com"
	priv, _ := keys.Generate()
	if err := ks.StoreSigningKey(did, priv); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&store.Entity{CCID: "con1alice", DID: did, Handle: handle, Status: "active", Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := repos.InitActor(context.Background(), did, handle); err != nil {
		t.Fatal(err)
	}
	ap, plain, err := apppass.Create(db, did, "phone")
	if err != nil {
		t.Fatal(err)
	}

	dir := &fakeDirectory{services: map[string]map[string]string{}}
	srv := NewServer(db, repos, nil, "pds.example", "test")
	srv.Keys = ks
	srv.Sessions = NewSessions(db, []byte("0123456789abcdef0123456789abcdef"), "pds.example")
	srv.Proxy = NewProxy(dir, "did:web:appview.test", "did:web:chat.test", ks, "test")
	e := echo.New()
	srv.Register(e)
	return &clientHarness{srv: srv, e: e, db: db, ks: ks, dir: dir, did: did, handle: handle, plain: plain, apID: ap.ID}
}

func (h *clientHarness) do(t *testing.T, method, path string, body any, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body == nil {
		rdr = bytes.NewReader(nil)
	} else if s, ok := body.(string); ok {
		rdr = bytes.NewReader([]byte(s))
	} else {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.e.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func (h *clientHarness) login(t *testing.T) {
	t.Helper()
	rec, out := h.do(t, http.MethodPost, "/xrpc/com.atproto.server.createSession",
		map[string]string{"identifier": h.handle, "password": h.plain}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("createSession: %d %s", rec.Code, rec.Body)
	}
	h.access, _ = out["accessJwt"].(string)
	h.refresh, _ = out["refreshJwt"].(string)
	if h.access == "" || h.refresh == "" {
		t.Fatalf("createSession: missing tokens: %s", rec.Body)
	}
}

func (h *clientHarness) authed(extra map[string]string) map[string]string {
	m := map[string]string{"Authorization": "Bearer " + h.access}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func jwtPayload(t *testing.T, tok string) map[string]any {
	t.Helper()
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWT: %q", tok)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}
