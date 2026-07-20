package mgmt

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/glebarez/sqlite"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/plcm"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

// fakeResolver maps handles to DIDs from an in-memory table controlled by the
// test (standing in for DNS TXT / well-known).
type fakeResolver struct{ table map[string]string }

func (r *fakeResolver) ResolveHandle(_ context.Context, handle string) (string, error) {
	if did, ok := r.table[handle]; ok {
		return did, nil
	}
	return "", http.ErrNoLocation
}

func fakePLC(t *testing.T) *httptest.Server {
	t.Helper()
	reg := map[string]bool{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		did := strings.TrimPrefix(r.URL.Path, "/")
		switch r.Method {
		case http.MethodPost:
			reg[did] = true
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if !reg[did] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": did})
		}
	}))
}

func setupServer(t *testing.T) (*Server, *fakeResolver, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "m.db")), &gorm.Config{
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
	plcSrv := fakePLC(t)
	t.Cleanup(plcSrv.Close)
	rot, _ := atcrypto.GeneratePrivateKeyK256()
	repos, err := repoman.New(db, t.TempDir(), ks, plcm.NewService(plcSrv.URL, rot, "concrnt.example"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, repos, nil, "concrnt.example", nil, "test")
	fr := &fakeResolver{table: map[string]string{}}
	srv.SetResolver(fr)
	return srv, fr, db
}

func authedReq(method, path, body, ccid string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("cc-requester", `{"ccid":"`+ccid+`"}`)
	return req
}

func TestSetupThenVerifyLifecycle(t *testing.T) {
	srv, fr, db := setupServer(t)
	e := echo.New()
	srv.Register(e)

	// setup → pending, DID minted, no repo yet.
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/setup", `{"handle":"alice.example.com"}`, "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("setup status %d: %s", rec.Code, rec.Body)
	}
	var setupResp struct {
		DID    string `json:"did"`
		Handle string `json:"handle"`
		Status string `json:"status"`
	}
	json.Unmarshal(rec.Body.Bytes(), &setupResp)
	if setupResp.Status != "pending" || setupResp.Handle != "alice.example.com" {
		t.Fatalf("unexpected setup resp %+v", setupResp)
	}

	// verify before DNS is set → stays pending.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/verify", ``, "con1alice"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("verify without DNS should fail, got %d: %s", rec.Code, rec.Body)
	}
	var ent store.Entity
	db.Where("ccid = ?", "con1alice").First(&ent)
	if ent.Status != "pending" {
		t.Fatalf("account should still be pending, got %q", ent.Status)
	}

	// Wrong DID mapping → did_mismatch.
	fr.table["alice.example.com"] = "did:plc:someoneelse"
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/verify", ``, "con1alice"))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "did_mismatch") {
		t.Fatalf("expected did_mismatch, got %d: %s", rec.Code, rec.Body)
	}

	// Correct mapping → active.
	fr.table["alice.example.com"] = setupResp.DID
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/verify", ``, "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("verify status %d: %s", rec.Code, rec.Body)
	}
	db.Where("ccid = ?", "con1alice").First(&ent)
	if ent.Status != "active" {
		t.Fatalf("account should be active, got %q", ent.Status)
	}
	if ent.DID != setupResp.DID {
		t.Fatal("did changed across lifecycle")
	}

	// verify is idempotent once active.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/verify", ``, "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("idempotent verify status %d: %s", rec.Code, rec.Body)
	}
}

func TestSetupRejectsNonDomainHandle(t *testing.T) {
	srv, _, _ := setupServer(t)
	e := echo.New()
	srv.Register(e)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/setup", `{"handle":"alice"}`, "con1alice"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bare label should be rejected, got %d: %s", rec.Code, rec.Body)
	}
}

func TestSetupRequiresAuth(t *testing.T) {
	srv, _, _ := setupServer(t)
	e := echo.New()
	srv.Register(e)

	req := httptest.NewRequest(http.MethodPost, "/atproto/api/setup", strings.NewReader(`{"handle":"a.example.com"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing cc-requester should 401, got %d", rec.Code)
	}
}
