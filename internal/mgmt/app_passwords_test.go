package mgmt

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/apppass"
	"github.com/concrnt/atproto/internal/store"
)

func TestAppPasswordLifecycle(t *testing.T) {
	srv, _, db := setupServer(t)
	e := echo.New()
	srv.Register(e)

	// Not bridged yet → 404; pending → 400.
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/app-passwords", `{"name":"phone"}`, "con1alice"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unbridged: %d %s", rec.Code, rec.Body)
	}
	if err := db.Create(&store.Entity{CCID: "con1alice", DID: "did:plc:alice", Handle: "alice.example.com", Status: "pending"}).Error; err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/app-passwords", `{"name":"phone"}`, "con1alice"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("pending: %d %s", rec.Code, rec.Body)
	}
	db.Model(&store.Entity{}).Where("ccid = ?", "con1alice").Update("status", "active")

	// Create returns the plaintext once, plus the login hint.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/app-passwords", `{"name":"phone"}`, "con1alice"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var created struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		Password string `json:"password"`
		Login    struct {
			Service    string `json:"service"`
			Identifier string `json:"identifier"`
		} `json:"login"`
	}
	json.Unmarshal(rec.Body.Bytes(), &created)
	if created.ID == "" || created.Name != "phone" || len(created.Password) != 19 {
		t.Fatalf("create body: %s", rec.Body)
	}
	if created.Login.Service != "https://concrnt.example" || created.Login.Identifier != "alice.example.com" {
		t.Fatalf("login hint: %s", rec.Body)
	}
	if _, err := apppass.Authenticate(db, "did:plc:alice", created.Password); err != nil {
		t.Fatalf("plaintext does not authenticate: %v", err)
	}

	// Duplicate name, bad name.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/app-passwords", `{"name":"phone"}`, "con1alice"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodPost, "/atproto/api/app-passwords", `{"name":""}`, "con1alice"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty name: %d", rec.Code)
	}

	// List never includes the secret.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/atproto/api/app-passwords", "", "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}
	var list struct {
		AppPasswords []map[string]any `json:"appPasswords"`
	}
	json.Unmarshal(rec.Body.Bytes(), &list)
	if len(list.AppPasswords) != 1 || list.AppPasswords[0]["name"] != "phone" || list.AppPasswords[0]["lastUsedAt"] == nil {
		t.Fatalf("list body: %s", rec.Body)
	}
	if _, ok := list.AppPasswords[0]["password"]; ok {
		t.Fatalf("list leaks the password")
	}
	if _, ok := list.AppPasswords[0]["hash"]; ok {
		t.Fatalf("list leaks the hash")
	}

	// Someone else cannot revoke it; the owner can; sessions go with it.
	db.Create(&store.Entity{CCID: "con1bob", DID: "did:plc:bob", Handle: "bob.example.com", Status: "active"})
	db.Create(&store.Session{JTI: "s1", DID: "did:plc:alice", AppPasswordID: created.ID})
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodDelete, "/atproto/api/app-passwords/"+created.ID, "", "con1bob"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("revoke by other: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodDelete, "/atproto/api/app-passwords/"+created.ID, "", "con1alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("revoke: %d %s", rec.Code, rec.Body)
	}
	var sessions int64
	db.Model(&store.Session{}).Where("app_password_id = ?", created.ID).Count(&sessions)
	if sessions != 0 {
		t.Fatalf("sessions survived revoke: %d", sessions)
	}
	if _, err := apppass.Authenticate(db, "did:plc:alice", created.Password); err == nil {
		t.Fatalf("revoked password still authenticates")
	}

	// cc-info advertises the endpoints; info carries the login hint.
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cc-info", nil))
	var info struct {
		Endpoints map[string]string `json:"endpoints"`
	}
	json.Unmarshal(rec.Body.Bytes(), &info)
	if info.Endpoints[ServiceName+".revokeAppPassword"] != "/atproto/api/app-passwords/{id}" ||
		info.Endpoints[ServiceName+".createAppPassword"] != "/atproto/api/app-passwords" {
		t.Fatalf("cc-info endpoints: %v", info.Endpoints)
	}
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, authedReq(http.MethodGet, "/atproto/api/info", "", "con1alice"))
	var infoResp struct {
		Login map[string]string `json:"login"`
	}
	json.Unmarshal(rec.Body.Bytes(), &infoResp)
	if infoResp.Login["service"] != "https://concrnt.example" || infoResp.Login["identifier"] != "alice.example.com" {
		t.Fatalf("info login hint: %s", rec.Body)
	}
}
