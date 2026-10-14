package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/inbound"
)

type fakeImporter struct {
	calls []string
	key   string
	err   error
}

func (f *fakeImporter) ImportPost(_ context.Context, atURI string) (string, error) {
	f.calls = append(f.calls, atURI)
	return f.key, f.err
}

// The import endpoint mirrors a post on demand for an authenticated user and
// answers its record key; the importer's not-found becomes 404 and any other
// failure 502, so the client can tell "nothing to import" from "try later".
func TestImportMirrorsPostForRequester(t *testing.T) {
	srv, _, _ := setupServer(t)
	imp := &fakeImporter{key: "cckv://con1service/atproto.concrnt.world/inbox/abc"}
	srv.Importer = imp
	e := echo.New()
	srv.Register(e)

	const uri = "at://did:plc:bob/app.bsky.feed.post/3kpost"
	post := func(body string, auth bool) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/atproto/api/import", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if auth {
			req.Header.Set("cc-requester", `{"ccid":"con1alice"}`)
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}

	rec := post(`{"uri":"`+uri+`"}`, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["key"] != imp.key {
		t.Errorf("key = %q, want %q", resp["key"], imp.key)
	}
	if len(imp.calls) != 1 || imp.calls[0] != uri {
		t.Errorf("importer calls = %v, want [%s]", imp.calls, uri)
	}

	if rec := post(`{"uri":"`+uri+`"}`, false); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated: status %d, want 401", rec.Code)
	}
	for _, body := range []string{`{}`, `{"uri":"https://bsky.app/profile/bob/post/3kpost"}`, `{"uri":"at://did:plc:bob"}`, `not json`} {
		if rec := post(body, true); rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status %d, want 400", body, rec.Code)
		}
	}
	if len(imp.calls) != 1 {
		t.Errorf("rejected requests must not reach the importer: calls = %v", imp.calls)
	}

	imp.err = inbound.ErrPostNotFound
	if rec := post(`{"uri":"`+uri+`"}`, true); rec.Code != http.StatusNotFound {
		t.Errorf("not found: status %d, want 404", rec.Code)
	}
	imp.err = errors.New("commit failed")
	if rec := post(`{"uri":"`+uri+`"}`, true); rec.Code != http.StatusBadGateway {
		t.Errorf("importer failure: status %d, want 502", rec.Code)
	}

	srv.Importer = nil
	if rec := post(`{"uri":"`+uri+`"}`, true); rec.Code != http.StatusNotImplemented {
		t.Errorf("no importer: status %d, want 501", rec.Code)
	}
}

// Clients gate the import button on the endpoint being advertised, like the
// ActivityPub bridge's net.concrnt.activitypub.import.
func TestCCInfoAdvertisesImport(t *testing.T) {
	srv, _, _ := setupServer(t)
	e := echo.New()
	srv.Register(e)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cc-info", nil))
	var info struct {
		Endpoints map[string]string `json:"endpoints"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if got := info.Endpoints[ServiceName+".import"]; got != "/atproto/api/import" {
		t.Errorf("import endpoint = %q", got)
	}
}
