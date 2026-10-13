package mgmt

// The operational endpoints are reached on the bridge port directly (the
// gateway never proxies them): /health answers the liveness probe and
// /metrics exposes the default Prometheus registry.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestOperationalEndpoints(t *testing.T) {
	srv, _, _ := setupServer(t)
	e := echo.New()
	srv.Register(e)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("health: code %d body %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics: code %d", rec.Code)
	}
	// The concrnt client's peer series only appear once a request has been
	// observed, so only the always-present Go collector is asserted here.
	if body := rec.Body.String(); !strings.Contains(body, "go_goroutines") {
		t.Errorf("metrics: missing go_goroutines in %q", body)
	}
}
