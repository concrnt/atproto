package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestCORSHeadersOnManagementAndPDSRoutes(t *testing.T) {
	e := newEcho()
	e.GET("/atproto/api/info", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })
	e.GET("/xrpc/_health", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })

	for _, path := range []string{"/atproto/api/info", "/xrpc/_health"} {
		// preflight
		req := httptest.NewRequest(http.MethodOptions, path, nil)
		req.Header.Set(echo.HeaderOrigin, "https://world.concrnt.net")
		req.Header.Set(echo.HeaderAccessControlRequestMethod, http.MethodGet)
		req.Header.Set(echo.HeaderAccessControlRequestHeaders, "authorization, atproto-proxy, atproto-accept-labelers")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s preflight: status %d", path, rec.Code)
		}
		if got := rec.Header().Get(echo.HeaderAccessControlAllowOrigin); got != "*" {
			t.Fatalf("%s preflight: allow-origin %q", path, got)
		}
		// The official Bluesky web app sends the proxy and labeler headers
		// on every appview/chat call; they must survive preflight.
		got := strings.ToLower(rec.Header().Get(echo.HeaderAccessControlAllowHeaders))
		for _, h := range []string{"authorization", "atproto-proxy", "atproto-accept-labelers"} {
			if !strings.Contains(got, h) {
				t.Fatalf("%s preflight: allow-headers %q lacks %s", path, got, h)
			}
		}

		// actual request
		req = httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set(echo.HeaderOrigin, "https://world.concrnt.net")
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s get: status %d", path, rec.Code)
		}
		if got := rec.Header().Get(echo.HeaderAccessControlAllowOrigin); got != "*" {
			t.Fatalf("%s get: allow-origin %q", path, got)
		}
		if got := strings.ToLower(rec.Header().Get(echo.HeaderAccessControlExposeHeaders)); !strings.Contains(got, "atproto-content-labelers") {
			t.Fatalf("%s get: expose-headers %q", path, got)
		}
	}
}
