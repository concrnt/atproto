// Package pds implements the public face of the masqueraded PDS: the sync
// XRPC endpoints and the subscribeRepos firehose (public data), plus what an
// atproto client needs to log in here as its PDS with an app password:
// sessions, a service proxy to the appview / chat service, preferences and
// repo reads. Repo writes are refused except the DM inbound policy record;
// the management API lives in package mgmt.
// Handle resolution is not served here: handles are user-owned domains, so
// the DNS TXT record (or a well-known file on the user's own web server) is
// the user's responsibility.
package pds

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/blobs"
	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

type Server struct {
	db      *gorm.DB
	repos   *repoman.Manager
	blobs   *blobs.Service
	pdsHost string // concrnt FQDN; the atproto serviceEndpoint origin
	version string

	// Sessions, Proxy and Keys enable the client-facing (authenticated)
	// surface. Left nil, those methods answer 501 as before.
	Sessions *Sessions
	Proxy    *Proxy
	Keys     *keys.Service

	// OnFreshConsumer is called when a firehose consumer subscribes without
	// a cursor (a relay's first connection to this host). The bridge uses it
	// to re-announce the active accounts, see repoman.AnnounceActiveAccounts.
	OnFreshConsumer func(ctx context.Context)
}

func NewServer(db *gorm.DB, repos *repoman.Manager, blobSvc *blobs.Service, pdsHost, version string) *Server {
	return &Server{db: db, repos: repos, blobs: blobSvc, pdsHost: pdsHost, version: version}
}

// Register mounts the public PDS routes onto e. The whole surface is
// unauthenticated public data; it shares the listener with the management
// API and both are reached only through the concrnt gateway.
func (s *Server) Register(e *echo.Echo) {
	e.GET("/xrpc/_health", s.handleHealth)
	e.GET("/xrpc/com.atproto.sync.subscribeRepos", s.handleSubscribeRepos)
	e.GET("/xrpc/com.atproto.sync.getRepo", s.handleGetRepo)
	e.GET("/xrpc/com.atproto.sync.getLatestCommit", s.handleGetLatestCommit)
	e.GET("/xrpc/com.atproto.sync.getRepoStatus", s.handleGetRepoStatus)
	e.GET("/xrpc/com.atproto.sync.listRepos", s.handleListRepos)
	e.GET("/xrpc/com.atproto.sync.getRecord", s.handleGetRecord)
	e.GET("/xrpc/com.atproto.sync.getBlob", s.handleGetBlob)
	e.GET("/xrpc/com.atproto.sync.listBlobs", s.handleListBlobs)
	e.GET("/xrpc/com.atproto.server.describeServer", s.handleDescribeServer)

	// Client sessions (app-password login from the official Bluesky app).
	e.POST("/xrpc/com.atproto.server.createSession", s.handleCreateSession)
	e.POST("/xrpc/com.atproto.server.refreshSession", s.handleRefreshSession)
	e.GET("/xrpc/com.atproto.server.getSession", s.handleGetSession)
	e.POST("/xrpc/com.atproto.server.deleteSession", s.handleDeleteSession)
	e.GET("/xrpc/com.atproto.server.getServiceAuth", s.handleGetServiceAuth)
	e.GET("/xrpc/com.atproto.server.listAppPasswords", s.handleListAppPasswords)
	e.GET("/xrpc/com.atproto.identity.resolveHandle", s.handleResolveHandle)

	// Repo reads (public) and the single whitelisted write.
	e.GET("/xrpc/com.atproto.repo.getRecord", s.handleRepoGetRecord)
	e.GET("/xrpc/com.atproto.repo.listRecords", s.handleRepoListRecords)
	e.GET("/xrpc/com.atproto.repo.describeRepo", s.handleRepoDescribeRepo)
	e.POST("/xrpc/com.atproto.repo.createRecord", s.handleRepoPutRecord)
	e.POST("/xrpc/com.atproto.repo.putRecord", s.handleRepoPutRecord)
	e.POST("/xrpc/com.atproto.repo.deleteRecord", s.handleRepoDeleteRecord)
	e.POST("/xrpc/com.atproto.repo.applyWrites", s.handleRepoApplyWrites)

	// Private client preferences, stored here (not in the repo).
	e.GET("/xrpc/app.bsky.actor.getPreferences", s.handleGetPreferences)
	e.POST("/xrpc/app.bsky.actor.putPreferences", s.handlePutPreferences)

	// Any other XRPC method: proxied to the appview / chat service for an
	// authenticated client, otherwise the standard "not implemented" error
	// so crawlers treat this as a read-only host rather than a broken one.
	e.GET("/xrpc/*", s.handleCatchAll)
	e.POST("/xrpc/*", s.handleCatchAll)
}

// handleCatchAll decides what happens to XRPC methods without a local route.
func (s *Server) handleCatchAll(c echo.Context) error {
	nsid := strings.TrimPrefix(c.Request().URL.Path, "/xrpc/")
	if readOnlyMethods[nsid] {
		return s.handleReadOnly(c)
	}
	target := c.Request().Header.Get(proxyHeaderName)
	if s.Proxy == nil || s.Sessions == nil || (target == "" && s.Proxy.defaultTarget(nsid) == "") {
		return s.handleNotImplemented(c)
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return nil
	}
	return s.Proxy.Forward(c, did, nsid, target)
}

// handleResolveHandle answers for bridged handles locally and otherwise
// forwards to the appview (which resolves the whole network).
func (s *Server) handleResolveHandle(c echo.Context) error {
	handle := normalizeIdentifier(c.QueryParam("handle"))
	if handle == "" {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing handle")
	}
	var ent store.Entity
	if err := s.db.Where("handle = ? AND status = ?", handle, "active").First(&ent).Error; err == nil {
		return c.JSON(http.StatusOK, map[string]string{"did": ent.DID})
	}
	if s.Proxy == nil || s.Sessions == nil || bearer(c) == "" {
		return xrpcError(c, http.StatusBadRequest, "HandleNotFound", "handle not hosted here")
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return nil
	}
	return s.Proxy.Forward(c, did, "com.atproto.identity.resolveHandle", s.Proxy.appviewDID+"#bsky_appview")
}

func xrpcError(c echo.Context, status int, name, msg string) error {
	return c.JSON(status, map[string]string{"error": name, "message": msg})
}

func (s *Server) handleHealth(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"version": "concrnt-atproto-bridge " + s.version})
}

func (s *Server) handleNotImplemented(c echo.Context) error {
	return xrpcError(c, http.StatusNotImplemented, "MethodNotImplemented",
		"this host is a read-only bridge PDS; this XRPC method is not available")
}

func (s *Server) handleDescribeServer(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"did":                  "did:web:" + s.pdsHost,
		"availableUserDomains": []string{},
		"inviteCodeRequired":   true,
	})
}

func (s *Server) entityByDID(c echo.Context) (*store.Entity, error) {
	did := c.QueryParam("did")
	if did == "" {
		return nil, xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing did parameter")
	}
	var ent store.Entity
	if err := s.db.Where("did = ?", did).First(&ent).Error; err != nil {
		return nil, xrpcError(c, http.StatusNotFound, "RepoNotFound", fmt.Sprintf("repo %s not hosted here", did))
	}
	return &ent, nil
}
