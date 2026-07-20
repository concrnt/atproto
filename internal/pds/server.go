// Package pds implements the public face of the masqueraded PDS: the sync
// XRPC endpoints and the subscribeRepos firehose. Everything here is
// unauthenticated public data; the management API lives in package mgmt.
// Handle resolution is not served here: handles are user-owned domains, so
// the DNS TXT record (or a well-known file on the user's own web server) is
// the user's responsibility.
package pds

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/blobs"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

type Server struct {
	db      *gorm.DB
	repos   *repoman.Manager
	blobs   *blobs.Service
	pdsHost string // concrnt FQDN; the atproto serviceEndpoint origin
	version string
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

	// Any other XRPC method: standard "not implemented" error, so crawlers
	// treat this as a read-only host rather than a broken one.
	e.GET("/xrpc/*", s.handleNotImplemented)
	e.POST("/xrpc/*", s.handleNotImplemented)
}

func xrpcError(c echo.Context, status int, name, msg string) error {
	return c.JSON(status, map[string]string{"error": name, "message": msg})
}

func (s *Server) handleHealth(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"version": "concrnt-atproto-bridge " + s.version})
}

func (s *Server) handleNotImplemented(c echo.Context) error {
	return xrpcError(c, http.StatusNotImplemented, "MethodNotImplemented",
		"this host is a read-only bridge PDS; write and session XRPC methods are not available")
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
