// Package mgmt serves /cc-info and the management API. It listens on a
// port only the concrnt gateway can reach; the gateway authenticates users
// and propagates their identity via the cc-requester header.
package mgmt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/relay"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

const ServiceName = "world.concrnt.atproto"

type Server struct {
	db       *gorm.DB
	repos    *repoman.Manager
	appview  *appview.Client
	resolver *identity.CacheDirectory
	pdsHost  string
	relays   []string
	version  string

	// OnProfileInit is called after setup so the outbound layer can push the
	// user's concrnt profile into their new bsky repo.
	OnProfileInit func(ctx context.Context, ent *store.Entity)
}

func NewServer(db *gorm.DB, repos *repoman.Manager, av *appview.Client, pdsHost string, relays []string, version string) *Server {
	base := identity.BaseDirectory{}
	cache := identity.NewCacheDirectory(&base, 10000, time.Hour, 5*time.Minute, 5*time.Minute)
	return &Server{
		db:       db,
		repos:    repos,
		appview:  av,
		resolver: cache,
		pdsHost:  pdsHost,
		relays:   relays,
		version:  version,
	}
}

func (s *Server) Echo() *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())

	e.GET("/cc-info", s.handleCCInfo)
	e.GET("/atproto/api/info", s.handleInfo)
	e.POST("/atproto/api/setup", s.handleSetup)
	e.GET("/atproto/api/settings", s.handleGetSettings)
	e.POST("/atproto/api/settings", s.handlePostSettings)
	e.GET("/atproto/api/following", s.handleFollowing)
	e.GET("/atproto/api/resolve-actor", s.handleResolveActor)
	e.GET("/atproto/api/resolve", s.handleResolveRecord)

	return e
}

// requester extracts the authenticated CCID propagated by the concrnt gateway.
func requester(c echo.Context) string {
	h := c.Request().Header.Get("cc-requester")
	if h == "" {
		return ""
	}
	var ent struct {
		CCID string `json:"ccid"`
	}
	if err := json.Unmarshal([]byte(h), &ent); err != nil {
		return ""
	}
	return ent.CCID
}

func (s *Server) handleCCInfo(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"name":    ServiceName,
		"version": s.version,
		"endpoints": map[string]string{
			ServiceName + ".info":         "/atproto/api/info",
			ServiceName + ".setup":        "/atproto/api/setup",
			ServiceName + ".settings":     "/atproto/api/settings",
			ServiceName + ".following":    "/atproto/api/following",
			ServiceName + ".resolveActor": "/atproto/api/resolve-actor{?target}",
			ServiceName + ".resolve":      "/atproto/api/resolve{?uri}",
		},
	})
}

func (s *Server) handleInfo(c echo.Context) error {
	resp := map[string]any{
		"pdsHost":      s.pdsHost,
		"handleDomain": s.pdsHost,
		"version":      s.version,
	}
	if ccid := requester(c); ccid != "" {
		var ent store.Entity
		if err := s.db.Where("ccid = ?", ccid).First(&ent).Error; err == nil {
			resp["entity"] = map[string]any{
				"did":     ent.DID,
				"handle":  ent.Handle,
				"enabled": ent.Enabled,
				"status":  ent.Status,
			}
		}
	}
	return c.JSON(http.StatusOK, resp)
}

var handleLocalRe = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func (s *Server) handleSetup(c echo.Context) error {
	ccid := requester(c)
	if ccid == "" {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "authentication required"})
	}

	var body struct {
		Handle string `json:"handle"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid body"})
	}
	local := strings.ToLower(strings.TrimSpace(body.Handle))
	if !handleLocalRe.MatchString(local) {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "handle must be 1-63 chars of lowercase letters, digits and hyphens",
		})
	}
	handle := local + "." + s.pdsHost

	var count int64
	s.db.Model(&store.Entity{}).Where("handle = ?", handle).Count(&count)
	if count > 0 {
		return c.JSON(http.StatusConflict, map[string]string{"error": "handle already taken"})
	}

	ent, err := s.repos.CreateAccount(c.Request().Context(), ccid, handle)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	go relay.RequestCrawl(context.Background(), s.relays, s.pdsHost)
	if s.OnProfileInit != nil {
		go s.OnProfileInit(context.Background(), ent)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"did":    ent.DID,
		"handle": ent.Handle,
	})
}

func (s *Server) entityOfRequester(c echo.Context) (*store.Entity, error) {
	ccid := requester(c)
	if ccid == "" {
		return nil, c.JSON(http.StatusUnauthorized, map[string]string{"error": "authentication required"})
	}
	var ent store.Entity
	if err := s.db.Where("ccid = ?", ccid).First(&ent).Error; err != nil {
		return nil, c.JSON(http.StatusNotFound, map[string]string{"error": "not bridged; call setup first"})
	}
	return &ent, nil
}

func (s *Server) handleGetSettings(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{
		"enabled":         ent.Enabled,
		"listenTimelines": ent.ListenTimelines,
	})
}

func (s *Server) handlePostSettings(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	var body struct {
		Enabled         *bool     `json:"enabled"`
		ListenTimelines *[]string `json:"listenTimelines"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid body"})
	}
	updates := map[string]any{}
	if body.Enabled != nil {
		updates["enabled"] = *body.Enabled
	}
	if body.ListenTimelines != nil {
		updates["listen_timelines"] = datatypes.NewJSONSlice(*body.ListenTimelines)
	}
	if len(updates) > 0 {
		if err := s.db.Model(&store.Entity{}).Where("uid = ?", ent.Uid).Updates(updates).Error; err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "update failed"})
		}
	}
	return s.handleGetSettings(c)
}

func (s *Server) handleFollowing(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	var follows []store.Follow
	if err := s.db.Where("ccid = ?", ent.CCID).Order("created_at desc").Find(&follows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "query failed"})
	}
	out := make([]map[string]any, 0, len(follows))
	for _, f := range follows {
		entry := map[string]any{"did": f.SubjectDID}
		var actor store.Actor
		if err := s.db.Where("did = ?", f.SubjectDID).First(&actor).Error; err == nil {
			entry["handle"] = actor.Handle
			entry["displayName"] = actor.DisplayName
			entry["avatar"] = actor.AvatarURL
		}
		out = append(out, entry)
	}
	return c.JSON(http.StatusOK, map[string]any{"following": out})
}

// handleResolveActor resolves a handle or DID into a profile preview, for
// follow UIs.
func (s *Server) handleResolveActor(c echo.Context) error {
	target := strings.TrimSpace(c.QueryParam("target"))
	target = strings.TrimPrefix(target, "@")
	if target == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "missing target"})
	}

	ctx := c.Request().Context()
	var did string
	if strings.HasPrefix(target, "did:") {
		did = target
	} else {
		h, err := syntax.ParseHandle(target)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid handle"})
		}
		ident, err := s.resolver.LookupHandle(ctx, h)
		if err != nil {
			return c.JSON(http.StatusNotFound, map[string]string{"error": fmt.Sprintf("failed to resolve %s", target)})
		}
		did = ident.DID.String()
	}

	profile, err := s.appview.GetProfile(ctx, did)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "profile not found"})
	}
	return c.JSON(http.StatusOK, profile)
}

// handleResolveRecord proxies an at-uri to the appview so clients can render
// atproto/record.json references.
func (s *Server) handleResolveRecord(c echo.Context) error {
	uri := c.QueryParam("uri")
	if !strings.HasPrefix(uri, "at://") {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "uri must be an at:// uri"})
	}
	posts, err := s.appview.GetPosts(c.Request().Context(), []string{uri})
	if err != nil || len(posts) == 0 {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "post not found"})
	}
	return c.JSON(http.StatusOK, posts[0])
}
