// Package mgmt serves /cc-info and the management API. It listens on a
// port only the concrnt gateway can reach; the gateway authenticates users
// and propagates their identity via the cc-requester header.
package mgmt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/relay"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

const ServiceName = "world.concrnt.atproto"

// HandleResolver resolves an atproto handle to its DID. Abstracted so tests
// can inject a fake without touching DNS.
type HandleResolver interface {
	ResolveHandle(ctx context.Context, handle string) (did string, err error)
}

// indigoResolver adapts indigo's identity directory. It is uncached: handle
// verification must see the DNS TXT the user just set, not a stale/negative
// cache entry.
type indigoResolver struct {
	dir identity.Directory
}

func (r indigoResolver) ResolveHandle(ctx context.Context, handle string) (string, error) {
	h, err := syntax.ParseHandle(handle)
	if err != nil {
		return "", err
	}
	ident, err := r.dir.LookupHandle(ctx, h)
	if err != nil {
		return "", err
	}
	return ident.DID.String(), nil
}

type Server struct {
	db          *gorm.DB
	repos       *repoman.Manager
	appview     *appview.Client
	resolver    HandleResolver
	pdsHost     string
	relays      []string
	version     string
	serviceCcid string
	graph       graphCache

	// OnProfileInit is called after an account goes active so the outbound
	// layer can push the user's concrnt profile into their new bsky repo.
	OnProfileInit func(ctx context.Context, ent *store.Entity)
}

func NewServer(db *gorm.DB, repos *repoman.Manager, av *appview.Client, pdsHost string, relays []string, version string, serviceCcid string) *Server {
	base := identity.BaseDirectory{}
	return &Server{
		db:          db,
		repos:       repos,
		appview:     av,
		resolver:    indigoResolver{dir: &base},
		pdsHost:     pdsHost,
		relays:      relays,
		version:     version,
		serviceCcid: serviceCcid,
	}
}

// SetResolver overrides the handle resolver (used by tests).
func (s *Server) SetResolver(r HandleResolver) { s.resolver = r }

// Register mounts the management routes onto e. These trust the gateway's
// cc-requester header for authentication, so the bridge listener must only
// be reachable through the concrnt gateway.
func (s *Server) Register(e *echo.Echo) {
	e.GET("/cc-info", s.handleCCInfo)
	e.GET("/atproto/api/info", s.handleInfo)
	e.POST("/atproto/api/setup", s.handleSetup)
	e.POST("/atproto/api/verify", s.handleVerify)
	e.GET("/atproto/api/settings", s.handleGetSettings)
	e.POST("/atproto/api/settings", s.handlePostSettings)
	e.GET("/atproto/api/following", s.handleFollowing)
	e.GET("/atproto/api/followers", s.handleFollowers)
	e.GET("/atproto/api/resolve-actor", s.handleResolveActor)
	e.GET("/atproto/api/resolve", s.handleResolveRecord)
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
			ServiceName + ".verify":       "/atproto/api/verify",
			ServiceName + ".settings":     "/atproto/api/settings",
			ServiceName + ".following":    "/atproto/api/following",
			ServiceName + ".followers":    "/atproto/api/followers{?limit,cursor}",
			ServiceName + ".resolveActor": "/atproto/api/resolve-actor{?target}",
			ServiceName + ".resolve":      "/atproto/api/resolve{?uri}",
		},
	})
}

func (s *Server) handleInfo(c echo.Context) error {
	resp := map[string]any{
		"pdsHost":     s.pdsHost,
		"version":     s.version,
		"serviceCcid": s.serviceCcid,
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

// handleSetup starts bridging: it mints a DID that claims the user's own
// domain as its handle, but leaves the account pending. The user must prove
// domain ownership (a `_atproto.<handle>` DNS TXT record pointing at the DID,
// or an HTTPS well-known) before /atproto/api/verify activates it. No repo,
// firehose event, or relay crawl happens until then.
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
	handle := strings.ToLower(strings.TrimSpace(body.Handle))
	handle = strings.TrimPrefix(handle, "@")
	if _, err := syntax.ParseHandle(handle); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "handle must be a domain you control (e.g. alice.example.com)",
		})
	}

	var count int64
	s.db.Model(&store.Entity{}).Where("handle = ?", handle).Count(&count)
	if count > 0 {
		return c.JSON(http.StatusConflict, map[string]string{"error": "handle already taken"})
	}

	ent, err := s.repos.CreatePending(c.Request().Context(), ccid, handle)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"did":    ent.DID,
		"handle": ent.Handle,
		"status": ent.Status,
		"verification": map[string]any{
			"dns": map[string]string{
				"name":  "_atproto." + ent.Handle,
				"type":  "TXT",
				"value": "did=" + ent.DID,
			},
			// Alternative for users who run a web server on the handle
			// domain: serve the DID as plain text at this URL themselves.
			"https": map[string]string{
				"url":  "https://" + ent.Handle + "/.well-known/atproto-did",
				"body": ent.DID,
			},
			"next": "publish one of the records above on your domain, then POST /atproto/api/verify",
		},
	})
}

// handleVerify confirms the user's handle now resolves to their DID and, on
// success, activates the account: repo init, #identity/#account firehose
// events, profile sync, and a relay crawl request.
func (s *Server) handleVerify(c echo.Context) error {
	ent, herr := s.entityOfRequester(c)
	if ent == nil {
		return herr
	}
	if ent.Status == "active" {
		return c.JSON(http.StatusOK, map[string]any{"did": ent.DID, "handle": ent.Handle, "status": "active"})
	}

	ctx := c.Request().Context()
	resolved, err := s.resolver.ResolveHandle(ctx, ent.Handle)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":  "handle_not_resolvable",
			"detail": fmt.Sprintf("could not resolve %s: %v", ent.Handle, err),
			"hint":   "add the _atproto DNS TXT record (or well-known) and retry; DNS may take a few minutes",
		})
	}
	if resolved != ent.DID {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":  "did_mismatch",
			"detail": fmt.Sprintf("%s resolves to %s, expected %s", ent.Handle, resolved, ent.DID),
		})
	}

	if err := s.repos.Activate(ctx, ent.DID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	go relay.RequestCrawl(context.Background(), s.relays, s.pdsHost)
	if s.OnProfileInit != nil {
		go s.OnProfileInit(context.Background(), ent)
	}

	return c.JSON(http.StatusOK, map[string]any{"did": ent.DID, "handle": ent.Handle, "status": "active"})
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

// handleFollowers proxies app.bsky.graph.getFollowers for the requester's own
// bridged DID, behind a short TTL cache. Followers are never materialized
// locally; the appview is the source of truth.
func (s *Server) handleFollowers(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	if ent.DID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "account has no DID yet"})
	}

	limit := 50
	if l, err := strconv.Atoi(c.QueryParam("limit")); err == nil && l >= 1 && l <= 100 {
		limit = l
	}
	cursor := c.QueryParam("cursor")

	key := fmt.Sprintf("%s\x00%s\x00%d", ent.DID, cursor, limit)
	page := s.graph.get(key)
	if page == nil {
		page, err = s.appview.GetFollowers(c.Request().Context(), ent.DID, limit, cursor)
		if err != nil {
			return c.JSON(http.StatusBadGateway, map[string]string{"error": "appview query failed"})
		}
		s.graph.put(key, page)
	}

	out := make([]map[string]any, 0, len(page.Profiles))
	for _, p := range page.Profiles {
		out = append(out, map[string]any{
			"did":         p.DID,
			"handle":      p.Handle,
			"displayName": p.DisplayName,
			"avatar":      p.Avatar,
		})
	}
	resp := map[string]any{"followers": out}
	if page.Cursor != "" {
		resp["cursor"] = page.Cursor
	}
	return c.JSON(http.StatusOK, resp)
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
		resolved, err := s.resolver.ResolveHandle(ctx, target)
		if err != nil {
			return c.JSON(http.StatusNotFound, map[string]string{"error": fmt.Sprintf("failed to resolve %s", target)})
		}
		did = resolved
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
