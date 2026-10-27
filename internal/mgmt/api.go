// Package mgmt serves /cc-info and the management API. It listens on a
// port only the concrnt gateway can reach; the gateway authenticates users
// and propagates their identity via the cc-requester header.
package mgmt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/atproto/internal/apppass"
	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/inbound"
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

	// ActorTTL bounds how old an at_actors row may be before the following
	// list refreshes it from the appview. Shared meaning with the ingester's
	// actor cache TTL.
	ActorTTL time.Duration

	// OnProfileInit is called after an account goes active so the outbound
	// layer can push the user's concrnt profile into their new bsky repo.
	OnProfileInit func(ctx context.Context, ent *store.Entity)

	// Importer mirrors a post on demand for /atproto/api/import (the inbound
	// ingester in production). nil answers that endpoint with 501.
	Importer PostImporter
}

// PostImporter materializes a Bluesky post as a concrnt record and returns
// its cckv key. It reports inbound.ErrPostNotFound for unknown at-uris.
type PostImporter interface {
	ImportPost(ctx context.Context, atURI string) (string, error)
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
		ActorTTL:    time.Hour,
	}
}

// SetResolver overrides the handle resolver (used by tests).
func (s *Server) SetResolver(r HandleResolver) { s.resolver = r }

// Register mounts the management routes onto e. These trust the gateway's
// cc-requester header for authentication, so the bridge listener must only
// be reachable through the concrnt gateway.
func (s *Server) Register(e *echo.Echo) {
	e.GET("/cc-info", s.handleCCInfo)
	// Operational endpoints. The gateway only proxies /atproto and /xrpc, so
	// these stay off the public origin: /health is the liveness probe and
	// /metrics is scraped on the pod port directly. The default registry
	// carries the Go/process collectors plus the concrnt client's peer
	// request series (registered by the client package itself).
	e.GET("/health", handleHealth)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	e.GET("/atproto/api/info", s.handleInfo)
	e.POST("/atproto/api/setup", s.handleSetup)
	e.POST("/atproto/api/verify", s.handleVerify)
	e.POST("/atproto/api/announce", s.handleAnnounce)
	e.GET("/atproto/api/following", s.handleFollowing)
	e.GET("/atproto/api/followers", s.handleFollowers)
	e.GET("/atproto/api/resolve-actor", s.handleResolveActor)
	e.GET("/atproto/api/resolve", s.handleResolveRecord)
	e.GET("/atproto/api/author-feed", s.handleAuthorFeed)
	e.POST("/atproto/api/import", s.handleImport)
	e.GET("/atproto/api/app-passwords", s.handleListAppPasswords)
	e.POST("/atproto/api/app-passwords", s.handleCreateAppPassword)
	e.DELETE("/atproto/api/app-passwords/:id", s.handleRevokeAppPassword)
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

func handleHealth(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCCInfo(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"name":    ServiceName,
		"version": s.version,
		"endpoints": map[string]string{
			ServiceName + ".info":         "/atproto/api/info",
			ServiceName + ".setup":        "/atproto/api/setup",
			ServiceName + ".verify":       "/atproto/api/verify",
			ServiceName + ".announce":     "/atproto/api/announce",
			ServiceName + ".following":    "/atproto/api/following",
			ServiceName + ".followers":    "/atproto/api/followers{?limit,cursor}",
			ServiceName + ".resolveActor": "/atproto/api/resolve-actor{?target}",
			ServiceName + ".resolve":      "/atproto/api/resolve{?uri}",
			ServiceName + ".authorFeed":   "/atproto/api/author-feed{?actor,limit,cursor}",
			ServiceName + ".import":       "/atproto/api/import",
			// App passwords: log the official Bluesky app in to this bridge
			// as the user's PDS (DMs); repo stays read-only.
			ServiceName + ".appPasswords":      "/atproto/api/app-passwords",
			ServiceName + ".createAppPassword": "/atproto/api/app-passwords",
			ServiceName + ".revokeAppPassword": "/atproto/api/app-passwords/{id}",
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
			// What to type into the official app's login form.
			resp["login"] = map[string]string{
				"service":    "https://" + s.pdsHost,
				"identifier": ent.Handle,
			}
		}
	}
	return c.JSON(http.StatusOK, resp)
}

func appPasswordJSON(r store.AppPassword) map[string]any {
	m := map[string]any{
		"id":        r.ID,
		"name":      r.Name,
		"createdAt": r.CreatedAt.UTC().Format(time.RFC3339),
	}
	if r.LastUsedAt != nil {
		m["lastUsedAt"] = r.LastUsedAt.UTC().Format(time.RFC3339)
	}
	return m
}

func (s *Server) handleListAppPasswords(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	rows, err := apppass.List(s.db, ent.DID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		out = append(out, appPasswordJSON(r))
	}
	return c.JSON(http.StatusOK, map[string]any{"appPasswords": out})
}

// handleCreateAppPassword issues a credential for logging the official
// Bluesky app in here. The plaintext is returned once and never stored.
func (s *Server) handleCreateAppPassword(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	if ent.Status != "active" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "account is not active; call verify first"})
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid body"})
	}
	row, plain, err := apppass.Create(s.db, ent.DID, body.Name)
	switch {
	case errors.Is(err, apppass.ErrInvalidName):
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, apppass.ErrNameTaken):
		return c.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, apppass.ErrTooMany):
		return c.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
	case err != nil:
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	resp := appPasswordJSON(*row)
	resp["password"] = plain
	resp["login"] = map[string]string{
		"service":    "https://" + s.pdsHost,
		"identifier": ent.Handle,
	}
	return c.JSON(http.StatusCreated, resp)
}

func (s *Server) handleRevokeAppPassword(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	if err := apppass.Revoke(s.db, ent.DID, c.Param("id")); err != nil {
		if errors.Is(err, apppass.ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "app password not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]any{"revoked": true})
}

// handleSetup starts bridging: it mints a DID that claims the user's own
// domain as its handle, but leaves the account pending. The user must prove
// domain ownership (a `_atproto.<handle>` DNS TXT record pointing at the DID,
// or an HTTPS well-known) before /atproto/api/verify activates it. No repo,
// firehose event, or relay crawl happens until then.
// While still pending, calling it again with another handle swaps the
// claimed domain on the same DID (so the DNS value stays valid).
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

	var own store.Entity
	hasOwn := s.db.Where("ccid = ?", ccid).First(&own).Error == nil
	if hasOwn && own.Status != "pending" {
		return c.JSON(http.StatusConflict, map[string]string{"error": "account is already active"})
	}

	var ent *store.Entity
	if hasOwn && own.Handle == handle {
		ent = &own
	} else {
		var count int64
		s.db.Model(&store.Entity{}).Where("handle = ?", handle).Count(&count)
		if count > 0 {
			return c.JSON(http.StatusConflict, map[string]string{"error": "handle already taken"})
		}
		var err error
		if hasOwn {
			// Still unverified: re-point the same DID at the new domain.
			ent, err = s.repos.ChangePendingHandle(c.Request().Context(), own.DID, handle)
		} else {
			ent, err = s.repos.CreatePending(c.Request().Context(), ccid, handle)
		}
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
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

	// Ask the relays to subscribe before the activation events are emitted:
	// a relay only receives events from the moment it connects, so for this
	// host's first account the #identity/#account/#commit would otherwise
	// precede the relay. The cursor-less-consumer re-announcement
	// (repoman.AnnounceActiveAccounts) covers the remaining connect latency.
	crawlCtx, cancelCrawl := context.WithTimeout(ctx, 10*time.Second)
	relay.RequestCrawl(crawlCtx, s.relays, s.pdsHost)
	cancelCrawl()

	if err := s.repos.Activate(ctx, ent.DID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if s.OnProfileInit != nil {
		go s.OnProfileInit(context.Background(), ent)
	}

	return c.JSON(http.StatusOK, map[string]any{"did": ent.DID, "handle": ent.Handle, "status": "active"})
}

// handleAnnounce re-pushes everything the network should know about the
// caller: #identity/#account on the firehose, a relay crawl request, and the
// concrnt profile into the bsky repo (a new #commit). For when the appview
// shows handle.invalid (its handle check failed or ran before DNS
// propagated), a relay missed the activation, or the profile never made it
// across: the appview re-verifies on #identity and re-indexes on #commit.
func (s *Server) handleAnnounce(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	if ent.Status != "active" {
		return c.JSON(http.StatusConflict, map[string]string{"error": "account is not active; call verify first"})
	}
	ctx := c.Request().Context()
	if err := s.repos.AnnounceAccount(ctx, ent.DID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	go relay.RequestCrawl(context.Background(), s.relays, s.pdsHost)
	if s.OnProfileInit != nil {
		go s.OnProfileInit(context.Background(), ent)
	}
	return c.JSON(http.StatusOK, map[string]any{"did": ent.DID, "handle": ent.Handle, "status": ent.Status})
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

func (s *Server) handleFollowing(c echo.Context) error {
	ent, err := s.entityOfRequester(c)
	if ent == nil {
		return err
	}
	var follows []store.Follow
	if err := s.db.Where("ccid = ?", ent.CCID).Order("created_at desc").Find(&follows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "query failed"})
	}
	actors := s.followingActors(c.Request().Context(), follows)
	out := make([]map[string]any, 0, len(follows))
	for _, f := range follows {
		entry := map[string]any{"did": f.SubjectDID}
		if actor, ok := actors[f.SubjectDID]; ok {
			entry["handle"] = actor.Handle
			entry["displayName"] = actor.DisplayName
			entry["avatar"] = actor.AvatarURL
		}
		out = append(out, entry)
	}
	return c.JSON(http.StatusOK, map[string]any{"following": out})
}

// followingActors returns the cached display info for every followed DID.
// at_actors is otherwise only filled when a followee's post is ingested, so
// a followee who has not posted since the follow would render as a bare DID;
// missing or stale rows are hydrated from the appview in batches and written
// back so later requests are served from the cache. Appview failures keep
// whatever is cached (possibly stale) rather than failing the listing.
func (s *Server) followingActors(ctx context.Context, follows []store.Follow) map[string]store.Actor {
	actors := map[string]store.Actor{}
	if len(follows) == 0 {
		return actors
	}
	dids := make([]string, 0, len(follows))
	for _, f := range follows {
		dids = append(dids, f.SubjectDID)
	}
	var cached []store.Actor
	s.db.Where("did IN ?", dids).Find(&cached)
	for _, a := range cached {
		actors[a.DID] = a
	}

	var refresh []string
	for _, did := range dids {
		if a, ok := actors[did]; !ok || time.Since(a.FetchedAt) > s.ActorTTL {
			refresh = append(refresh, did)
		}
	}
	if len(refresh) == 0 || s.appview == nil {
		return actors
	}

	const batch = 25
	for start := 0; start < len(refresh); start += batch {
		end := start + batch
		if end > len(refresh) {
			end = len(refresh)
		}
		profiles, err := s.appview.GetProfiles(ctx, refresh[start:end])
		if err != nil {
			slog.Warn("failed to hydrate followed actors", "count", end-start, "error", err)
			continue
		}
		for _, p := range profiles {
			actor := store.Actor{
				DID:         p.DID,
				Handle:      p.Handle,
				DisplayName: p.DisplayName,
				AvatarURL:   p.Avatar,
				FetchedAt:   time.Now(),
			}
			s.db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "did"}},
				UpdateAll: true,
			}).Create(&actor)
			actors[actor.DID] = actor
		}
	}
	return actors
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
// handleResolveRecord returns the appview post view for an at-uri. When the
// post is one this bridge exported from concrnt, the view is annotated with
// `concrntURI` (the original cckv post) so clients can render it natively,
// the way the AP bridge's note URLs embed the concrnt URI.
func (s *Server) handleResolveRecord(c echo.Context) error {
	uri := c.QueryParam("uri")
	if !strings.HasPrefix(uri, "at://") {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "uri must be an at:// uri"})
	}
	posts, err := s.appview.GetPosts(c.Request().Context(), []string{uri})
	if err != nil || len(posts) == 0 {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "post not found"})
	}
	view := posts[0]
	if ccURI := s.exportedPostCcURI(uri); ccURI != "" {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(view, &m); err == nil {
			if enc, err := json.Marshal(ccURI); err == nil {
				m["concrntURI"] = enc
				if annotated, err := json.Marshal(m); err == nil {
					view = annotated
				}
			}
		}
	}
	return c.JSONBlob(http.StatusOK, view)
}

// handleImport mirrors a Bluesky post into concrnt on demand (POST {uri})
// and answers {key} with the record's cckv URI, the ActivityPub bridge's
// import endpoint for notes. Any authenticated user may ask; the record is
// signed by the service account and distributed to no timeline, so the
// request only makes the post addressable for the native detail view.
func (s *Server) handleImport(c echo.Context) error {
	if requester(c) == "" {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "authentication required"})
	}
	var body struct {
		URI string `json:"uri"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}
	uri := strings.TrimSpace(body.URI)
	parsed, err := syntax.ParseATURI(uri)
	if err != nil || parsed.Collection() != "app.bsky.feed.post" || parsed.RecordKey() == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "uri must be an at:// uri of an app.bsky.feed.post record"})
	}
	if s.Importer == nil {
		return c.JSON(http.StatusNotImplemented, map[string]string{"error": "import is not available"})
	}

	key, err := s.Importer.ImportPost(c.Request().Context(), uri)
	if err != nil {
		if errors.Is(err, inbound.ErrPostNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "post not found"})
		}
		slog.Warn("import failed", "atUri", uri, "requester", requester(c), "error", err)
		return c.JSON(http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("failed to import post: %v", err)})
	}
	slog.Info("imported post", "atUri", uri, "key", key, "requester", requester(c))
	return c.JSON(http.StatusOK, map[string]string{"key": key})
}

// exportedPostCcURI resolves the at-uri of a post this bridge exported to
// its concrnt post URI, "" when the post was not exported by the bridge.
func (s *Server) exportedPostCcURI(atURI string) string {
	var m store.URIMap
	if err := s.db.Where("at_uri = ? AND ref_type = ?", atURI, "outbound-post").First(&m).Error; err != nil {
		return ""
	}
	return m.CcURI
}

// handleAuthorFeed proxies app.bsky.feed.getAuthorFeed for any actor so
// clients can show a Bluesky user's past posts under their profile, the way
// the AP person page walks a remote outbox. Each item carries the appview
// post view (annotated with `concrntURI` when this bridge exported it, like
// resolve) and the repost reason; the hydrated reply context is dropped since
// clients resolve the parent themselves. The appview cursor is echoed back
// untouched.
func (s *Server) handleAuthorFeed(c echo.Context) error {
	actor := strings.TrimSpace(c.QueryParam("actor"))
	actor = strings.TrimPrefix(actor, "@")
	if actor == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "missing actor"})
	}

	ctx := c.Request().Context()
	var did string
	if strings.HasPrefix(actor, "did:") {
		did = actor
	} else {
		resolved, err := s.resolver.ResolveHandle(ctx, actor)
		if err != nil {
			return c.JSON(http.StatusNotFound, map[string]string{"error": fmt.Sprintf("failed to resolve %s", actor)})
		}
		did = resolved
	}

	limit := 30
	if l, err := strconv.Atoi(c.QueryParam("limit")); err == nil && l >= 1 && l <= 100 {
		limit = l
	}
	cursor := c.QueryParam("cursor")

	page, err := s.appview.GetAuthorFeed(ctx, did, limit, cursor)
	if err != nil {
		return c.JSON(http.StatusBadGateway, map[string]string{"error": "appview query failed"})
	}

	type feedItem struct {
		post   map[string]json.RawMessage
		reason json.RawMessage
		uri    string
	}
	items := make([]feedItem, 0, len(page.Items))
	uris := make([]string, 0, len(page.Items))
	for _, raw := range page.Items {
		var item struct {
			Post   map[string]json.RawMessage `json:"post"`
			Reason json.RawMessage            `json:"reason"`
		}
		if err := json.Unmarshal(raw, &item); err != nil || item.Post == nil {
			continue
		}
		var uri string
		if err := json.Unmarshal(item.Post["uri"], &uri); err != nil || uri == "" {
			continue
		}
		items = append(items, feedItem{post: item.Post, reason: item.Reason, uri: uri})
		uris = append(uris, uri)
	}

	exported := s.exportedPostCcURIs(uris)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if ccURI, ok := exported[item.uri]; ok {
			if enc, err := json.Marshal(ccURI); err == nil {
				item.post["concrntURI"] = enc
			}
		}
		entry := map[string]any{"post": item.post}
		if len(item.reason) > 0 {
			entry["reason"] = item.reason
		}
		out = append(out, entry)
	}
	resp := map[string]any{"feed": out}
	if page.Cursor != "" {
		resp["cursor"] = page.Cursor
	}
	return c.JSON(http.StatusOK, resp)
}

// exportedPostCcURIs is the batch form of exportedPostCcURI: at-uri → concrnt
// post URI for every uri this bridge exported, in one query.
func (s *Server) exportedPostCcURIs(atURIs []string) map[string]string {
	result := map[string]string{}
	if len(atURIs) == 0 {
		return result
	}
	var rows []store.URIMap
	if err := s.db.Where("at_uri IN ? AND ref_type = ?", atURIs, "outbound-post").Find(&rows).Error; err != nil {
		return result
	}
	for _, m := range rows {
		result[m.AtURI] = m.CcURI
	}
	return result
}
