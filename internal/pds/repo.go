package pds

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/bluesky-social/indigo/api/chat"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/store"
)

// The account's repo is written by the outbound daemon from concrnt
// activity; atproto clients get read access plus exactly one writable
// record: the DM inbound policy, which has no concrnt counterpart.
const (
	declarationCollection = "chat.bsky.actor.declaration"
	declarationRkey       = "self"
	readOnlyErrorName     = "ReadOnlyBridge"
	readOnlyErrorMsg      = "read-only bridge: posts, likes, follows and profile are managed on concrnt"
)

// readOnlyMethods are answered with the read-only error whether or not the
// caller is authenticated, so clients get a clear message instead of 501.
var readOnlyMethods = map[string]bool{
	"com.atproto.repo.uploadBlob":                       true,
	"com.atproto.repo.importRepo":                       true,
	"com.atproto.server.createAccount":                  true,
	"com.atproto.server.deleteAccount":                  true,
	"com.atproto.server.deactivateAccount":              true,
	"com.atproto.server.activateAccount":                true,
	"com.atproto.server.updateEmail":                    true,
	"com.atproto.server.requestEmailUpdate":             true,
	"com.atproto.server.requestEmailConfirmation":       true,
	"com.atproto.server.confirmEmail":                   true,
	"com.atproto.server.requestPasswordReset":           true,
	"com.atproto.server.resetPassword":                  true,
	"com.atproto.server.createAppPassword":              true,
	"com.atproto.server.revokeAppPassword":              true,
	"com.atproto.server.createInviteCode":               true,
	"com.atproto.server.createInviteCodes":              true,
	"com.atproto.identity.updateHandle":                 true,
	"com.atproto.identity.submitPlcOperation":           true,
	"com.atproto.identity.requestPlcOperationSignature": true,
	"com.atproto.identity.signPlcOperation":             true,
	"com.atproto.identity.getRecommendedDidCredentials": true,
}

func (s *Server) handleReadOnly(c echo.Context) error {
	return xrpcError(c, http.StatusForbidden, readOnlyErrorName, readOnlyErrorMsg)
}

// entityByRepo resolves the `repo` parameter (handle or DID) of the
// com.atproto.repo read methods.
func (s *Server) entityByRepo(c echo.Context) (*store.Entity, error) {
	id := normalizeIdentifier(c.QueryParam("repo"))
	if id == "" {
		return nil, xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing repo parameter")
	}
	var ent store.Entity
	if err := s.db.Where("did = ? OR handle = ?", id, id).First(&ent).Error; err != nil {
		return nil, xrpcError(c, http.StatusNotFound, "RepoNotFound", "repo "+id+" not hosted here")
	}
	return &ent, nil
}

func (s *Server) handleRepoGetRecord(c echo.Context) error {
	ent, err := s.entityByRepo(c)
	if ent == nil {
		return err
	}
	collection, rkey := c.QueryParam("collection"), c.QueryParam("rkey")
	if collection == "" || rkey == "" {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing collection or rkey")
	}
	cc, val, err := s.repos.GetRecordRaw(c.Request().Context(), ent.DID, collection, rkey)
	if err != nil {
		return xrpcError(c, http.StatusNotFound, "RecordNotFound", "record not found")
	}
	return c.JSON(http.StatusOK, map[string]any{
		"uri":   "at://" + ent.DID + "/" + collection + "/" + rkey,
		"cid":   cc,
		"value": val,
	})
}

func (s *Server) handleRepoListRecords(c echo.Context) error {
	ent, err := s.entityByRepo(c)
	if ent == nil {
		return err
	}
	collection := c.QueryParam("collection")
	if collection == "" {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing collection")
	}
	limit := 50
	if raw := c.QueryParam("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "limit must be 1..100")
		}
		limit = n
	}
	reverse := c.QueryParam("reverse") == "true"
	items, next, err := s.repos.ListRecords(c.Request().Context(), ent.DID, collection, limit, c.QueryParam("cursor"), reverse)
	if err != nil {
		return xrpcError(c, http.StatusNotFound, "RepoNotFound", "no repo")
	}
	records := make([]map[string]any, 0, len(items))
	for _, it := range items {
		records = append(records, map[string]any{
			"uri":   "at://" + ent.DID + "/" + collection + "/" + it.Rkey,
			"cid":   it.CID,
			"value": it.Value,
		})
	}
	resp := map[string]any{"records": records}
	if next != "" {
		resp["cursor"] = next
	}
	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handleRepoDescribeRepo(c echo.Context) error {
	ent, err := s.entityByRepo(c)
	if ent == nil {
		return err
	}
	collections, err := s.repos.ListCollections(c.Request().Context(), ent.DID)
	if err != nil {
		collections = []string{}
	}
	if collections == nil {
		collections = []string{}
	}
	resp := map[string]any{
		"handle":          ent.Handle,
		"did":             ent.DID,
		"collections":     collections,
		"handleIsCorrect": true,
	}
	if doc, err := s.didDocFor(ent); err == nil {
		resp["didDoc"] = doc
	}
	return c.JSON(http.StatusOK, resp)
}

// ---- writes ----

// writeTarget is one repo write the client asked for, after the
// collection/rkey whitelist check.
type writeTarget struct {
	op     string // create / update / delete
	record *chat.ActorDeclaration
}

func parseDeclaration(raw json.RawMessage) (*chat.ActorDeclaration, error) {
	var rec chat.ActorDeclaration
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, errors.New("record is not a chat.bsky.actor.declaration")
	}
	switch rec.AllowIncoming {
	case "all", "none", "following":
	default:
		return nil, errors.New("allowIncoming must be all, none or following")
	}
	rec.LexiconTypeID = declarationCollection
	return &rec, nil
}

// allowedWrite reports whether (collection, rkey) is the one record clients
// may write. An empty rkey (createRecord) is accepted and pinned to "self".
func allowedWrite(collection, rkey string) bool {
	return collection == declarationCollection && (rkey == "" || rkey == declarationRkey)
}

// authedRepoWrite authenticates the caller and checks the body's repo is
// the caller's own. It writes the error response itself when returning "".
func (s *Server) authedRepoWrite(c echo.Context, repo string) string {
	if s.Sessions == nil {
		_ = s.handleNotImplemented(c)
		return ""
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return ""
	}
	repo = normalizeIdentifier(repo)
	if repo != did {
		var ent store.Entity
		if err := s.db.Where("did = ?", did).First(&ent).Error; err != nil || repo != ent.Handle {
			_ = xrpcError(c, http.StatusForbidden, "InvalidRequest", "repo must be the authenticated account")
			return ""
		}
	}
	return did
}

func (s *Server) writeResult(c echo.Context, did, collection, rkey, cc string) error {
	resp := map[string]any{
		"uri": "at://" + did + "/" + collection + "/" + rkey,
		"cid": cc,
	}
	if rev, err := s.repos.Rev(c.Request().Context(), did); err == nil {
		resp["commit"] = map[string]any{"rev": rev}
	}
	return c.JSON(http.StatusOK, resp)
}

// handleRepoPutRecord serves createRecord and putRecord alike.
func (s *Server) handleRepoPutRecord(c echo.Context) error {
	var body struct {
		Repo       string          `json:"repo"`
		Collection string          `json:"collection"`
		Rkey       string          `json:"rkey"`
		Record     json.RawMessage `json:"record"`
	}
	if err := c.Bind(&body); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "invalid body")
	}
	if !allowedWrite(body.Collection, body.Rkey) {
		return s.handleReadOnly(c)
	}
	did := s.authedRepoWrite(c, body.Repo)
	if did == "" {
		return nil
	}
	rec, err := parseDeclaration(body.Record)
	if err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRecord", err.Error())
	}
	_, cc, err := s.repos.PutRecord(c.Request().Context(), did, declarationCollection, declarationRkey, rec)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
	return s.writeResult(c, did, declarationCollection, declarationRkey, cc)
}

func (s *Server) handleRepoDeleteRecord(c echo.Context) error {
	var body struct {
		Repo       string `json:"repo"`
		Collection string `json:"collection"`
		Rkey       string `json:"rkey"`
	}
	if err := c.Bind(&body); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "invalid body")
	}
	if !allowedWrite(body.Collection, body.Rkey) || body.Rkey == "" {
		return s.handleReadOnly(c)
	}
	did := s.authedRepoWrite(c, body.Repo)
	if did == "" {
		return nil
	}
	if err := s.repos.DeleteRecord(c.Request().Context(), did, declarationCollection, declarationRkey); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.JSON(http.StatusOK, map[string]any{})
		}
		// A missing record is not an error for delete.
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such") {
			return c.JSON(http.StatusOK, map[string]any{})
		}
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
	resp := map[string]any{}
	if rev, err := s.repos.Rev(c.Request().Context(), did); err == nil {
		resp["commit"] = map[string]any{"rev": rev}
	}
	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handleRepoApplyWrites(c echo.Context) error {
	var body struct {
		Repo   string            `json:"repo"`
		Writes []json.RawMessage `json:"writes"`
	}
	if err := c.Bind(&body); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "invalid body")
	}
	if len(body.Writes) == 0 {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "writes is empty")
	}
	// Validate every element first: a batch is all-or-nothing.
	targets := make([]writeTarget, 0, len(body.Writes))
	for _, raw := range body.Writes {
		var w struct {
			Type       string          `json:"$type"`
			Collection string          `json:"collection"`
			Rkey       string          `json:"rkey"`
			Value      json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "invalid write element")
		}
		if !allowedWrite(w.Collection, w.Rkey) {
			return s.handleReadOnly(c)
		}
		switch w.Type {
		case "com.atproto.repo.applyWrites#create", "com.atproto.repo.applyWrites#update":
			rec, err := parseDeclaration(w.Value)
			if err != nil {
				return xrpcError(c, http.StatusBadRequest, "InvalidRecord", err.Error())
			}
			targets = append(targets, writeTarget{op: strings.TrimPrefix(w.Type, "com.atproto.repo.applyWrites#"), record: rec})
		case "com.atproto.repo.applyWrites#delete":
			if w.Rkey == "" {
				return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "delete needs rkey")
			}
			targets = append(targets, writeTarget{op: "delete"})
		default:
			return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "unknown write type "+w.Type)
		}
	}
	did := s.authedRepoWrite(c, body.Repo)
	if did == "" {
		return nil
	}
	ctx := c.Request().Context()
	results := make([]map[string]any, 0, len(targets))
	for _, t := range targets {
		uri := "at://" + did + "/" + declarationCollection + "/" + declarationRkey
		switch t.op {
		case "delete":
			if err := s.repos.DeleteRecord(ctx, did, declarationCollection, declarationRkey); err != nil &&
				!strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "no such") {
				return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
			}
			results = append(results, map[string]any{"$type": "com.atproto.repo.applyWrites#deleteResult"})
		default:
			_, cc, err := s.repos.PutRecord(ctx, did, declarationCollection, declarationRkey, t.record)
			if err != nil {
				return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
			}
			results = append(results, map[string]any{
				"$type": "com.atproto.repo.applyWrites#" + t.op + "Result",
				"uri":   uri,
				"cid":   cc,
			})
		}
	}
	resp := map[string]any{"results": results}
	if rev, err := s.repos.Rev(ctx, did); err == nil {
		resp["commit"] = map[string]any{"rev": rev}
	}
	return c.JSON(http.StatusOK, resp)
}
