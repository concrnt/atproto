package pds

import (
	"net/http"
	"strconv"

	"github.com/bluesky-social/indigo/carstore"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"

	"github.com/concrnt/atproto/internal/store"
)

func (s *Server) handleGetRepo(c echo.Context) error {
	ent, err := s.entityByDID(c)
	if ent == nil {
		return err
	}
	since := c.QueryParam("since")

	c.Response().Header().Set(echo.HeaderContentType, "application/vnd.ipld.car")
	c.Response().WriteHeader(http.StatusOK)
	return s.repos.CarStore().ReadUserCar(c.Request().Context(), ent.Uid, since, true, c.Response())
}

func (s *Server) handleGetLatestCommit(c echo.Context) error {
	ent, err := s.entityByDID(c)
	if ent == nil {
		return err
	}
	head, err := s.repos.CarStore().GetUserRepoHead(c.Request().Context(), ent.Uid)
	if err != nil {
		return xrpcError(c, http.StatusNotFound, "RepoNotFound", "no repo head")
	}
	rev, err := s.repos.CarStore().GetUserRepoRev(c.Request().Context(), ent.Uid)
	if err != nil {
		return xrpcError(c, http.StatusNotFound, "RepoNotFound", "no repo rev")
	}
	return c.JSON(http.StatusOK, map[string]string{"cid": head.String(), "rev": rev})
}

func (s *Server) handleGetRepoStatus(c echo.Context) error {
	ent, err := s.entityByDID(c)
	if ent == nil {
		return err
	}
	resp := map[string]any{
		"did":    ent.DID,
		"active": ent.Status == "active" && ent.Enabled,
	}
	if ent.Rev != "" {
		resp["rev"] = ent.Rev
	}
	if ent.Status != "active" {
		resp["status"] = ent.Status
	}
	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handleListRepos(c echo.Context) error {
	limit := 500
	if l := c.QueryParam("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 1000 {
			limit = v
		}
	}
	var afterUid uint64
	if cur := c.QueryParam("cursor"); cur != "" {
		if v, err := strconv.ParseUint(cur, 10, 64); err == nil {
			afterUid = v
		}
	}

	// Only active accounts have an initialized repo; pending ones (awaiting
	// handle verification) must not appear to relays as crawlable.
	var ents []store.Entity
	if err := s.db.Where("uid > ? AND did <> '' AND status = ?", afterUid, "active").Order("uid").Limit(limit).Find(&ents).Error; err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalError", "query failed")
	}

	repos := make([]map[string]any, 0, len(ents))
	for _, ent := range ents {
		repos = append(repos, map[string]any{
			"did":    ent.DID,
			"head":   ent.HeadCID,
			"rev":    ent.Rev,
			"active": ent.Status == "active" && ent.Enabled,
		})
	}
	resp := map[string]any{"repos": repos}
	if len(ents) == limit {
		resp["cursor"] = strconv.FormatUint(uint64(ents[len(ents)-1].Uid), 10)
	}
	return c.JSON(http.StatusOK, resp)
}

// handleGetRecord returns a CAR with the commit block and the MST proof path
// for a single record.
func (s *Server) handleGetRecord(c echo.Context) error {
	ent, err := s.entityByDID(c)
	if ent == nil {
		return err
	}
	collection := c.QueryParam("collection")
	rkey := c.QueryParam("rkey")
	if collection == "" || rkey == "" {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing collection or rkey")
	}

	ctx := c.Request().Context()
	head, err := s.repos.CarStore().GetUserRepoHead(ctx, ent.Uid)
	if err != nil {
		return xrpcError(c, http.StatusNotFound, "RepoNotFound", "no repo head")
	}
	_, blks, err := s.repos.RepoManager().GetRecordProof(ctx, ent.Uid, collection, rkey)
	if err != nil {
		return xrpcError(c, http.StatusNotFound, "RecordNotFound", "record not found")
	}

	c.Response().Header().Set(echo.HeaderContentType, "application/vnd.ipld.car")
	c.Response().WriteHeader(http.StatusOK)
	w := c.Response()
	if _, err := carstore.WriteCarHeader(w, head); err != nil {
		return err
	}
	for _, blk := range blks {
		if _, err := carstore.LdWrite(w, blk.Cid().Bytes(), blk.RawData()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) handleGetBlob(c echo.Context) error {
	did := c.QueryParam("did")
	cidStr := c.QueryParam("cid")
	if did == "" || cidStr == "" {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing did or cid")
	}
	if _, err := cid.Parse(cidStr); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "malformed cid")
	}
	rec, r, err := s.blobs.Open(did, cidStr)
	if err != nil {
		return xrpcError(c, http.StatusNotFound, "BlobNotFound", "no such blob")
	}
	defer r.Close()
	return c.Stream(http.StatusOK, rec.Mime, r)
}

func (s *Server) handleListBlobs(c echo.Context) error {
	did := c.QueryParam("did")
	if did == "" {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "missing did")
	}
	limit := 500
	if l := c.QueryParam("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 1000 {
			limit = v
		}
	}
	cids, err := s.blobs.List(did, limit)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalError", "query failed")
	}
	return c.JSON(http.StatusOK, map[string]any{"cids": cids})
}
