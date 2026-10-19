package pds

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"gorm.io/datatypes"
	"gorm.io/gorm/clause"

	"github.com/concrnt/atproto/internal/store"
)

// app.bsky.actor preferences are private client state the official app
// keeps on its PDS (saved feeds, muted words, content labels, ...). They are
// stored as an opaque array; nothing in the bridge interprets them. These
// routes are local even though the app sends them with an atproto-proxy
// header, which is what the reference PDS does too.

const prefsMaxBody = 256 << 10

func (s *Server) handleGetPreferences(c echo.Context) error {
	if s.Sessions == nil {
		return s.handleNotImplemented(c)
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return nil
	}
	var row store.Preferences
	prefs := json.RawMessage("[]")
	if err := s.db.Where("did = ?", did).First(&row).Error; err == nil && len(row.Prefs) > 0 {
		prefs = json.RawMessage(row.Prefs)
	}
	return c.JSON(http.StatusOK, map[string]any{"preferences": prefs})
}

func (s *Server) handlePutPreferences(c echo.Context) error {
	if s.Sessions == nil {
		return s.handleNotImplemented(c)
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request().Body, prefsMaxBody+1))
	if err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "failed to read body")
	}
	if len(raw) > prefsMaxBody {
		return xrpcError(c, http.StatusRequestEntityTooLarge, "PayloadTooLarge", "preferences too large")
	}
	var body struct {
		Preferences json.RawMessage `json:"preferences"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "invalid body")
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(body.Preferences, &arr); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "preferences must be an array")
	}
	compact, _ := json.Marshal(arr)
	row := store.Preferences{DID: did, Prefs: datatypes.JSON(compact), UpdatedAt: time.Now()}
	if err := s.db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "did"}}, UpdateAll: true}).Create(&row).Error; err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
	return c.JSON(http.StatusOK, map[string]any{})
}
