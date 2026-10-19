package pds

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/auth"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/apppass"
	"github.com/concrnt/atproto/internal/store"
)

// Sessions issues and validates the access/refresh JWT pairs that atproto
// clients obtain through com.atproto.server.createSession with an app
// password. Tokens are HS256 over a bridge-wide secret; revocation is a row
// in at_sessions keyed by the refresh lineage's JTI, which the access token
// carries too.
type Sessions struct {
	db     *gorm.DB
	secret []byte
	aud    string // did:web:<pdsHost>

	// createSession brute-force guard: failures per identifier.
	mu       sync.Mutex
	failures map[string]*failureWindow
}

type failureWindow struct {
	count int
	reset time.Time
}

const (
	scopeAccess  = "com.atproto.appPassPrivileged"
	scopeRefresh = "com.atproto.refresh"

	accessTTL  = 2 * time.Hour
	refreshTTL = 90 * 24 * time.Hour

	loginFailureLimit  = 10
	loginFailureWindow = 15 * time.Minute
)

func NewSessions(db *gorm.DB, secret []byte, pdsHost string) *Sessions {
	return &Sessions{db: db, secret: secret, aud: "did:web:" + pdsHost, failures: map[string]*failureWindow{}}
}

type sessionClaims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
	APID  string `json:"apid"`
}

var (
	errTokenExpired = errors.New("token expired")
	errTokenInvalid = errors.New("token invalid")
)

func randomID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func (s *Sessions) sign(did, scope, jti, apid string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := sessionClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   did,
			Audience:  jwt.ClaimStrings{s.aud},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			ID:        jti,
		},
		Scope: scope,
		APID:  apid,
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
}

// Issue opens a new session lineage for did and returns (access, refresh).
func (s *Sessions) Issue(did, apid string) (string, string, error) {
	jti := randomID()
	if err := s.db.Create(&store.Session{
		JTI:           jti,
		DID:           did,
		AppPasswordID: apid,
		ExpiresAt:     time.Now().Add(refreshTTL),
		CreatedAt:     time.Now(),
	}).Error; err != nil {
		return "", "", err
	}
	access, err := s.sign(did, scopeAccess, jti, apid, accessTTL)
	if err != nil {
		return "", "", err
	}
	refresh, err := s.sign(did, scopeRefresh, jti, apid, refreshTTL)
	if err != nil {
		return "", "", err
	}
	return access, refresh, nil
}

// parse validates signature, audience, scope and expiry. Expiry is reported
// separately (errTokenExpired) because clients only refresh on it.
func (s *Sessions) parse(tokenString, scope string) (*sessionClaims, error) {
	var claims sessionClaims
	tok, err := jwt.ParseWithClaims(tokenString, &claims, func(t *jwt.Token) (any, error) {
		return s.secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithAudience(s.aud))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, errTokenExpired
		}
		return nil, errTokenInvalid
	}
	if !tok.Valid || claims.Scope != scope || claims.Subject == "" || claims.ID == "" {
		return nil, errTokenInvalid
	}
	return &claims, nil
}

func (s *Sessions) lineageAlive(jti string) bool {
	var count int64
	s.db.Model(&store.Session{}).Where("jti = ?", jti).Count(&count)
	return count > 0
}

// Authenticate resolves the bearer access token of c to the account DID.
// On failure it has already written the XRPC error and returns "".
func (s *Sessions) Authenticate(c echo.Context) string {
	raw := bearer(c)
	if raw == "" {
		_ = xrpcError(c, http.StatusUnauthorized, "AuthMissing", "Authentication Required")
		return ""
	}
	claims, err := s.parse(raw, scopeAccess)
	if errors.Is(err, errTokenExpired) {
		// 400 ExpiredToken is what makes clients call refreshSession; a 401
		// logs the user out.
		_ = xrpcError(c, http.StatusBadRequest, "ExpiredToken", "Token has expired")
		return ""
	}
	if err != nil || !s.lineageAlive(claims.ID) {
		_ = xrpcError(c, http.StatusUnauthorized, "InvalidToken", "Token could not be verified")
		return ""
	}
	var ent store.Entity
	if err := s.db.Where("did = ? AND status = ?", claims.Subject, "active").First(&ent).Error; err != nil {
		_ = xrpcError(c, http.StatusUnauthorized, "InvalidToken", "Account is not active")
		return ""
	}
	return claims.Subject
}

// Refresh rotates the lineage of refreshJWT and returns a new pair.
func (s *Sessions) Refresh(refreshJWT string) (string, string, string, error) {
	claims, err := s.parse(refreshJWT, scopeRefresh)
	if err != nil {
		return "", "", "", err
	}
	newJTI := randomID()
	err = s.db.Transaction(func(tx *gorm.DB) error {
		res := tx.Where("jti = ?", claims.ID).Delete(&store.Session{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errTokenInvalid
		}
		return tx.Create(&store.Session{
			JTI:           newJTI,
			DID:           claims.Subject,
			AppPasswordID: claims.APID,
			ExpiresAt:     time.Now().Add(refreshTTL),
			CreatedAt:     time.Now(),
		}).Error
	})
	if err != nil {
		return "", "", "", err
	}
	access, err := s.sign(claims.Subject, scopeAccess, newJTI, claims.APID, accessTTL)
	if err != nil {
		return "", "", "", err
	}
	refresh, err := s.sign(claims.Subject, scopeRefresh, newJTI, claims.APID, refreshTTL)
	if err != nil {
		return "", "", "", err
	}
	return claims.Subject, access, refresh, nil
}

// Delete ends the lineage of refreshJWT (idempotent).
func (s *Sessions) Delete(refreshJWT string) error {
	claims, err := s.parse(refreshJWT, scopeRefresh)
	if err != nil {
		return err
	}
	return s.db.Where("jti = ?", claims.ID).Delete(&store.Session{}).Error
}

func (s *Sessions) loginBlocked(identifier string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.failures[identifier]
	if !ok || time.Now().After(w.reset) {
		return false
	}
	return w.count >= loginFailureLimit
}

func (s *Sessions) noteLoginFailure(identifier string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.failures[identifier]
	if !ok || time.Now().After(w.reset) {
		s.failures[identifier] = &failureWindow{count: 1, reset: time.Now().Add(loginFailureWindow)}
		return
	}
	w.count++
}

func (s *Sessions) clearLoginFailures(identifier string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.failures, identifier)
}

func bearer(c echo.Context) string {
	h := c.Request().Header.Get(echo.HeaderAuthorization)
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// ---- handlers ----

// didDocFor renders the DID document of a bridged account from local state
// (same content as the PLC log), so clients can learn the PDS endpoint from
// the session response without a network round trip.
func (s *Server) didDocFor(ent *store.Entity) (map[string]any, error) {
	if s.Keys == nil {
		return nil, fmt.Errorf("no key service")
	}
	priv, err := s.Keys.SigningKey(ent.DID)
	if err != nil {
		return nil, err
	}
	pub, err := priv.PublicKey()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"@context": []string{
			"https://www.w3.org/ns/did/v1",
			"https://w3id.org/security/multikey/v1",
			"https://w3id.org/security/suites/secp256k1-2019/v1",
		},
		"id":          ent.DID,
		"alsoKnownAs": []string{"at://" + ent.Handle},
		"verificationMethod": []map[string]any{{
			"id":                 ent.DID + "#atproto",
			"type":               "Multikey",
			"controller":         ent.DID,
			"publicKeyMultibase": pub.Multibase(),
		}},
		"service": []map[string]any{{
			"id":              "#atproto_pds",
			"type":            "AtprotoPersonalDataServer",
			"serviceEndpoint": "https://" + s.pdsHost,
		}},
	}, nil
}

func (s *Server) sessionBody(ent *store.Entity, access, refresh string) map[string]any {
	body := map[string]any{
		"handle":          ent.Handle,
		"did":             ent.DID,
		"emailConfirmed":  true,
		"emailAuthFactor": false,
		"active":          true,
	}
	if doc, err := s.didDocFor(ent); err == nil {
		body["didDoc"] = doc
	}
	if access != "" {
		body["accessJwt"] = access
		body["refreshJwt"] = refresh
	}
	return body
}

// normalizeIdentifier accepts a handle (optionally @-prefixed) or a DID.
func normalizeIdentifier(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(id, "did:") {
		return id
	}
	return strings.ToLower(strings.TrimPrefix(id, "@"))
}

func (s *Server) handleCreateSession(c echo.Context) error {
	if s.Sessions == nil {
		return s.handleNotImplemented(c)
	}
	var body struct {
		Identifier string `json:"identifier"`
		Password   string `json:"password"`
	}
	if err := c.Bind(&body); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "invalid body")
	}
	identifier := normalizeIdentifier(body.Identifier)
	if identifier == "" || body.Password == "" {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "identifier and password are required")
	}
	if s.Sessions.loginBlocked(identifier) {
		return xrpcError(c, http.StatusTooManyRequests, "RateLimitExceeded", "Too many failed login attempts; try again later")
	}
	deny := func() error {
		s.Sessions.noteLoginFailure(identifier)
		return xrpcError(c, http.StatusUnauthorized, "AuthenticationRequired", "Invalid identifier or password")
	}
	var ent store.Entity
	if err := s.db.Where("(did = ? OR handle = ?) AND status = ?", identifier, identifier, "active").First(&ent).Error; err != nil {
		return deny()
	}
	ap, err := apppass.Authenticate(s.db, ent.DID, body.Password)
	if err != nil {
		return deny()
	}
	access, refresh, err := s.Sessions.Issue(ent.DID, ap.ID)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
	s.Sessions.clearLoginFailures(identifier)
	return c.JSON(http.StatusOK, s.sessionBody(&ent, access, refresh))
}

func (s *Server) handleRefreshSession(c echo.Context) error {
	if s.Sessions == nil {
		return s.handleNotImplemented(c)
	}
	raw := bearer(c)
	if raw == "" {
		return xrpcError(c, http.StatusUnauthorized, "AuthMissing", "Authentication Required")
	}
	did, access, refresh, err := s.Sessions.Refresh(raw)
	if errors.Is(err, errTokenExpired) {
		return xrpcError(c, http.StatusBadRequest, "ExpiredToken", "Token has expired")
	}
	if err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidToken", "Token could not be verified")
	}
	var ent store.Entity
	if err := s.db.Where("did = ?", did).First(&ent).Error; err != nil {
		return xrpcError(c, http.StatusUnauthorized, "InvalidToken", "Account not found")
	}
	return c.JSON(http.StatusOK, s.sessionBody(&ent, access, refresh))
}

func (s *Server) handleGetSession(c echo.Context) error {
	if s.Sessions == nil {
		return s.handleNotImplemented(c)
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return nil
	}
	var ent store.Entity
	if err := s.db.Where("did = ?", did).First(&ent).Error; err != nil {
		return xrpcError(c, http.StatusUnauthorized, "InvalidToken", "Account not found")
	}
	return c.JSON(http.StatusOK, s.sessionBody(&ent, "", ""))
}

func (s *Server) handleDeleteSession(c echo.Context) error {
	if s.Sessions == nil {
		return s.handleNotImplemented(c)
	}
	raw := bearer(c)
	if raw == "" {
		return xrpcError(c, http.StatusUnauthorized, "AuthMissing", "Authentication Required")
	}
	if err := s.Sessions.Delete(raw); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidToken", "Token could not be verified")
	}
	return c.JSON(http.StatusOK, map[string]any{})
}

// handleGetServiceAuth signs an inter-service token on behalf of the logged
// in account (video upload, age assurance and the like use it).
func (s *Server) handleGetServiceAuth(c echo.Context) error {
	if s.Sessions == nil || s.Keys == nil {
		return s.handleNotImplemented(c)
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return nil
	}
	aud := c.QueryParam("aud")
	if _, err := syntax.ParseDID(aud); err != nil {
		return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "aud must be a DID")
	}
	var lxm *syntax.NSID
	if raw := c.QueryParam("lxm"); raw != "" {
		n, err := syntax.ParseNSID(raw)
		if err != nil {
			return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "lxm must be an NSID")
		}
		lxm = &n
	}
	ttl := time.Minute
	if raw := c.QueryParam("exp"); raw != "" {
		var expUnix int64
		if _, err := fmt.Sscanf(raw, "%d", &expUnix); err != nil {
			return xrpcError(c, http.StatusBadRequest, "InvalidRequest", "exp must be a unix timestamp")
		}
		ttl = time.Until(time.Unix(expUnix, 0))
		if ttl <= 0 {
			return xrpcError(c, http.StatusBadRequest, "BadExpiration", "exp is in the past")
		}
		if ttl > time.Hour {
			return xrpcError(c, http.StatusBadRequest, "BadExpiration", "exp may be at most one hour ahead")
		}
	}
	priv, err := s.Keys.SigningKey(did)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", "signing key unavailable")
	}
	tok, err := auth.SignServiceAuth(syntax.DID(did), aud, ttl, lxm, priv)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
	return c.JSON(http.StatusOK, map[string]string{"token": tok})
}

// handleListAppPasswords lets the official app's settings page render the
// names; issuing and revoking stay in world-app (handleReadOnly).
func (s *Server) handleListAppPasswords(c echo.Context) error {
	if s.Sessions == nil {
		return s.handleNotImplemented(c)
	}
	did := s.Sessions.Authenticate(c)
	if did == "" {
		return nil
	}
	rows, err := apppass.List(s.db, did)
	if err != nil {
		return xrpcError(c, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		out = append(out, map[string]any{
			"name":       r.Name,
			"createdAt":  r.CreatedAt.UTC().Format(time.RFC3339),
			"privileged": true,
		})
	}
	return c.JSON(http.StatusOK, map[string]any{"passwords": out})
}
