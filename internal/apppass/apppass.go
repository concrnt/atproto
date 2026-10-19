// Package apppass issues and checks the app passwords that let atproto
// clients (the official Bluesky app) log in to the bridge PDS. Both the
// management API (issue / list / revoke, on the user's behalf) and the PDS
// face (createSession) use it.
package apppass

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/store"
)

var (
	ErrNameTaken     = errors.New("app password name already in use")
	ErrTooMany       = errors.New("too many app passwords")
	ErrInvalidName   = errors.New("app password name must be 1-32 characters")
	ErrNotFound      = errors.New("app password not found")
	ErrBadPassword   = errors.New("invalid password")
	MaxPerAccount    = 20
	passwordAlphabet = "abcdefghijklmnopqrstuvwxyz234567" // 32 symbols, no ambiguous 0/1/l
)

// Generate returns a fresh plaintext in Bluesky's familiar
// xxxx-xxxx-xxxx-xxxx shape: 16 symbols from a 32-symbol alphabet (80 bits).
func Generate() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	var sb strings.Builder
	for i, b := range buf {
		if i > 0 && i%4 == 0 {
			sb.WriteByte('-')
		}
		sb.WriteByte(passwordAlphabet[int(b)%len(passwordAlphabet)])
	}
	return sb.String()
}

// Hash is the stored form of a plaintext.
func Hash(plain string) string {
	sum := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(sum[:])
}

func newID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

// Create issues a new app password for did and returns it with the plaintext
// (the only time it is available).
func Create(db *gorm.DB, did, name string) (*store.AppPassword, string, error) {
	name = strings.TrimSpace(name)
	if n := utf8.RuneCountInString(name); n == 0 || n > 32 {
		return nil, "", ErrInvalidName
	}
	var count int64
	if err := db.Model(&store.AppPassword{}).Where("did = ?", did).Count(&count).Error; err != nil {
		return nil, "", err
	}
	if count >= int64(MaxPerAccount) {
		return nil, "", ErrTooMany
	}
	var dup int64
	if err := db.Model(&store.AppPassword{}).Where("did = ? AND name = ?", did, name).Count(&dup).Error; err != nil {
		return nil, "", err
	}
	if dup > 0 {
		return nil, "", ErrNameTaken
	}
	plain := Generate()
	row := &store.AppPassword{
		ID:        newID(),
		DID:       did,
		Name:      name,
		Hash:      Hash(plain),
		CreatedAt: time.Now(),
	}
	if err := db.Create(row).Error; err != nil {
		return nil, "", fmt.Errorf("failed to store app password: %w", err)
	}
	return row, plain, nil
}

// List returns did's app passwords, oldest first.
func List(db *gorm.DB, did string) ([]store.AppPassword, error) {
	var rows []store.AppPassword
	err := db.Where("did = ?", did).Order("created_at asc").Find(&rows).Error
	return rows, err
}

// Revoke deletes the app password (matched by id or name) and every session
// that was opened with it, in one transaction.
func Revoke(db *gorm.DB, did, idOrName string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var row store.AppPassword
		err := tx.Where("did = ? AND (id = ? OR name = ?)", did, idOrName, idOrName).First(&row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if err := tx.Where("app_password_id = ?", row.ID).Delete(&store.Session{}).Error; err != nil {
			return err
		}
		return tx.Delete(&row).Error
	})
}

// Authenticate checks plain against did's app passwords and marks the match
// as used. It reports ErrBadPassword on any mismatch.
func Authenticate(db *gorm.DB, did, plain string) (*store.AppPassword, error) {
	plain = strings.TrimSpace(plain)
	if plain == "" {
		return nil, ErrBadPassword
	}
	h := Hash(plain)
	var row store.AppPassword
	err := db.Where("did = ? AND hash = ?", did, h).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrBadPassword
	}
	if err != nil {
		return nil, err
	}
	// The lookup already matched on the digest; the constant-time compare is
	// belt and braces against a collation-insensitive index.
	if subtle.ConstantTimeCompare([]byte(row.Hash), []byte(h)) != 1 {
		return nil, ErrBadPassword
	}
	now := time.Now()
	db.Model(&row).Update("last_used_at", now)
	row.LastUsedAt = &now
	return &row, nil
}
