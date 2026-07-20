// Package keys manages per-account K256 signing keys (AES-GCM encrypted at
// rest) and implements repomgr.KeyManager on top of them.
package keys

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/store"
)

const KeyTypeSigning = "signing"

type Service struct {
	db   *gorm.DB
	aead cipher.AEAD

	mu    sync.RWMutex
	cache map[string]*atcrypto.PrivateKeyK256
}

func NewService(db *gorm.DB, encryptionKeyHex string) (*Service, error) {
	kek, err := hex.DecodeString(encryptionKeyHex)
	if err != nil || len(kek) != 32 {
		return nil, fmt.Errorf("keyEncryptionKey must be 32 bytes of hex")
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Service{db: db, aead: aead, cache: map[string]*atcrypto.PrivateKeyK256{}}, nil
}

func (s *Service) encrypt(plain []byte) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return append(nonce, s.aead.Seal(nil, nonce, plain, nil)...), nil
}

func (s *Service) decrypt(blob []byte) ([]byte, error) {
	ns := s.aead.NonceSize()
	if len(blob) < ns {
		return nil, fmt.Errorf("ciphertext too short")
	}
	return s.aead.Open(nil, blob[:ns], blob[ns:], nil)
}

// Generate creates a new K256 key without persisting it. The caller stores
// it with StoreSigningKey once the DID is known (the DID itself derives from
// a PLC genesis op that references this key, so generation must come first).
func Generate() (*atcrypto.PrivateKeyK256, error) {
	return atcrypto.GeneratePrivateKeyK256()
}

// StoreSigningKey persists priv as the signing key of did.
func (s *Service) StoreSigningKey(did string, priv *atcrypto.PrivateKeyK256) error {
	enc, err := s.encrypt([]byte(priv.Multibase()))
	if err != nil {
		return err
	}
	pub, err := priv.PublicKey()
	if err != nil {
		return err
	}
	rec := store.Key{
		DID:        did,
		KeyType:    KeyTypeSigning,
		PrivateEnc: enc,
		Public:     pub.DIDKey(),
	}
	if err := s.db.Create(&rec).Error; err != nil {
		return err
	}
	s.mu.Lock()
	s.cache[did] = priv
	s.mu.Unlock()
	return nil
}

// SigningKey loads (and caches) the signing key for did.
func (s *Service) SigningKey(did string) (*atcrypto.PrivateKeyK256, error) {
	s.mu.RLock()
	if k, ok := s.cache[did]; ok {
		s.mu.RUnlock()
		return k, nil
	}
	s.mu.RUnlock()

	var rec store.Key
	if err := s.db.Where("did = ? AND key_type = ?", did, KeyTypeSigning).First(&rec).Error; err != nil {
		return nil, fmt.Errorf("no signing key for %s: %w", did, err)
	}
	plain, err := s.decrypt(rec.PrivateEnc)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt signing key for %s: %w", did, err)
	}
	parsed, err := atcrypto.ParsePrivateMultibase(string(plain))
	if err != nil {
		return nil, err
	}
	k256, ok := parsed.(*atcrypto.PrivateKeyK256)
	if !ok {
		return nil, fmt.Errorf("signing key for %s is not K256", did)
	}
	s.mu.Lock()
	s.cache[did] = k256
	s.mu.Unlock()
	return k256, nil
}

// SignForUser implements repomgr.KeyManager.
func (s *Service) SignForUser(ctx context.Context, did string, msg []byte) ([]byte, error) {
	k, err := s.SigningKey(did)
	if err != nil {
		return nil, err
	}
	return k.HashAndSign(msg)
}

// VerifyUserSignature implements repomgr.KeyManager.
func (s *Service) VerifyUserSignature(ctx context.Context, did string, sig []byte, msg []byte) error {
	k, err := s.SigningKey(did)
	if err != nil {
		return err
	}
	pub, err := k.PublicKey()
	if err != nil {
		return err
	}
	return pub.HashAndVerify(msg, sig)
}
