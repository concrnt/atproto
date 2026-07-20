// Package plcm creates and updates did:plc identities for bridged accounts.
// All directory submissions go through a serial queue with retry, since
// plc.directory rate-limits and account creation is not latency-sensitive.
package plcm

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/did-method-plc/go-didplc/didplc"
)

type Service struct {
	client      *didplc.Client
	rotationKey atcrypto.PrivateKey
	pdsEndpoint string

	mu sync.Mutex // serializes directory submissions
}

func NewService(directoryURL string, rotationKey atcrypto.PrivateKey, pdsHost string) *Service {
	return &Service{
		client: &didplc.Client{
			DirectoryURL: directoryURL,
			UserAgent:    "concrnt-atproto-bridge",
		},
		rotationKey: rotationKey,
		pdsEndpoint: "https://" + pdsHost,
	}
}

func (s *Service) rotationDIDKey() (string, error) {
	pub, err := s.rotationKey.PublicKey()
	if err != nil {
		return "", err
	}
	return pub.DIDKey(), nil
}

// CreateDID builds, signs and submits a genesis operation for a new bridged
// account and returns (did, opCID).
func (s *Service) CreateDID(ctx context.Context, signingKeyDID string, handle string) (string, string, error) {
	rot, err := s.rotationDIDKey()
	if err != nil {
		return "", "", err
	}
	op := didplc.RegularOp{
		Type:                "plc_operation",
		RotationKeys:        []string{rot},
		VerificationMethods: map[string]string{"atproto": signingKeyDID},
		AlsoKnownAs:         []string{"at://" + handle},
		Services: map[string]didplc.OpService{
			"atproto_pds": {
				Type:     "AtprotoPersonalDataServer",
				Endpoint: s.pdsEndpoint,
			},
		},
		Prev: nil,
	}
	if err := op.Sign(s.rotationKey); err != nil {
		return "", "", fmt.Errorf("failed to sign plc genesis op: %w", err)
	}
	did, err := op.DID()
	if err != nil {
		return "", "", err
	}
	if err := s.submit(ctx, did, &op); err != nil {
		return "", "", err
	}
	return did, op.CID().String(), nil
}

// UpdateHandle submits an update operation replacing alsoKnownAs, and returns
// the new op CID. prevCID must be the account's latest operation CID.
func (s *Service) UpdateHandle(ctx context.Context, did, signingKeyDID, prevCID, newHandle string) (string, error) {
	rot, err := s.rotationDIDKey()
	if err != nil {
		return "", err
	}
	op := didplc.RegularOp{
		Type:                "plc_operation",
		RotationKeys:        []string{rot},
		VerificationMethods: map[string]string{"atproto": signingKeyDID},
		AlsoKnownAs:         []string{"at://" + newHandle},
		Services: map[string]didplc.OpService{
			"atproto_pds": {
				Type:     "AtprotoPersonalDataServer",
				Endpoint: s.pdsEndpoint,
			},
		},
		Prev: &prevCID,
	}
	if err := op.Sign(s.rotationKey); err != nil {
		return "", fmt.Errorf("failed to sign plc update op: %w", err)
	}
	if err := s.submit(ctx, did, &op); err != nil {
		return "", err
	}
	return op.CID().String(), nil
}

// WaitForRegistration polls the directory until the DID resolves (or ctx
// expires). Relays validate DIDs on first sight, so firehose events must not
// be emitted before this returns nil.
func (s *Service) WaitForRegistration(ctx context.Context, did string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := s.client.Resolve(ctx, did)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("did %s did not become resolvable: %w", did, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Service) submit(ctx context.Context, did string, op didplc.Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error
	for attempt := range 3 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 5 * time.Second):
			}
		}
		lastErr = s.client.Submit(ctx, did, op)
		if lastErr == nil {
			return nil
		}
		slog.Warn("plc submit failed", "did", did, "attempt", attempt+1, "error", lastErr)
	}
	return fmt.Errorf("plc submit for %s failed after retries: %w", did, lastErr)
}
