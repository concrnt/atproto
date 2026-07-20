// Package core is the bridge's connection layer to the concrnt server:
// signing documents with the service account's master key, committing them
// (keeping the response, which client.Client.Commit discards), and consuming
// realtime events from the shared Redis instance.
package core

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	concrnt "github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	Client     *client.Client
	ccid       string
	domain     string
	privateKey string
	http       *http.Client
}

func NewService(ccid, domain, privateKey string) *Service {
	s := &Service{
		ccid:       ccid,
		domain:     domain,
		privateKey: privateKey,
		http:       &http.Client{Timeout: 20 * time.Second},
	}

	// A domain with an explicit scheme (e.g. http://localhost:8000 on a
	// devnet) is an API base override: learn the server's real FQDN from
	// its well-known and remap that host onto the override URL, since the
	// upstream client always dials https://<fqdn>.
	if strings.Contains(domain, "://") {
		fqdn, err := fetchWellKnownDomain(s.baseURL())
		if err != nil {
			slog.Warn("failed to resolve concrnt domain from override url; resolution may not work", "url", domain, "error", err)
			s.Client = client.New(domain)
			return s
		}
		s.Client = client.New(fqdn)
		s.Client.AddHostRemapping(fqdn, domain)
	} else {
		s.Client = client.New(domain)
	}
	return s
}

func fetchWellKnownDomain(baseURL string) (string, error) {
	resp, err := http.Get(baseURL + "/.well-known/concrnt")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var wk concrnt.WellKnownConcrnt
	if err := json.NewDecoder(resp.Body).Decode(&wk); err != nil {
		return "", err
	}
	if wk.Domain == "" {
		return "", fmt.Errorf("well-known response carried no domain")
	}
	return wk.Domain, nil
}

func (s *Service) CCID() string { return s.ccid }

// baseURL allows the configured domain to carry an explicit scheme
// (e.g. http://localhost:8000 on a devnet); plain FQDNs get https.
func (s *Service) baseURL() string {
	if strings.Contains(s.domain, "://") {
		return strings.TrimSuffix(s.domain, "/")
	}
	return "https://" + s.domain
}

// Sign marshals a document and wraps it in a SignedDocument with a direct
// master-key proof, the same way the ActivityPub bridge commits.
func Sign[T any](doc concrnt.Document[T], privateKey string) (*concrnt.SignedDocument, error) {
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}
	sigBytes, err := concrnt.SignBytes(docBytes, privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign document: %w", err)
	}
	sig := hex.EncodeToString(sigBytes)
	return &concrnt.SignedDocument{
		Document: string(docBytes),
		Proof: concrnt.Proof{
			Type:      concrnt.ProofTypeEcrecover,
			Signature: &sig,
		},
	}, nil
}

// Commit signs doc with the service master key and POSTs it to the local
// server's commit endpoint, returning the resulting SignedDocument (which
// carries the assigned cckv/ccfs URIs).
func Commit[T any](ctx context.Context, s *Service, doc concrnt.Document[T]) (*concrnt.SignedDocument, error) {
	sd, err := Sign(doc, s.privateKey)
	if err != nil {
		return nil, err
	}
	return s.CommitSigned(ctx, sd)
}

func (s *Service) CommitSigned(ctx context.Context, sd *concrnt.SignedDocument) (*concrnt.SignedDocument, error) {
	body, err := json.Marshal(sd)
	if err != nil {
		return nil, err
	}

	url := s.baseURL() + "/api/v2/commit"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("commit request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("commit to %s failed: status %d: %s", url, resp.StatusCode, string(respBody))
	}

	var result concrnt.SignedDocument
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse commit response: %w", err)
	}
	return &result, nil
}

// GetDocument resolves a cckv/ccfs URI into its parsed document.
func GetDocument[T any](ctx context.Context, s *Service, uri string) (*concrnt.Document[T], error) {
	var doc concrnt.Document[T]
	err := s.Client.GetRecord(ctx, uri, nil, &doc)
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

// Events subscribes to all concrnt core events via the shared Redis instance
// (PSUBSCRIBE *; channel name = resource URI) and emits them on the returned
// channel until ctx is done.
func Events(ctx context.Context, redisURL string) (<-chan ChannelEvent, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	sub := rdb.PSubscribe(ctx, "*")
	out := make(chan ChannelEvent, 256)

	go func() {
		defer close(out)
		defer sub.Close()
		defer rdb.Close()
		ch := sub.Channel(redis.WithChannelSize(256))
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var ev concrnt.Event
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
					slog.Debug("skipping unparsable pubsub payload", "channel", msg.Channel, "error", err)
					continue
				}
				select {
				case out <- ChannelEvent{Channel: msg.Channel, Event: ev}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

// ChannelEvent pairs a concrnt core event with the Redis channel (resource
// URI) it was published on.
type ChannelEvent struct {
	Channel string
	Event   concrnt.Event
}
