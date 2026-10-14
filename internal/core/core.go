// Package core is the bridge's connection layer to the concrnt server:
// signing documents with the service account's master key, committing them
// (keeping the response, which client.Client.Commit discards), and consuming
// realtime events from the shared Redis instance.
package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	concrnt "github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/jwt"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	Client     *client.Client
	ccid       string
	domain     string
	fqdn       string
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
			s.fqdn = domain
			return s
		}
		s.Client = client.New(fqdn)
		s.Client.AddHostRemapping(fqdn, domain)
		s.fqdn = fqdn
	} else {
		s.Client = client.New(domain)
		s.fqdn = domain
	}
	return s
}

// FQDN is the concrnt server's canonical domain (from its well-known). This
// is the public origin the gateway serves on, so it doubles as the bridge's
// PDS host: the atproto serviceEndpoint and the relay crawl hostname.
func (s *Service) FQDN() string { return s.fqdn }

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

// Import signs doc with the service master key and submits it through the
// server's repository import endpoint (POST /api/v2/repository) instead of
// the commit endpoint. The import path commits in local-only mode, and an
// authenticated requester committing its own documents there is exempt from
// the backdate window, so a Bluesky post older than that window can still be
// mirrored (the ActivityPub bridge imports remote notes the same way). The
// endpoint never fans out to timelines, so docs with distributes do not
// belong here.
func Import[T any](ctx context.Context, s *Service, doc concrnt.Document[T]) error {
	sd, err := Sign(doc, s.privateKey)
	if err != nil {
		return err
	}
	line, err := json.Marshal(sd)
	if err != nil {
		return err
	}
	token, err := s.masterToken()
	if err != nil {
		return err
	}

	url := s.baseURL() + "/api/v2/repository"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(append(line, '\n')))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("import request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("import to %s failed: status %d: %s", url, resp.StatusCode, string(respBody))
	}

	// The endpoint answers 200 with one entry per rejected line; an empty
	// list means every line was committed.
	var results []struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(respBody, &results); err != nil {
		return fmt.Errorf("failed to parse import response: %w", err)
	}
	if len(results) > 0 {
		return fmt.Errorf("import rejected: %s", results[0].Error)
	}
	return nil
}

// masterToken issues a short-lived JWT signed by the service master key, the
// raw-key login the auth middleware accepts (issuer = the bare CCID as a cckv
// URI, audience = the server's FQDN).
func (s *Service) masterToken() (string, error) {
	now := time.Now()
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return jwt.Create(jwt.Claims{
		Issuer:         "cckv://" + s.ccid,
		Subject:        "concrnt",
		Audience:       s.fqdn,
		IssuedAt:       strconv.FormatInt(now.Unix(), 10),
		ExpirationTime: strconv.FormatInt(now.Add(5*time.Minute).Unix(), 10),
		JWTID:          hex.EncodeToString(nonce[:]),
	}, s.privateKey)
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

// IsNotFound reports whether err is the upstream client's "status code 404"
// failure for a missing resource. The client exposes no typed error for this,
// so the message is matched here; keep every such check behind this helper.
func IsNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "status code 404")
}

// QueryAllByPrefix pages through the server's query endpoint and returns every
// record under prefix, restricted to schema when non-empty. The since cursor
// is inclusive, so boundary rows repeat across pages; rows are deduplicated by
// cckv key (last write wins). complete is false when the cursor stalls (a full
// page sharing one createdAt) — the listing is then a usable but possibly
// partial view, and callers must not treat missing keys as deletions.
// Queries are anonymous: records behind a read policy are invisible here.
func QueryAllByPrefix(ctx context.Context, s *Service, prefix string, schema string) (docs []concrnt.Document[json.RawMessage], complete bool, err error) {
	byKey := map[string]concrnt.Document[json.RawMessage]{}
	var since *time.Time
	// The server's cursor is the RFC3339Nano createdAt of the first row past
	// the page (orderby=key is never used here, so no cckv cursors). It is
	// kept verbatim for the stall check and parsed back into Since.
	var lastCursor string
	complete = true
	for {
		page, err := s.Client.Query(ctx, s.fqdn, client.QueryParams{
			Prefix: prefix,
			Schema: schema,
			Since:  since,
			Limit:  100,
			Order:  "asc",
		})
		if err != nil {
			return nil, false, fmt.Errorf("query %s failed: %w", prefix, err)
		}
		for _, sd := range page.Items {
			if sd.CCKV == nil {
				continue
			}
			var doc concrnt.Document[json.RawMessage]
			if err := json.Unmarshal([]byte(sd.Document), &doc); err != nil {
				slog.Debug("skipping unparsable queried document", "key", *sd.CCKV, "error", err)
				continue
			}
			byKey[*sd.CCKV] = doc
		}
		// Items are filtered by read permission after paging, so an empty
		// page with a cursor still means there may be more rows.
		if page.Next == nil {
			break
		}
		if *page.Next == lastCursor {
			slog.Warn("query cursor stalled; listing may be incomplete", "prefix", prefix, "cursor", lastCursor)
			complete = false
			break
		}
		next, err := time.Parse(time.RFC3339Nano, *page.Next)
		if err != nil {
			slog.Warn("unparsable query cursor; listing may be incomplete", "prefix", prefix, "cursor", *page.Next, "error", err)
			complete = false
			break
		}
		lastCursor = *page.Next
		since = &next
	}
	docs = make([]concrnt.Document[json.RawMessage], 0, len(byKey))
	for _, doc := range byKey {
		docs = append(docs, doc)
	}
	return docs, complete, nil
}

// Events subscribes to all concrnt core events via the shared Redis instance
// (PSUBSCRIBE cc-event:*; channel name = prefix + resource URI, stripped before
// emit) and emits them on the returned channel until ctx is done.
func Events(ctx context.Context, redisURL string) (<-chan ChannelEvent, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	sub := rdb.PSubscribe(ctx, interop.EventChannelPrefix+"*")
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
				case out <- ChannelEvent{Channel: strings.TrimPrefix(msg.Channel, interop.EventChannelPrefix), Event: ev}:
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
