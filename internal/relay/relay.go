// Package relay notifies upstream relays that this host exists and should be
// crawled.
package relay

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
)

// RequestCrawl asks each relay to crawl pdsHost. Failures are logged, not
// fatal: the call is idempotent and retried on every startup and account
// creation.
func RequestCrawl(ctx context.Context, relays []string, pdsHost string) {
	for _, r := range relays {
		client := &xrpc.Client{Host: r}
		err := comatproto.SyncRequestCrawl(ctx, client, &comatproto.SyncRequestCrawl_Input{
			Hostname: pdsHost,
		})
		if err != nil {
			slog.Warn("requestCrawl failed", "relay", r, "error", err)
			continue
		}
		slog.Info("requested crawl", "relay", r, "host", pdsHost)
	}
}

// Watch periodically asks each relay how it sees pdsHost and, when the relay
// is not subscribed (an indigo relay gives up after 16 failed dials and waits
// for a requestCrawl), re-announces the accounts and requests a crawl. The
// announce goes first so its #sync is in the replay the relay resumes from;
// it covers commits that aged out of the firehose window while offline.
// announce returns how many accounts it announced; with none there is nothing
// for a relay to crawl yet.
func Watch(ctx context.Context, relays []string, pdsHost string, interval time.Duration, announce func(context.Context) (int, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, r := range relays {
			if !needsCrawl(ctx, r, pdsHost) {
				continue
			}
			n, err := announce(ctx)
			if err != nil {
				slog.Error("failed to announce accounts before re-crawl", "relay", r, "error", err)
			} else if n == 0 {
				continue
			}
			RequestCrawl(ctx, []string{r}, pdsHost)
		}
	}
}

func needsCrawl(ctx context.Context, relay, pdsHost string) bool {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := comatproto.SyncGetHostStatus(ctx, &xrpc.Client{Host: relay}, pdsHost)
	if err != nil {
		var xe *xrpc.Error
		if errors.As(err, &xe) && xe.StatusCode == http.StatusNotFound {
			slog.Warn("relay does not know this host", "relay", relay, "host", pdsHost)
			return true
		}
		slog.Warn("failed to get host status from relay", "relay", relay, "error", err)
		return false
	}
	status := ""
	if out.Status != nil {
		status = *out.Status
	}
	switch status {
	case "active":
		return false
	case "banned":
		slog.Error("relay has banned this host", "relay", relay, "host", pdsHost)
		return false
	}
	slog.Warn("relay is not subscribed to this host", "relay", relay, "host", pdsHost, "status", status)
	return true
}
