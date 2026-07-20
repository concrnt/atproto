// Package relay notifies upstream relays that this host exists and should be
// crawled.
package relay

import (
	"context"
	"log/slog"

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
