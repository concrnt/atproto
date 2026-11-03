package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fakeRelay(t *testing.T, statusCode int, body string, crawls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.getHostStatus":
			if r.URL.Query().Get("hostname") != "pds.example.com" {
				t.Errorf("hostname %q", r.URL.Query().Get("hostname"))
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(statusCode)
			w.Write([]byte(body))
		case "/xrpc/com.atproto.sync.requestCrawl":
			crawls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestWatchRecrawlsUnsubscribedRelay(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		body    string
		recrawl bool
	}{
		{"active", 200, `{"hostname":"pds.example.com","status":"active","seq":10}`, false},
		{"offline", 200, `{"hostname":"pds.example.com","status":"offline","seq":10}`, true},
		{"idle", 200, `{"hostname":"pds.example.com","status":"idle","seq":10}`, true},
		{"banned", 200, `{"hostname":"pds.example.com","status":"banned"}`, false},
		{"unknown host", 404, `{"error":"HostNotFound","message":"host not found"}`, true},
		{"relay error", 500, `{"error":"InternalError"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var crawls, announces atomic.Int32
			srv := fakeRelay(t, tc.code, tc.body, &crawls)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			Watch(ctx, []string{srv.URL}, "pds.example.com", 50*time.Millisecond, func(context.Context) (int, error) {
				// The announce must be sequenced before the crawl it precedes.
				if crawls.Load() != announces.Load() {
					t.Errorf("announce after crawl")
				}
				announces.Add(1)
				return 1, nil
			})
			// The context can end between the last announce and its crawl.
			if d := announces.Load() - crawls.Load(); tc.recrawl && (crawls.Load() == 0 || d < 0 || d > 1) {
				t.Fatalf("crawls=%d announces=%d, want one announce per crawl", crawls.Load(), announces.Load())
			}
			if !tc.recrawl && (crawls.Load() != 0 || announces.Load() != 0) {
				t.Fatalf("crawls=%d announces=%d, want none", crawls.Load(), announces.Load())
			}
		})
	}
}

func TestWatchSkipsCrawlWithoutAccounts(t *testing.T) {
	var crawls atomic.Int32
	srv := fakeRelay(t, 404, `{"error":"HostNotFound"}`, &crawls)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	Watch(ctx, []string{srv.URL}, "pds.example.com", 50*time.Millisecond, func(context.Context) (int, error) { return 0, nil })
	if crawls.Load() != 0 {
		t.Fatalf("crawled %d times with no accounts", crawls.Load())
	}
}
