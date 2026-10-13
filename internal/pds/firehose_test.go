package pds

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/glebarez/sqlite"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/plcm"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "pds.db")), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := store.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	ks, err := keys.NewService(db, strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	rot, _ := atcrypto.GeneratePrivateKeyK256()
	repos, err := repoman.New(db, t.TempDir(), ks, plcm.NewService("http://plc.invalid", rot, "pds.example"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(db, repos, nil, "pds.example", "test")
}

func dialFirehose(t *testing.T, srv *httptest.Server, query string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos" + query
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestFreshConsumerTriggersAnnouncement(t *testing.T) {
	s := newTestServer(t)
	var calls atomic.Int32
	s.OnFreshConsumer = func(context.Context) { calls.Add(1) }

	e := echo.New()
	s.Register(e)
	srv := httptest.NewServer(e)
	defer srv.Close()

	waitCalls := func(want int32) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for calls.Load() != want && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if calls.Load() != want {
			t.Fatalf("OnFreshConsumer calls = %d, want %d", calls.Load(), want)
		}
	}

	// No cursor → announcement.
	fresh := dialFirehose(t, srv, "")
	waitCalls(1)
	fresh.Close()

	// Resuming consumer (cursor given) → no announcement.
	resume := dialFirehose(t, srv, "?cursor=1")
	time.Sleep(200 * time.Millisecond)
	waitCalls(1)
	resume.Close()

	// Malformed cursor is rejected before any hook.
	resp, err := http.Get(srv.URL + "/xrpc/com.atproto.sync.subscribeRepos?cursor=x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed cursor status %d", resp.StatusCode)
	}
	waitCalls(1)
}

// A consumer resuming with ?cursor=N must receive only events after N.
func TestCursorResumesStrictlyAfter(t *testing.T) {
	ctx := context.Background()
	s := newTestServer(t)
	e := echo.New()
	s.Register(e)
	srv := httptest.NewServer(e)
	defer srv.Close()

	did := "did:plc:cursortest"
	for i := 0; i < 3; i++ {
		if err := s.repos.EmitAccount(ctx, did, true, nil); err != nil {
			t.Fatal(err)
		}
	}

	conn := dialFirehose(t, srv, "?cursor=1")
	defer conn.Close()
	got := make(chan int64, 4)
	cb := &events.RepoStreamCallbacks{
		RepoAccount: func(evt *comatproto.SyncSubscribeRepos_Account) error {
			got <- evt.Seq
			return nil
		},
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go events.HandleRepoStream(streamCtx, conn, sequential.NewScheduler("test", cb.EventHandler), nil)

	var seqs []int64
	for len(seqs) < 2 {
		select {
		case seq := <-got:
			seqs = append(seqs, seq)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out, got %v", seqs)
		}
	}
	if seqs[0] != 2 || seqs[1] != 3 {
		t.Fatalf("resume from cursor 1 returned %v, want [2 3]", seqs)
	}
}
