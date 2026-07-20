// Command bridge runs the concrnt⇔Bluesky bridge: a masqueraded read-only
// PDS (public listener), a management API behind the concrnt gateway, an
// outbound daemon exporting concrnt activity, and an inbound Jetstream
// consumer mirroring Bluesky activity.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/concrnt/atproto/internal/appview"
	"github.com/concrnt/atproto/internal/blobs"
	"github.com/concrnt/atproto/internal/config"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/inbound"
	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/mgmt"
	"github.com/concrnt/atproto/internal/outbound"
	"github.com/concrnt/atproto/internal/pds"
	"github.com/concrnt/atproto/internal/plcm"
	"github.com/concrnt/atproto/internal/relay"
	"github.com/concrnt/atproto/internal/repoman"
	"github.com/concrnt/atproto/internal/store"
)

var version = "dev"

type slogWriter struct{}

func (slogWriter) Printf(format string, args ...any) {
	slog.Warn(fmt.Sprintf(format, args...))
}

func parseRotationKey(s string) (atcrypto.PrivateKey, error) {
	if k, err := atcrypto.ParsePrivateMultibase(s); err == nil {
		return k, nil
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("masterRotationKey is neither multibase nor hex")
	}
	return atcrypto.ParsePrivateBytesK256(raw)
}

func run() error {
	cfg, err := config.Load(os.Getenv("CONFIG_PATH"))
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(cfg.Atproto.DataDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	db, err := gorm.Open(postgres.Open(cfg.Database.URL), &gorm.Config{
		Logger: logger.New(slogWriter{}, logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
		}),
	})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	if err := store.AutoMigrate(db); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	ks, err := keys.NewService(db, cfg.Atproto.KeyEncryptionKey)
	if err != nil {
		return err
	}

	// Core connection first: its resolved FQDN is the public origin the
	// gateway serves on, which doubles as the bridge's PDS host (the atproto
	// serviceEndpoint and the relay crawl hostname).
	blobSvc := blobs.NewService(db, cfg.Atproto.DataDir)
	coreSvc := core.NewService(cfg.Concrnt.CCID, cfg.Concrnt.Domain, cfg.Concrnt.PrivateKey)
	av := appview.NewClient(cfg.Atproto.Appview.URL)
	pdsHost := coreSvc.FQDN()
	if pdsHost == "" {
		return fmt.Errorf("could not determine concrnt FQDN; check concrnt.domain")
	}

	rotationKey, err := parseRotationKey(cfg.Atproto.MasterRotationKey)
	if err != nil {
		return err
	}
	plcSvc := plcm.NewService(cfg.Atproto.PLCDirectory, rotationKey, pdsHost)

	repos, err := repoman.New(db, cfg.Atproto.DataDir, ks, plcSvc,
		time.Duration(cfg.Atproto.FirehoseRetentionHours)*time.Hour)
	if err != nil {
		return err
	}
	go repos.RunGC(ctx)

	reload := time.Duration(cfg.Atproto.EntityReloadIntervalSecs) * time.Second

	// Outbound: concrnt events → repo writes.
	daemon := outbound.NewDaemon(db, coreSvc, repos, blobSvc, cfg.Concrnt.PostURLTemplate, reload)
	events, err := core.Events(ctx, cfg.Redis.URL)
	if err != nil {
		return fmt.Errorf("failed to subscribe to concrnt events: %w", err)
	}
	go daemon.Run(ctx, events)

	// Inbound: Jetstream → concrnt commits.
	ingester := inbound.NewIngester(db, coreSvc, av,
		time.Duration(cfg.Atproto.ActorCacheTTLMinutes)*time.Minute)
	consumer := inbound.NewConsumer(db, cfg.Atproto.Jetstream, ingester, reload)
	go consumer.Run(ctx)

	// A single listener carries everything; the concrnt gateway proxies both
	// the public PDS routes (/xrpc) and the management API
	// (/cc-info, /atproto/api). The bridge must not be exposed directly.
	e := echo.New()
	e.HideBanner = true
	e.Use(echomiddleware.Recover())

	pds.NewServer(db, repos, blobSvc, pdsHost, version).Register(e)

	mgmtSrv := mgmt.NewServer(db, repos, av, pdsHost, cfg.Atproto.Relays, version, cfg.Concrnt.CCID)
	mgmtSrv.OnProfileInit = func(ctx context.Context, ent *store.Entity) {
		if err := daemon.SyncProfile(ctx, ent); err != nil {
			slog.Error("initial profile sync failed", "ccid", ent.CCID, "error", err)
		}
	}
	mgmtSrv.Register(e)

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Server.Port)
		slog.Info("bridge listener starting", "addr", addr, "pdsHost", pdsHost)
		if err := e.Start(addr); err != nil {
			slog.Error("listener stopped", "error", err)
			cancel()
		}
	}()

	// Let relays know we exist (idempotent, only useful once we host repos).
	if !cfg.Atproto.DisableRelayNotify {
		var count int64
		db.Model(&store.Entity{}).Where("did <> '' AND status = ?", "active").Count(&count)
		if count > 0 {
			go relay.RequestCrawl(ctx, cfg.Atproto.Relays, pdsHost)
		}
	}

	<-ctx.Done()
	slog.Info("shutting down")
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
