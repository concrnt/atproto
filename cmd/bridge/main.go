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

	rotationKey, err := parseRotationKey(cfg.Atproto.MasterRotationKey)
	if err != nil {
		return err
	}
	plcSvc := plcm.NewService(cfg.Atproto.PLCDirectory, rotationKey, cfg.Atproto.PDSHost)

	repos, err := repoman.New(db, cfg.Atproto.DataDir, ks, plcSvc,
		time.Duration(cfg.Atproto.FirehoseRetentionHours)*time.Hour)
	if err != nil {
		return err
	}
	go repos.RunGC(ctx)

	blobSvc := blobs.NewService(db, cfg.Atproto.DataDir)
	coreSvc := core.NewService(cfg.Concrnt.CCID, cfg.Concrnt.Domain, cfg.Concrnt.PrivateKey)
	av := appview.NewClient(cfg.Atproto.Appview.URL)

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

	// Public PDS listener.
	pdsSrv := pds.NewServer(db, repos, blobSvc, cfg.Atproto.PDSHost, version)
	go func() {
		addr := fmt.Sprintf(":%d", cfg.Server.PDSPort)
		slog.Info("pds listener starting", "addr", addr, "host", cfg.Atproto.PDSHost)
		if err := pdsSrv.Echo().Start(addr); err != nil {
			slog.Error("pds listener stopped", "error", err)
			cancel()
		}
	}()

	// Management listener (behind the concrnt gateway).
	mgmtSrv := mgmt.NewServer(db, repos, av, cfg.Atproto.PDSHost, cfg.Atproto.Relays, version)
	mgmtSrv.OnProfileInit = func(ctx context.Context, ent *store.Entity) {
		if err := daemon.SyncProfile(ctx, ent); err != nil {
			slog.Error("initial profile sync failed", "ccid", ent.CCID, "error", err)
		}
	}
	go func() {
		addr := fmt.Sprintf(":%d", cfg.Server.Port)
		slog.Info("management listener starting", "addr", addr)
		if err := mgmtSrv.Echo().Start(addr); err != nil {
			slog.Error("management listener stopped", "error", err)
			cancel()
		}
	}()

	// Let relays know we exist (idempotent, only useful once we host repos).
	if !cfg.Atproto.DisableRelayNotify {
		var count int64
		db.Model(&store.Entity{}).Where("did <> ''").Count(&count)
		if count > 0 {
			go relay.RequestCrawl(ctx, cfg.Atproto.Relays, cfg.Atproto.PDSHost)
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
