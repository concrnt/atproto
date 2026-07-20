// Package repoman glues indigo's carstore + repomgr + events together into
// the bridge's repo layer: it owns account creation (PLC registration, repo
// init) and record CRUD, and turns repomgr events into firehose frames.
package repoman

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/carstore"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/pebblepersist"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/util"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/keys"
	"github.com/concrnt/atproto/internal/plcm"
	"github.com/concrnt/atproto/internal/store"
)

type Manager struct {
	db   *gorm.DB
	cs   carstore.CarStore
	mgr  *repomgr.RepoManager
	em   *events.EventManager
	pp   *pebblepersist.PebblePersist
	keys *keys.Service
	plc  *plcm.Service

	mu       sync.RWMutex
	didByUid map[models.Uid]string
}

func New(db *gorm.DB, dataDir string, ks *keys.Service, plc *plcm.Service, retention time.Duration) (*Manager, error) {
	cs, err := carstore.NewCarStore(db, []string{filepath.Join(dataDir, "carstore")})
	if err != nil {
		return nil, fmt.Errorf("failed to open carstore: %w", err)
	}

	ppOpts := pebblepersist.DefaultPebblePersistOptions
	ppOpts.DbPath = filepath.Join(dataDir, "events")
	ppOpts.PersistDuration = retention
	pp, err := pebblepersist.NewPebblePersistance(&ppOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open event persistence: %w", err)
	}

	em := events.NewEventManager(pp)

	m := &Manager{
		db:       db,
		cs:       cs,
		mgr:      repomgr.NewRepoManager(cs, ks),
		em:       em,
		pp:       pp,
		keys:     ks,
		plc:      plc,
		didByUid: map[models.Uid]string{},
	}
	m.mgr.SetEventHandler(m.handleRepoEvent, false)
	return m, nil
}

// Events exposes the event manager for the firehose WebSocket handler.
func (m *Manager) Events() *events.EventManager { return m.em }

// RunGC runs pebble event garbage collection until ctx is done.
func (m *Manager) RunGC(ctx context.Context) { m.pp.GCThread(ctx) }

func (m *Manager) didForUid(uid models.Uid) (string, error) {
	m.mu.RLock()
	did, ok := m.didByUid[uid]
	m.mu.RUnlock()
	if ok {
		return did, nil
	}
	var ent store.Entity
	if err := m.db.Where("uid = ?", uid).First(&ent).Error; err != nil {
		return "", err
	}
	m.mu.Lock()
	m.didByUid[uid] = ent.DID
	m.mu.Unlock()
	return ent.DID, nil
}

func (m *Manager) handleRepoEvent(ctx context.Context, evt *repomgr.RepoEvent) {
	did, err := m.didForUid(evt.User)
	if err != nil || did == "" {
		slog.Error("repo event for unknown uid", "uid", evt.User, "error", err)
		return
	}

	ops := make([]*comatproto.SyncSubscribeRepos_RepoOp, 0, len(evt.Ops))
	for _, op := range evt.Ops {
		ops = append(ops, &comatproto.SyncSubscribeRepos_RepoOp{
			Action: string(op.Kind),
			Path:   op.Collection + "/" + op.Rkey,
			Cid:    (*lexutil.LexLink)(op.RecCid),
		})
	}

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Commit: lexutil.LexLink(evt.NewRoot),
		Rev:    evt.Rev,
		Since:  evt.Since,
		Blocks: evt.RepoSlice,
		Ops:    ops,
		Blobs:  []lexutil.LexLink{},
		Time:   time.Now().UTC().Format(util.ISO8601),
		// TODO(sync-v1.1): set PrevData (the previous commit's MST root) once
		// we track it; relays currently accept commits without it.
	}

	if err := m.em.AddEvent(ctx, &events.XRPCStreamEvent{
		RepoCommit: commit,
		PrivUid:    evt.User,
	}); err != nil {
		slog.Error("failed to enqueue firehose commit", "did", did, "error", err)
	}

	if err := m.db.Model(&store.Entity{}).Where("uid = ?", evt.User).
		Updates(map[string]any{"head_cid": evt.NewRoot.String(), "rev": evt.Rev}).Error; err != nil {
		slog.Error("failed to update entity head", "did", did, "error", err)
	}
}

// EmitIdentity broadcasts an #identity event for did.
func (m *Manager) EmitIdentity(ctx context.Context, did, handle string) error {
	return m.em.AddEvent(ctx, &events.XRPCStreamEvent{
		RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
			Did:    did,
			Handle: &handle,
			Time:   time.Now().UTC().Format(util.ISO8601),
		},
	})
}

// EmitAccount broadcasts an #account event for did.
func (m *Manager) EmitAccount(ctx context.Context, did string, active bool, status *string) error {
	return m.em.AddEvent(ctx, &events.XRPCStreamEvent{
		RepoAccount: &comatproto.SyncSubscribeRepos_Account{
			Did:    did,
			Active: active,
			Status: status,
			Time:   time.Now().UTC().Format(util.ISO8601),
		},
	})
}

// CreateAccount provisions a fully-functional bridged account: entity row,
// signing key, did:plc registration, repo init. It returns the stored entity.
// Event ordering matters: the DID must resolve on the PLC directory before
// any firehose event mentions it, or relays will reject the account.
func (m *Manager) CreateAccount(ctx context.Context, ccid, handle string) (*store.Entity, error) {
	var existing store.Entity
	if err := m.db.Where("ccid = ?", ccid).First(&existing).Error; err == nil {
		return nil, fmt.Errorf("ccid %s is already bridged as %s", ccid, existing.DID)
	}

	signingKey, err := keys.Generate()
	if err != nil {
		return nil, err
	}
	pub, err := signingKey.PublicKey()
	if err != nil {
		return nil, err
	}

	did, opCID, err := m.plc.CreateDID(ctx, pub.DIDKey(), handle)
	if err != nil {
		return nil, fmt.Errorf("plc registration failed: %w", err)
	}
	if err := m.keys.StoreSigningKey(did, signingKey); err != nil {
		return nil, err
	}

	ent := store.Entity{
		CCID:         ccid,
		DID:          did,
		Handle:       handle,
		PLCLastOpCID: opCID,
		Status:       "active",
		Enabled:      true,
	}
	if err := m.db.Create(&ent).Error; err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.didByUid[ent.Uid] = did
	m.mu.Unlock()

	if err := m.plc.WaitForRegistration(ctx, did); err != nil {
		return nil, err
	}

	if err := m.EmitIdentity(ctx, did, handle); err != nil {
		return nil, err
	}
	if err := m.EmitAccount(ctx, did, true, nil); err != nil {
		return nil, err
	}
	if err := m.mgr.InitNewActor(ctx, ent.Uid, handle, did, "", "", ""); err != nil {
		return nil, fmt.Errorf("repo init failed: %w", err)
	}

	return &ent, nil
}

// InitActor initializes an empty repo (with a bare profile) for an entity
// that already exists in the store. CreateAccount calls this implicitly;
// it is exposed for flows that provision the entity row separately.
func (m *Manager) InitActor(ctx context.Context, did, handle string) error {
	ent, err := m.entityByDID(did)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.didByUid[ent.Uid] = did
	m.mu.Unlock()
	return m.mgr.InitNewActor(ctx, ent.Uid, handle, did, "", "", "")
}

// GetRecordJSON reads a record out of did's repo as JSON.
func (m *Manager) GetRecordJSON(ctx context.Context, did, collection, rkey string) ([]byte, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return nil, err
	}
	_, rec, err := m.mgr.GetRecord(ctx, ent.Uid, collection, rkey, cid.Undef)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rec)
}

// CreateRecord writes rec into did's repo and returns (atURI, cid).
func (m *Manager) CreateRecord(ctx context.Context, did, collection string, rec cbg.CBORMarshaler) (string, string, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return "", "", err
	}
	path, cc, err := m.mgr.CreateRecord(ctx, ent.Uid, collection, rec)
	if err != nil {
		return "", "", err
	}
	return "at://" + did + "/" + path, cc.String(), nil
}

// PutRecord upserts a record at a fixed rkey (e.g. actor profile "self").
// repomgr.UpdateRecord is an MST put, so it works whether or not the record
// already exists; the firehose op reads as "update" which is fine for our
// only fixed-rkey use (the profile, created by InitNewActor).
func (m *Manager) PutRecord(ctx context.Context, did, collection, rkey string, rec cbg.CBORMarshaler) (string, string, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return "", "", err
	}
	cc, err := m.mgr.UpdateRecord(ctx, ent.Uid, collection, rkey, rec)
	if err != nil {
		return "", "", err
	}
	return "at://" + did + "/" + collection + "/" + rkey, cc.String(), nil
}

// DeleteRecord removes a record from did's repo.
func (m *Manager) DeleteRecord(ctx context.Context, did, collection, rkey string) error {
	ent, err := m.entityByDID(did)
	if err != nil {
		return err
	}
	return m.mgr.DeleteRecord(ctx, ent.Uid, collection, rkey)
}

func (m *Manager) entityByDID(did string) (*store.Entity, error) {
	var ent store.Entity
	if err := m.db.Where("did = ?", did).First(&ent).Error; err != nil {
		return nil, fmt.Errorf("no bridged entity for %s: %w", did, err)
	}
	return &ent, nil
}

// CarStore exposes the underlying carstore for the sync XRPC handlers.
func (m *Manager) CarStore() carstore.CarStore { return m.cs }

// RepoManager exposes the underlying repomgr (for MST record proofs).
func (m *Manager) RepoManager() *repomgr.RepoManager { return m.mgr }

// UidForDID resolves the carstore uid of a bridged did.
func (m *Manager) UidForDID(did string) (models.Uid, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return 0, err
	}
	return ent.Uid, nil
}
