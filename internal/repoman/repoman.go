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
	"sort"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/carstore"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/pebblepersist"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/repo"
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

	announceMu   sync.Mutex
	lastAnnounce time.Time

	// seqMu serializes sequence assignment with persistence so the persisted
	// stream is strictly increasing; seq is the last number handed out.
	seqMu sync.Mutex
	seq   int64
}

// announceInterval throttles AnnounceActiveAccounts: a relay connects without
// a cursor once, but dev tools (goat firehose) may reconnect repeatedly.
const announceInterval = time.Minute

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
	// Sequence numbers are the bridge's job: pebblepersist stores whatever
	// Sequence() returns and never assigns one. Continue from the last
	// persisted event so cursors survive restarts. (Walks the retained
	// window with Playback rather than GetLast, which leaks its iterator
	// and then makes Shutdown fail.)
	if err := pp.Playback(context.Background(), 0, func(evt *events.XRPCStreamEvent) error {
		if seq := evt.Sequence(); seq > m.seq {
			m.seq = seq
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to read persisted firehose events: %w", err)
	}
	m.mgr.SetEventHandler(m.handleRepoEvent, false)
	return m, nil
}

// Close releases the firehose event store so the data dir can be reopened
// (tests simulating a restart). Every write is already synced, so the
// process does not need this at shutdown; closing under a live cursor
// playback would panic.
func (m *Manager) Close(ctx context.Context) error {
	return m.pp.Shutdown(ctx)
}

// addEvent stamps the next sequence number on evt and persists/broadcasts
// it. Relay consumers drop any event whose seq is not greater than the last
// one they saw on the connection (indigo relay: "got events out of order
// from stream"), so unsequenced events (all seq 0) let exactly one event
// through per connection and silently lose everything after it; they also
// never advance the relay's cursor, so every reconnect starts cursor-less.
func (m *Manager) addEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	m.seqMu.Lock()
	defer m.seqMu.Unlock()
	m.seq++
	switch {
	case evt.RepoCommit != nil:
		evt.RepoCommit.Seq = m.seq
	case evt.RepoIdentity != nil:
		evt.RepoIdentity.Seq = m.seq
	case evt.RepoAccount != nil:
		evt.RepoAccount.Seq = m.seq
	case evt.RepoSync != nil:
		evt.RepoSync.Seq = m.seq
	}
	return m.em.AddEvent(ctx, evt)
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
	}
	// Sync v1.1: prevData is the previous commit's MST root. Strict relays
	// (bsky.network) accept a commit without it only when they have no prior
	// state for the account, and drop every later one ("missing prevData
	// field"), so only the first commit a relay sees would ever land.
	if evt.OldRoot != nil {
		prevData, err := m.commitDataCID(ctx, evt.User, *evt.OldRoot)
		if err != nil {
			slog.Error("failed to read previous commit for prevData", "did", did, "error", err)
		} else {
			commit.PrevData = (*lexutil.LexLink)(&prevData)
		}
	}

	if err := m.addEvent(ctx, &events.XRPCStreamEvent{
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

// commitDataCID reads the MST root (`data`) of the commit at root out of the
// user's carstore.
func (m *Manager) commitDataCID(ctx context.Context, uid models.Uid, root cid.Cid) (cid.Cid, error) {
	ds, err := m.cs.ReadOnlySession(uid)
	if err != nil {
		return cid.Undef, err
	}
	r, err := repo.OpenRepo(ctx, ds, root)
	if err != nil {
		return cid.Undef, err
	}
	return r.DataCid(), nil
}

// EmitIdentity broadcasts an #identity event for did.
func (m *Manager) EmitIdentity(ctx context.Context, did, handle string) error {
	return m.addEvent(ctx, &events.XRPCStreamEvent{
		RepoIdentity: &comatproto.SyncSubscribeRepos_Identity{
			Did:    did,
			Handle: &handle,
			Time:   time.Now().UTC().Format(util.ISO8601),
		},
	})
}

// EmitAccount broadcasts an #account event for did.
func (m *Manager) EmitAccount(ctx context.Context, did string, active bool, status *string) error {
	return m.addEvent(ctx, &events.XRPCStreamEvent{
		RepoAccount: &comatproto.SyncSubscribeRepos_Account{
			Did:    did,
			Active: active,
			Status: status,
			Time:   time.Now().UTC().Format(util.ISO8601),
		},
	})
}

// AnnounceAccount re-broadcasts #identity and #account for an active account.
// Relays and the appview only learn about an account from firehose events,
// so this is how a consumer that missed the activation events (or whose
// handle verification failed at the time) gets told again.
func (m *Manager) AnnounceAccount(ctx context.Context, did string) error {
	ent, err := m.entityByDID(did)
	if err != nil {
		return err
	}
	if ent.Status != "active" {
		return fmt.Errorf("account %s is not active", did)
	}
	if err := m.EmitIdentity(ctx, did, ent.Handle); err != nil {
		return err
	}
	return m.EmitAccount(ctx, did, true, nil)
}

// AnnounceActiveAccounts re-broadcasts #identity/#account for every active
// account, throttled to once per announceInterval. It is called when a
// firehose consumer connects without a cursor: a newly subscribed relay
// starts at the live position and never backfills, so events emitted
// before it connected (in particular this host's first activation, which
// precedes the relay's own requestCrawl round-trip) are otherwise lost.
// Returns the number of accounts announced (0 when throttled).
func (m *Manager) AnnounceActiveAccounts(ctx context.Context) (int, error) {
	m.announceMu.Lock()
	if time.Since(m.lastAnnounce) < announceInterval {
		m.announceMu.Unlock()
		return 0, nil
	}
	m.lastAnnounce = time.Now()
	m.announceMu.Unlock()

	var ents []store.Entity
	if err := m.db.Where("status = ? AND did <> ''", "active").Find(&ents).Error; err != nil {
		return 0, err
	}
	for _, ent := range ents {
		if err := m.EmitIdentity(ctx, ent.DID, ent.Handle); err != nil {
			return 0, err
		}
		if err := m.EmitAccount(ctx, ent.DID, true, nil); err != nil {
			return 0, err
		}
	}
	return len(ents), nil
}

// CreatePending provisions a bridged account up to (but not including)
// activation: a signing key, a did:plc that claims `handle` as its
// alsoKnownAs, and a pending entity row. No repo, firehose event, or relay
// crawl happens yet — the handle is unverified, so the account must not be
// visible to the network until Activate.
func (m *Manager) CreatePending(ctx context.Context, ccid, handle string) (*store.Entity, error) {
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
		Status:       "pending",
		Enabled:      true,
	}
	if err := m.db.Create(&ent).Error; err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.didByUid[ent.Uid] = did
	m.mu.Unlock()

	return &ent, nil
}

// ChangePendingHandle swaps the handle a still-unverified account claims:
// a PLC update op replacing alsoKnownAs on the same DID, then the row. The
// DID (and so the DNS TXT value) is unchanged, and nothing has reached the
// network yet, so there is nothing else to undo.
func (m *Manager) ChangePendingHandle(ctx context.Context, did, newHandle string) (*store.Entity, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return nil, err
	}
	if ent.Status != "pending" {
		return nil, fmt.Errorf("account %s is %s, handle can only change while pending", did, ent.Status)
	}
	signingKey, err := m.keys.SigningKey(did)
	if err != nil {
		return nil, err
	}
	pub, err := signingKey.PublicKey()
	if err != nil {
		return nil, err
	}
	opCID, err := m.plc.UpdateHandle(ctx, did, pub.DIDKey(), ent.PLCLastOpCID, newHandle)
	if err != nil {
		return nil, fmt.Errorf("plc handle update failed: %w", err)
	}
	if err := m.db.Model(&store.Entity{}).Where("uid = ?", ent.Uid).
		Updates(map[string]any{"handle": newHandle, "plc_last_op_cid": opCID}).Error; err != nil {
		return nil, err
	}
	ent.Handle = newHandle
	ent.PLCLastOpCID = opCID
	return ent, nil
}

// Activate turns a pending account live: it waits for the DID to resolve on
// the PLC directory, then emits #identity → #account → the initial #commit
// (repo init) in that order. Relays validate the DID before accepting
// commits, so the ordering is load-bearing.
func (m *Manager) Activate(ctx context.Context, did string) error {
	ent, err := m.entityByDID(did)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.didByUid[ent.Uid] = did
	m.mu.Unlock()

	if err := m.plc.WaitForRegistration(ctx, did); err != nil {
		return err
	}
	if err := m.EmitIdentity(ctx, did, ent.Handle); err != nil {
		return err
	}
	if err := m.EmitAccount(ctx, did, true, nil); err != nil {
		return err
	}
	if err := m.mgr.InitNewActor(ctx, ent.Uid, ent.Handle, did, "", "", ""); err != nil {
		return fmt.Errorf("repo init failed: %w", err)
	}

	return m.db.Model(&store.Entity{}).Where("uid = ?", ent.Uid).
		Update("status", "active").Error
}

// InitActor initializes just the repo (bare profile) for an entity that
// already exists, without PLC registration or firehose account events.
// Used where the PLC/handle lifecycle is handled separately (e.g. tests).
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
// Every repomgr write path (UpdateRecord, BatchWrite update) ends in
// mst.Add, which fails with "value already set" when the key exists, so an
// existing record is replaced as delete+put inside one BatchWrite commit:
// a single firehose #commit whose ops read delete then update for the path.
func (m *Manager) PutRecord(ctx context.Context, did, collection, rkey string, rec cbg.CBORMarshaler) (string, string, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return "", "", err
	}
	atURI := "at://" + did + "/" + collection + "/" + rkey

	if _, _, err := m.mgr.GetRecord(ctx, ent.Uid, collection, rkey, cid.Undef); err != nil {
		cc, err := m.mgr.UpdateRecord(ctx, ent.Uid, collection, rkey, rec)
		if err != nil {
			return "", "", err
		}
		return atURI, cc.String(), nil
	}

	writes := []*comatproto.RepoApplyWrites_Input_Writes_Elem{
		{RepoApplyWrites_Delete: &comatproto.RepoApplyWrites_Delete{Collection: collection, Rkey: rkey}},
		{RepoApplyWrites_Update: &comatproto.RepoApplyWrites_Update{
			Collection: collection,
			Rkey:       rkey,
			Value:      &lexutil.LexiconTypeDecoder{Val: rec},
		}},
	}
	if err := m.mgr.BatchWrite(ctx, ent.Uid, writes); err != nil {
		return "", "", err
	}
	cc, _, err := m.mgr.GetRecord(ctx, ent.Uid, collection, rkey, cid.Undef)
	if err != nil {
		return "", "", err
	}
	return atURI, cc.String(), nil
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

// openRepo opens a read-only view of uid's current repo.
func (m *Manager) openRepo(ctx context.Context, uid models.Uid) (*repo.Repo, error) {
	head, err := m.cs.GetUserRepoHead(ctx, uid)
	if err != nil {
		return nil, err
	}
	ds, err := m.cs.ReadOnlySession(uid)
	if err != nil {
		return nil, err
	}
	return repo.OpenRepo(ctx, ds, head)
}

// RecordItem is one entry of ListRecords.
type RecordItem struct {
	Rkey  string
	CID   string
	Value map[string]any
}

// GetRecordRaw reads a record as generic atproto data, so collections the
// bridge has no typed decoder for (whatever a client stored) still round-trip.
func (m *Manager) GetRecordRaw(ctx context.Context, did, collection, rkey string) (string, map[string]any, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return "", nil, err
	}
	r, err := m.openRepo(ctx, ent.Uid)
	if err != nil {
		return "", nil, err
	}
	cc, raw, err := r.GetRecordBytes(ctx, collection+"/"+rkey)
	if err != nil {
		return "", nil, err
	}
	val, err := atdata.UnmarshalCBOR(*raw)
	if err != nil {
		return "", nil, err
	}
	return cc.String(), val, nil
}

// ListRecords pages through one collection in rkey order. cursor is the
// last rkey of the previous page; the returned cursor is empty on the last
// page. Repos here are small, so the MST walk collects keys in memory.
func (m *Manager) ListRecords(ctx context.Context, did, collection string, limit int, cursor string, reverse bool) ([]RecordItem, string, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return nil, "", err
	}
	r, err := m.openRepo(ctx, ent.Uid)
	if err != nil {
		return nil, "", err
	}
	type entry struct {
		rkey string
		cid  cid.Cid
	}
	var all []entry
	prefix := collection + "/"
	err = r.ForEach(ctx, prefix, func(k string, v cid.Cid) error {
		if !strings.HasPrefix(k, prefix) {
			return nil
		}
		all = append(all, entry{rkey: strings.TrimPrefix(k, prefix), cid: v})
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	if reverse {
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	}
	start := 0
	if cursor != "" {
		for i, e := range all {
			if e.rkey == cursor {
				start = i + 1
				break
			}
		}
	}
	if limit <= 0 {
		limit = 50
	}
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	items := make([]RecordItem, 0, end-start)
	for _, e := range all[start:end] {
		_, raw, err := r.GetRecordBytes(ctx, prefix+e.rkey)
		if err != nil {
			return nil, "", err
		}
		val, err := atdata.UnmarshalCBOR(*raw)
		if err != nil {
			return nil, "", err
		}
		items = append(items, RecordItem{Rkey: e.rkey, CID: e.cid.String(), Value: val})
	}
	next := ""
	if end < len(all) && len(items) > 0 {
		next = items[len(items)-1].Rkey
	}
	return items, next, nil
}

// ListCollections returns the NSIDs that have at least one record in did's repo.
func (m *Manager) ListCollections(ctx context.Context, did string) ([]string, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return nil, err
	}
	r, err := m.openRepo(ctx, ent.Uid)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	err = r.ForEach(ctx, "", func(k string, _ cid.Cid) error {
		coll, _, ok := strings.Cut(k, "/")
		if ok && !seen[coll] {
			seen[coll] = true
			out = append(out, coll)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// Rev returns the current repo revision of did.
func (m *Manager) Rev(ctx context.Context, did string) (string, error) {
	ent, err := m.entityByDID(did)
	if err != nil {
		return "", err
	}
	return m.cs.GetUserRepoRev(ctx, ent.Uid)
}
