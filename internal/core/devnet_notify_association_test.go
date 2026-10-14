package core_test

// Devnet E2E for the notification shape the inbound bridge relies on:
// notifications are associations on the bridged user's resources delivered
// to their real (restrict-readers + write-public) notify timeline, while the
// mirrored body record stays undistributed in the service namespace. The
// service account is the DEVNET_* account; the bridged user is a throwaway
// account registered by the test (devnet registration is open).
//
//	DEVNET_E2E=1 DEVNET_URL=http://localhost:8000 \
//	DEVNET_CCID=con1... DEVNET_PRIVKEY=<hex> go test ./internal/core/

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	concrnt "github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/schemas"

	"github.com/concrnt/atproto/internal/convert/toconcrnt"
	"github.com/concrnt/atproto/internal/core"
	"github.com/concrnt/atproto/internal/world"
)

// registerThrowaway creates a fresh account on the devnet and returns its
// ccid and master key.
func registerThrowaway(t *testing.T, baseURL string) (string, string) {
	t.Helper()
	resp, err := http.Get(baseURL + "/.well-known/concrnt")
	if err != nil {
		t.Fatalf("well-known failed: %v", err)
	}
	var wk concrnt.WellKnownConcrnt
	if err := json.NewDecoder(resp.Body).Decode(&wk); err != nil {
		t.Fatalf("well-known decode failed: %v", err)
	}
	resp.Body.Close()

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	priv := hex.EncodeToString(raw)
	ccid, err := concrnt.PrivKeyToAddr(priv, "con")
	if err != nil {
		t.Fatal(err)
	}
	doc := concrnt.Document[schemas.Entity]{
		Kind: "entity", Key: concrnt.ComposeCCURI("cckv", ccid, ""),
		Value: schemas.Entity{Domain: wk.Domain}, Author: ccid,
		Schema: schemas.EntityURL, CreatedAt: time.Now().UTC(),
	}
	sd, err := core.Sign(doc, priv)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(concrnt.RegisterRequest{SignedDocument: *sd})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/api/v2/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("captcha", "devnet-test-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("register failed: %d %s", resp.StatusCode, out)
	}
	return ccid, priv
}

// countAssociations lists the associations on target (anonymously; the
// target is a public post) with the given schema.
// timelineHas reports whether the timeline's recent items include href,
// polling briefly: the core delivers distributes through its job queue, so
// the reference lands a moment after the commit returns.
func timelineHas(t *testing.T, baseURL, timeline, href string) bool {
	t.Helper()
	q := url.Values{}
	q.Set("uris", timeline)
	q.Set("limit", "64")
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(baseURL + "/api/v2/timeline/recent?" + q.Encode())
		if err != nil {
			t.Fatalf("timeline query failed: %v", err)
		}
		var items []struct {
			Href string `json:"href"`
		}
		err = json.NewDecoder(resp.Body).Decode(&items)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("timeline decode failed: %v", err)
		}
		for _, it := range items {
			if it.Href == href {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func countAssociations(t *testing.T, baseURL, target, schema string) int {
	t.Helper()
	q := url.Values{}
	q.Set("uri", target)
	q.Set("schema", schema)
	q.Set("limit", "100")
	resp, err := http.Get(baseURL + "/api/v2/associations?" + q.Encode())
	if err != nil {
		t.Fatalf("associations query failed: %v", err)
	}
	defer resp.Body.Close()
	var result concrnt.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("associations decode failed: %v", err)
	}
	return len(result.Items)
}

func TestDevnetNotifyAssociations(t *testing.T) {
	if os.Getenv("DEVNET_E2E") != "1" {
		t.Skip("set DEVNET_E2E=1 to run against a devnet")
	}
	ccid := os.Getenv("DEVNET_CCID")
	privkey := os.Getenv("DEVNET_PRIVKEY")
	baseURL := os.Getenv("DEVNET_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8000"
	}
	if ccid == "" || privkey == "" {
		t.Fatal("DEVNET_CCID and DEVNET_PRIVKEY are required")
	}

	ctx := context.Background()
	svc := core.NewService(ccid, baseURL, privkey)

	// The bridged user: profile, a post to be interacted with, and the real
	// notify timeline shape the client creates (clientSetup.ts).
	userCCID, userPriv := registerThrowaway(t, baseURL)
	user := core.NewService(userCCID, baseURL, userPriv)
	profileKey := toconcrnt.MainProfile(userCCID)
	if _, err := core.Commit(ctx, user, concrnt.Document[world.ProfileValue]{
		Kind: "record", Key: profileKey, Schema: "https://schema.concrnt.world/p/main.json",
		Value: world.ProfileValue{Username: "Notify E2E"}, Author: userCCID, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("profile commit failed: %v", err)
	}
	notify := toconcrnt.NotifyTimeline(userCCID)
	restrictReaders := "https://policy.concrnt.world/t/restrict-readers.json"
	writePublic := "https://policy.concrnt.world/t/write-public.json"
	if _, err := core.Commit(ctx, user, concrnt.Document[map[string]any]{
		Kind: "record", Key: notify, Schema: "https://schema.concrnt.world/t/user.json",
		Value: map[string]any{}, Author: userCCID, CreatedAt: time.Now().UTC(),
		Policy: &concrnt.Policy{Entries: []concrnt.PolicyEntry{
			{URL: &restrictReaders, Params: &map[string]any{"entities": []string{userCCID}}},
			{URL: &writePublic},
		}},
	}); err != nil {
		t.Fatalf("notify timeline commit failed: %v", err)
	}
	postKey := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/posts/notifye2e%d", userCCID, time.Now().Unix())
	if _, err := core.Commit(ctx, user, concrnt.Document[world.MessageValue]{
		Kind: "record", Key: postKey, Schema: world.SchemaMarkdown,
		Value: world.MessageValue{Body: "hello"}, Author: userCCID, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("post commit failed: %v", err)
	}

	// The user's bridge inbox, shaped as views/Bluesky.tsx creates it:
	// allow-writers for the bridge's service account, no reader restriction.
	inbox := toconcrnt.InboxTimeline(userCCID)
	allowWriters := "https://policy.concrnt.world/t/allow-writers.json"
	if _, err := core.Commit(ctx, user, concrnt.Document[map[string]any]{
		Kind: "record", Key: inbox, Schema: "https://schema.concrnt.world/t/user.json",
		Value: map[string]any{}, Author: userCCID, CreatedAt: time.Now().UTC(),
		Policy: &concrnt.Policy{Entries: []concrnt.PolicyEntry{
			{URL: &allowWriters, Params: &map[string]any{"entities": []string{ccid}}},
		}},
	}); err != nil {
		t.Fatalf("inbox timeline commit failed: %v", err)
	}

	stamp := time.Now().Unix()
	actorDID := fmt.Sprintf("did:plc:e2eactor%d", stamp)
	override := &world.ProfileOverride{Username: "E2E Actor", Link: "https://bsky.app/profile/e2e.actor"}
	createdAt := time.Now().UTC().Truncate(time.Second)

	// 1. The reply body is delivered to the replied-to user's inbox (AP
	//    bridge parity: it shows up on their home timeline even when they
	//    do not follow the author) and never to the notify timeline, so it
	//    stays anonymously readable: the inbox carries no reader policy.
	replyAtURI := fmt.Sprintf("at://%s/app.bsky.feed.post/reply%d", actorDID, stamp)
	body := toconcrnt.BuildRecordDoc(ccid, replyAtURI, "bafyfake", []string{inbox}, createdAt)
	if _, err := core.Commit(ctx, svc, body); err != nil {
		t.Fatalf("body commit failed: %v", err)
	}
	gotBody, err := core.GetDocument[world.AtprotoRecord](ctx, svc, body.Key)
	if err != nil {
		t.Fatalf("inbox-distributed body must be anonymously readable: %v", err)
	}
	if gotBody.Value.AtURI != replyAtURI {
		t.Errorf("resolved wrong body: %+v", gotBody)
	}
	if !timelineHas(t, baseURL, inbox, body.Key) {
		t.Errorf("reply body %s not found in the replied-to user's inbox %s", body.Key, inbox)
	}

	// Control: the same record distributed into the notify timeline inherits
	// restrict-readers and is not readable anonymously (the bug this shape
	// avoids).
	controlAtURI := fmt.Sprintf("at://%s/app.bsky.feed.post/control%d", actorDID, stamp)
	control := toconcrnt.BuildRecordDoc(ccid, controlAtURI, "bafyfake", []string{notify}, createdAt)
	if _, err := core.Commit(ctx, svc, control); err != nil {
		t.Fatalf("control commit failed: %v", err)
	}
	var gotControl concrnt.Document[world.AtprotoRecord]
	if err := svc.Client.GetRecord(ctx, control.Key, &client.Options{NoCache: true}, &gotControl); err == nil {
		t.Errorf("record distributed to the notify timeline must not be anonymously readable")
	}

	// 2. Reply association on the user's post, delivered to the notify
	//    timeline, committed by the service account under write-public.
	reply := toconcrnt.BuildReplyAssociation(ccid, postKey, body.Key, override, []string{notify}, createdAt)
	replyResult, err := core.Commit(ctx, svc, reply)
	if err != nil {
		t.Fatalf("reply association commit failed: %v", err)
	}
	if replyResult.CCFS == nil {
		t.Fatalf("reply association response carried no ccfs: %+v", replyResult)
	}
	if n := countAssociations(t, baseURL, postKey, world.SchemaReplyAssociation); n != 1 {
		t.Errorf("expected 1 reply association, got %d", n)
	}

	// Byte-identical replay (jetstream cursor rewind) → same documentID →
	// the server treats it as a no-op, no duplicate association.
	if _, err := core.Commit(ctx, svc, reply); err != nil {
		t.Fatalf("replay commit failed: %v", err)
	}
	if n := countAssociations(t, baseURL, postKey, world.SchemaReplyAssociation); n != 1 {
		t.Errorf("replay must not duplicate: got %d reply associations", n)
	}

	// 3. Reroute association pointing at a mirrored repost body.
	repostAtURI := fmt.Sprintf("at://%s/app.bsky.feed.repost/rp%d", actorDID, stamp)
	repost := toconcrnt.BuildRerouteDoc(ccid, repostAtURI, postKey, override, nil, createdAt)
	if _, err := core.Commit(ctx, svc, repost); err != nil {
		t.Fatalf("repost body commit failed: %v", err)
	}
	reroute := toconcrnt.BuildRerouteAssociation(ccid, postKey, repost.Key, override, []string{notify}, createdAt)
	rerouteResult, err := core.Commit(ctx, svc, reroute)
	if err != nil {
		t.Fatalf("reroute association commit failed: %v", err)
	}
	if rerouteResult.CCFS == nil {
		t.Fatalf("reroute association response carried no ccfs: %+v", rerouteResult)
	}
	if n := countAssociations(t, baseURL, postKey, world.SchemaRerouteAssociation); n != 1 {
		t.Errorf("expected 1 reroute association, got %d", n)
	}

	// 4. Follow notification as an association on the user's main profile.
	follow := toconcrnt.BuildFollowNotifyAssociation(ccid, actorDID, profileKey, []string{notify}, createdAt)
	followResult, err := core.Commit(ctx, svc, follow)
	if err != nil {
		t.Fatalf("follow notification commit failed: %v", err)
	}
	if followResult.CCFS == nil {
		t.Fatalf("follow notification response carried no ccfs: %+v", followResult)
	}
	if n := countAssociations(t, baseURL, profileKey, world.SchemaAtprotoFollowNotify); n != 1 {
		t.Errorf("expected 1 follow notification, got %d", n)
	}
	if _, err := core.Commit(ctx, svc, follow); err != nil {
		t.Fatalf("follow replay commit failed: %v", err)
	}
	if n := countAssociations(t, baseURL, profileKey, world.SchemaAtprotoFollowNotify); n != 1 {
		t.Errorf("follow replay must not duplicate: got %d", n)
	}

	// 5. Deleting by ccfs (unlike / unfollow / post delete paths) removes
	//    the association.
	for _, ccfs := range []string{*replyResult.CCFS, *rerouteResult.CCFS, *followResult.CCFS} {
		if _, err := core.Commit(ctx, svc, toconcrnt.BuildDeleteDoc(ccid, ccfs)); err != nil {
			t.Fatalf("association delete failed for %s: %v", ccfs, err)
		}
	}
	if n := countAssociations(t, baseURL, postKey, world.SchemaReplyAssociation); n != 0 {
		t.Errorf("reply association still present after delete: %d", n)
	}
	if n := countAssociations(t, baseURL, postKey, world.SchemaRerouteAssociation); n != 0 {
		t.Errorf("reroute association still present after delete: %d", n)
	}
	if n := countAssociations(t, baseURL, profileKey, world.SchemaAtprotoFollowNotify); n != 0 {
		t.Errorf("follow notification still present after delete: %d", n)
	}

	// Cleanup the service-side bodies (the throwaway user is left behind).
	for _, key := range []string{body.Key, control.Key, repost.Key} {
		if _, err := core.Commit(ctx, svc, toconcrnt.BuildDeleteDoc(ccid, key)); err != nil {
			t.Errorf("cleanup delete failed for %s: %v", key, err)
		}
	}
}
