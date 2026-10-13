package inbound

import (
	"sync"

	"gorm.io/gorm"

	"github.com/concrnt/atproto/internal/store"
)

// liveIndex is an in-memory set of at-uris we have mirrored into concrnt.
// The full Jetstream delete stream is far too hot to check against Postgres
// per event; membership here gates the DB work. Reloaded periodically and
// updated synchronously on ingest.
type liveIndex struct {
	mu             sync.RWMutex
	mirroredPosts  map[string]bool // at_remote_posts (posts + repost subjects)
	inboundLikes   map[string]bool // at_uri_map ref_type=inbound-like
	inboundFollows map[string]bool // at_uri_map ref_type=inbound-follow
}

func newLiveIndex() *liveIndex {
	return &liveIndex{
		mirroredPosts:  map[string]bool{},
		inboundLikes:   map[string]bool{},
		inboundFollows: map[string]bool{},
	}
}

func (ix *liveIndex) reload(db *gorm.DB) error {
	var postURIs []string
	if err := db.Model(&store.RemotePost{}).Pluck("at_uri", &postURIs).Error; err != nil {
		return err
	}
	var likeURIs []string
	if err := db.Model(&store.URIMap{}).Where("ref_type = ?", "inbound-like").Pluck("at_uri", &likeURIs).Error; err != nil {
		return err
	}
	var followURIs []string
	if err := db.Model(&store.URIMap{}).Where("ref_type = ?", "inbound-follow").Pluck("at_uri", &followURIs).Error; err != nil {
		return err
	}

	posts := make(map[string]bool, len(postURIs))
	for _, u := range postURIs {
		posts[u] = true
	}
	likes := make(map[string]bool, len(likeURIs))
	for _, u := range likeURIs {
		likes[u] = true
	}
	follows := make(map[string]bool, len(followURIs))
	for _, u := range followURIs {
		follows[u] = true
	}

	ix.mu.Lock()
	ix.mirroredPosts = posts
	ix.inboundLikes = likes
	ix.inboundFollows = follows
	ix.mu.Unlock()
	return nil
}

func (ix *liveIndex) addPost(uri string) {
	ix.mu.Lock()
	ix.mirroredPosts[uri] = true
	ix.mu.Unlock()
}

func (ix *liveIndex) addLike(uri string) {
	ix.mu.Lock()
	ix.inboundLikes[uri] = true
	ix.mu.Unlock()
}

func (ix *liveIndex) addFollow(uri string) {
	ix.mu.Lock()
	ix.inboundFollows[uri] = true
	ix.mu.Unlock()
}

func (ix *liveIndex) remove(uri string) {
	ix.mu.Lock()
	delete(ix.mirroredPosts, uri)
	delete(ix.inboundLikes, uri)
	delete(ix.inboundFollows, uri)
	ix.mu.Unlock()
}

func (ix *liveIndex) hasPost(uri string) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.mirroredPosts[uri]
}

func (ix *liveIndex) hasLike(uri string) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.inboundLikes[uri]
}

func (ix *liveIndex) hasFollow(uri string) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.inboundFollows[uri]
}
