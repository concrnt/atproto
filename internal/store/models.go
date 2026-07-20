package store

import (
	"time"

	"github.com/bluesky-social/indigo/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Entity is a bridged concrnt account exposed to the atproto network.
// Uid doubles as the carstore models.Uid.
type Entity struct {
	Uid             models.Uid `gorm:"column:uid;primaryKey;autoIncrement"`
	CCID            string     `gorm:"column:ccid;uniqueIndex;not null"`
	DID             string     `gorm:"column:did;uniqueIndex"`
	Handle          string     `gorm:"uniqueIndex"`
	PLCLastOpCID    string     `gorm:"column:plc_last_op_cid"`
	HeadCID         string     `gorm:"column:head_cid"`
	Rev             string
	Status          string `gorm:"default:active"` // active / deactivated
	ListenTimelines datatypes.JSONSlice[string]
	Enabled         bool `gorm:"default:true"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (Entity) TableName() string { return "at_entities" }

// Key holds an account-scoped secret key (currently only "signing", K256),
// encrypted at rest with the configured key-encryption key.
type Key struct {
	DID        string `gorm:"column:did;primaryKey"`
	KeyType    string `gorm:"primaryKey"` // signing
	PrivateEnc []byte `gorm:"column:private_enc;not null"`
	Public     string `gorm:"not null"` // did:key
	CreatedAt  time.Time
}

func (Key) TableName() string { return "at_keys" }

// Follow caches a concrnt user's follow of a Bluesky account. The source of
// truth is the user's follow document in their cckv space.
type Follow struct {
	CCID       string `gorm:"column:ccid;primaryKey"`
	SubjectDID string `gorm:"column:subject_did;primaryKey;index"`
	FollowRkey string `gorm:"column:follow_rkey"`
	CreatedAt  time.Time
}

func (Follow) TableName() string { return "at_follows" }

// URIMap relates atproto records with concrnt resources, in both directions.
type URIMap struct {
	AtURI      string `gorm:"column:at_uri;primaryKey"`
	CcURI      string `gorm:"column:cc_uri;index"`
	DID        string `gorm:"column:did;index"`
	Collection string
	Rkey       string
	CID        string `gorm:"column:cid"`
	// outbound-post / outbound-repost / outbound-like / outbound-follow /
	// inbound-like / inbound-post ...
	RefType   string         `gorm:"index"`
	Meta      datatypes.JSON `gorm:"type:jsonb"`
	CreatedAt time.Time
}

func (URIMap) TableName() string { return "at_uri_map" }

// RemotePost caches raw Bluesky records we ingested, for reply-root and
// subject-cid resolution.
type RemotePost struct {
	AtURI     string         `gorm:"column:at_uri;primaryKey"`
	DID       string         `gorm:"column:did;index"`
	CID       string         `gorm:"column:cid"`
	Record    datatypes.JSON `gorm:"type:jsonb"`
	IndexedAt time.Time
}

func (RemotePost) TableName() string { return "at_remote_posts" }

// Actor caches Bluesky actor profiles for profileOverride rendering.
type Actor struct {
	DID         string `gorm:"column:did;primaryKey"`
	Handle      string
	DisplayName string
	AvatarURL   string `gorm:"column:avatar_url"`
	FetchedAt   time.Time
}

func (Actor) TableName() string { return "at_actors" }

// Blob is an ingested media blob served via com.atproto.sync.getBlob.
type Blob struct {
	DID       string `gorm:"column:did;primaryKey"`
	CID       string `gorm:"column:cid;primaryKey"`
	Mime      string
	Size      int64
	SrcURL    string `gorm:"column:src_url"`
	CreatedAt time.Time
}

func (Blob) TableName() string { return "at_blobs" }

// Cursor persists stream positions ("jetstream", ...).
type Cursor struct {
	Name      string `gorm:"primaryKey"`
	Value     int64
	UpdatedAt time.Time
}

func (Cursor) TableName() string { return "at_cursors" }

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&Entity{},
		&Key{},
		&Follow{},
		&URIMap{},
		&RemotePost{},
		&Actor{},
		&Blob{},
		&Cursor{},
	)
}
