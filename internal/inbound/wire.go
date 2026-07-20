package inbound

// Jetstream wire-v1 event shapes (the /subscribe endpoint served by the
// public jetstream*.bsky.network instances). The official Go client speaks
// only the newer /subscribe-v2 protocol, which public instances do not
// expose yet, so we consume the v1 JSON stream directly.

type Kind string

const (
	KindCommit   Kind = "commit"
	KindIdentity Kind = "identity"
	KindAccount  Kind = "account"
)

type Operation string

const (
	OpCreate Operation = "create"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

type Event struct {
	DID    string  `json:"did"`
	TimeUS int64   `json:"time_us"`
	Kind   Kind    `json:"kind"`
	Commit *Commit `json:"commit,omitempty"`
}

type Commit struct {
	Operation  Operation      `json:"operation"`
	Collection string         `json:"collection"`
	Rkey       string         `json:"rkey"`
	Rev        string         `json:"rev"`
	CID        string         `json:"cid,omitempty"`
	Record     map[string]any `json:"record,omitempty"`
}
