package ops

import "time"

// DegradedState is intentionally bounded so readiness and metrics labels stay
// stable even when the underlying error text is more detailed in logs.
type DegradedState struct {
	Code    string            `json:"reason_code,omitempty"`
	Reason  string            `json:"reason,omitempty"`
	Since   time.Time         `json:"since,omitempty"`
	PeerID  string            `json:"peer_id,omitempty"`
	RingID  string            `json:"ring_id,omitempty"`
	Context map[string]string `json:"context,omitempty"`
}

type Readiness struct {
	Ready    bool           `json:"ready"`
	Degraded *DegradedState `json:"degraded,omitempty"`
}

type Peer struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Local bool   `json:"local,omitempty"`
}

type PeerState struct {
	Mode                 string         `json:"mode"`
	LocalID              string         `json:"local_id,omitempty"`
	RingID               string         `json:"ring_id,omitempty"`
	Peers                []Peer         `json:"peers,omitempty"`
	CacheMode            string         `json:"cache_mode"`
	ReadSharding         string         `json:"read_sharding,omitempty"`
	PageShardingMinPages int64          `json:"page_sharding_min_pages,omitempty"`
	Ready                bool           `json:"ready"`
	Degraded             *DegradedState `json:"degraded,omitempty"`
	AuthConfigured       bool           `json:"auth_configured"`
}
