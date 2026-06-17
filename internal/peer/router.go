package peer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	ForwardedHeader = "X-Simple-S3-Cache-Peer-Forwarded"
	OwnerHeader     = "X-Simple-S3-Cache-Peer-Owner"
	FromHeader      = "X-Simple-S3-Cache-Peer-From"
	RingHeader      = "X-Simple-S3-Cache-Peer-Ring"
	TimestampHeader = "X-Simple-S3-Cache-Peer-Timestamp"
	SignatureHeader = "X-Simple-S3-Cache-Peer-Signature"
)

type Peer struct {
	ID  string
	URL string
}

type Router struct {
	localID string
	peers   []Peer
	byID    map[string]Peer
	ringID  string
}

func NewRouter(localID string, peers []Peer) (*Router, error) {
	return newRouter(localID, peers, true)
}

func NewOwnerRouter(peers []Peer) (*Router, error) {
	return newRouter("", peers, false)
}

func newRouter(localID string, peers []Peer, requireLocal bool) (*Router, error) {
	localID = strings.TrimSpace(localID)
	if requireLocal && localID == "" {
		return nil, errors.New("local peer id is required")
	}
	if len(peers) == 0 {
		return nil, errors.New("peer list is required")
	}

	copied := make([]Peer, 0, len(peers))
	byID := make(map[string]Peer, len(peers))
	for _, p := range peers {
		p.ID = strings.TrimSpace(p.ID)
		p.URL = strings.TrimSpace(p.URL)
		if p.ID == "" {
			return nil, errors.New("peer id is required")
		}
		if p.URL == "" {
			return nil, fmt.Errorf("peer %q url is required", p.ID)
		}
		if _, ok := byID[p.ID]; ok {
			return nil, fmt.Errorf("peer id %q is duplicated", p.ID)
		}
		copied = append(copied, p)
		byID[p.ID] = p
	}
	sort.Slice(copied, func(i, j int) bool {
		return copied[i].ID < copied[j].ID
	})
	if requireLocal {
		if _, ok := byID[localID]; !ok {
			return nil, fmt.Errorf("local peer id %q is not in peer list", localID)
		}
	} else if localID != "" {
		if _, ok := byID[localID]; !ok {
			return nil, fmt.Errorf("local peer id %q is not in peer list", localID)
		}
	}

	return &Router{
		localID: localID,
		peers:   copied,
		byID:    byID,
		ringID:  ringFingerprint(copied),
	}, nil
}

func (r *Router) LocalID() string {
	return r.localID
}

func (r *Router) Peers() []Peer {
	peers := make([]Peer, len(r.peers))
	copy(peers, r.peers)
	return peers
}

func (r *Router) RingID() string {
	return r.ringID
}

func (r *Router) Owner(bucket, key string) Peer {
	routingKey := bucket + "/" + key
	return r.ownerForRoutingKey(routingKey)
}

func PageOwnerKey(bucket, key string, pageIndex int64) string {
	return bucket + "/" + key + "\x00page\x00" + strconv.FormatInt(pageIndex, 10)
}

func (r *Router) PageOwner(bucket, key string, pageIndex int64) Peer {
	if pageIndex < 0 {
		return Peer{}
	}
	return r.ownerForRoutingKey(PageOwnerKey(bucket, key, pageIndex))
}

func (r *Router) ownerForRoutingKey(routingKey string) Peer {
	var owner Peer
	var bestScore uint64
	for i, p := range r.peers {
		score := rendezvousScore(routingKey, p.ID)
		if i == 0 || score > bestScore || score == bestScore && p.ID < owner.ID {
			bestScore = score
			owner = p
		}
	}
	return owner
}

func (r *Router) IsLocalOwner(bucket, key string) bool {
	return r.localID != "" && r.Owner(bucket, key).ID == r.localID
}

func (r *Router) IsLocalPageOwner(bucket, key string, pageIndex int64) bool {
	return r.localID != "" && r.PageOwner(bucket, key, pageIndex).ID == r.localID
}

func (r *Router) PeerByID(id string) (Peer, bool) {
	p, ok := r.byID[id]
	return p, ok
}

func rendezvousScore(key, peerID string) uint64 {
	sum := sha256.Sum256([]byte(key + "\x00" + peerID))
	return binary.BigEndian.Uint64(sum[:8])
}

func ringFingerprint(peers []Peer) string {
	hash := sha256.New()
	for _, p := range peers {
		_, _ = hash.Write([]byte(p.ID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(p.URL))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
