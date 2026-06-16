package peer

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type Peer struct {
	ID  string
	URL string
}

type Router struct {
	localID string
	peers   []Peer
	byID    map[string]Peer
}

func NewRouter(localID string, peers []Peer) (*Router, error) {
	localID = strings.TrimSpace(localID)
	if localID == "" {
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
	if _, ok := byID[localID]; !ok {
		return nil, fmt.Errorf("local peer id %q is not in peer list", localID)
	}

	return &Router{
		localID: localID,
		peers:   copied,
		byID:    byID,
	}, nil
}

func (r *Router) LocalID() string {
	return r.localID
}

func (r *Router) Owner(bucket, key string) Peer {
	routingKey := bucket + "/" + key
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
	return r.Owner(bucket, key).ID == r.localID
}

func (r *Router) PeerByID(id string) (Peer, bool) {
	p, ok := r.byID[id]
	return p, ok
}

func rendezvousScore(key, peerID string) uint64 {
	sum := sha256.Sum256([]byte(key + "\x00" + peerID))
	return binary.BigEndian.Uint64(sum[:8])
}
