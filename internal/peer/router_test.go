package peer

import "testing"

func TestRouterChoosesDeterministicOwner(t *testing.T) {
	router, err := NewRouter("cache-0", []Peer{
		{ID: "cache-2", URL: "http://cache-2"},
		{ID: "cache-0", URL: "http://cache-0"},
		{ID: "cache-1", URL: "http://cache-1"},
	})
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	first := router.Owner("photos", "2026/cat.jpg")
	for i := 0; i < 10; i++ {
		if got := router.Owner("photos", "2026/cat.jpg"); got != first {
			t.Fatalf("Owner() = %+v, want %+v", got, first)
		}
	}
}

func TestRouterIsIndependentOfPeerOrder(t *testing.T) {
	peers := []Peer{
		{ID: "cache-0", URL: "http://cache-0"},
		{ID: "cache-1", URL: "http://cache-1"},
		{ID: "cache-2", URL: "http://cache-2"},
	}
	reversed := []Peer{
		{ID: "cache-2", URL: "http://cache-2"},
		{ID: "cache-1", URL: "http://cache-1"},
		{ID: "cache-0", URL: "http://cache-0"},
	}
	a, err := NewRouter("cache-0", peers)
	if err != nil {
		t.Fatalf("NewRouter(a) error = %v", err)
	}
	b, err := NewRouter("cache-0", reversed)
	if err != nil {
		t.Fatalf("NewRouter(b) error = %v", err)
	}

	for _, key := range []string{"a", "b", "prefix/object.parquet", "nested/key"} {
		if got, want := a.Owner("bucket", key), b.Owner("bucket", key); got != want {
			t.Fatalf("Owner(%q) differs: %+v != %+v", key, got, want)
		}
	}
}

func TestRouterDistributesObjectsAcrossPeers(t *testing.T) {
	router, err := NewRouter("cache-0", []Peer{
		{ID: "cache-0", URL: "http://cache-0"},
		{ID: "cache-1", URL: "http://cache-1"},
		{ID: "cache-2", URL: "http://cache-2"},
		{ID: "cache-3", URL: "http://cache-3"},
	})
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	seen := map[string]bool{}
	for i := 0; i < 128; i++ {
		seen[router.Owner("bucket", string(rune('a'+i))).ID] = true
	}
	if len(seen) < 3 {
		t.Fatalf("Owner() distribution touched %d peers, want at least 3", len(seen))
	}
}

func TestRouterRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name    string
		localID string
		peers   []Peer
	}{
		{name: "missing local", localID: "", peers: []Peer{{ID: "cache-0", URL: "http://cache-0"}}},
		{name: "no peers", localID: "cache-0"},
		{name: "local absent", localID: "cache-0", peers: []Peer{{ID: "cache-1", URL: "http://cache-1"}}},
		{name: "duplicate", localID: "cache-0", peers: []Peer{{ID: "cache-0", URL: "http://cache-0"}, {ID: "cache-0", URL: "http://cache-0"}}},
		{name: "missing url", localID: "cache-0", peers: []Peer{{ID: "cache-0"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRouter(tt.localID, tt.peers); err == nil {
				t.Fatal("NewRouter() error = nil, want error")
			}
		})
	}
}
