package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ekkuleivonen/simple-s3-cache/internal/peer"
)

func TestGatewayRoutesObjectRequestToOwner(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}

	cache0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits["cache-0"]++
		mu.Unlock()
		if got := r.Header.Get(peer.ForwardedHeader); got != "1" {
			t.Fatalf("forwarded header = %q, want 1", got)
		}
		if got := r.Header.Get(peer.OwnerHeader); got != "cache-0" {
			t.Fatalf("owner header = %q, want cache-0", got)
		}
		if got := r.Header.Get(peer.FromHeader); got != "gateway" {
			t.Fatalf("from header = %q, want gateway", got)
		}
		w.Header().Set("X-Cache-Peer", "cache-0")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("from cache-0"))
	}))
	defer cache0.Close()
	cache1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits["cache-1"]++
		mu.Unlock()
		if got := r.Header.Get(peer.ForwardedHeader); got != "1" {
			t.Fatalf("forwarded header = %q, want 1", got)
		}
		if got := r.Header.Get(peer.OwnerHeader); got != "cache-1" {
			t.Fatalf("owner header = %q, want cache-1", got)
		}
		if got := r.Header.Get(peer.FromHeader); got != "gateway" {
			t.Fatalf("from header = %q, want gateway", got)
		}
		w.Header().Set("X-Cache-Peer", "cache-1")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("from cache-1"))
	}))
	defer cache1.Close()

	router, err := peer.NewOwnerRouter([]peer.Peer{
		{ID: "cache-1", URL: cache1.URL},
		{ID: "cache-0", URL: cache0.URL},
	})
	if err != nil {
		t.Fatalf("NewOwnerRouter() error = %v", err)
	}
	handler := New(Options{Router: router})

	req := httptest.NewRequest(http.MethodGet, "/photos/2026/cat.jpg?versionId=1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	owner := router.Owner("photos", "2026/cat.jpg")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if got := rec.Header().Get("X-Cache-Peer"); got != owner.ID {
		t.Fatalf("X-Cache-Peer = %q, want owner %q", got, owner.ID)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits[owner.ID] != 1 {
		t.Fatalf("owner hits = %d, want 1 (all hits: %+v)", hits[owner.ID], hits)
	}
	for id, count := range hits {
		if id != owner.ID && count != 0 {
			t.Fatalf("non-owner %s got %d hits", id, count)
		}
	}
}

func TestGatewayPreservesHostSigningHeadersAndBody(t *testing.T) {
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "s3.example.test" {
			t.Fatalf("Host = %q, want original host", r.Host)
		}
		if got := r.Header.Get("Authorization"); got != "AWS4-HMAC-SHA256 signed" {
			t.Fatalf("Authorization = %q, want preserved signing header", got)
		}
		if got := r.Header.Get("X-Amz-Date"); got != "20260617T023700Z" {
			t.Fatalf("X-Amz-Date = %q, want preserved signing header", got)
		}
		if got := r.Header.Get("Connection"); got != "" {
			t.Fatalf("Connection header forwarded as %q", got)
		}
		if got := r.Header.Get(peer.ForwardedHeader); got != "1" {
			t.Fatalf("forwarded header = %q, want gateway value", got)
		}
		if got := r.Header.Get(peer.OwnerHeader); got != "cache-0" {
			t.Fatalf("owner header = %q, want gateway-selected owner", got)
		}
		if got := r.Header.Get(peer.FromHeader); got != "gateway" {
			t.Fatalf("from header = %q, want gateway value", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll(body) error = %v", err)
		}
		if string(body) != "payload" {
			t.Fatalf("body = %q, want payload", string(body))
		}
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	}))
	defer peerServer.Close()

	router, err := peer.NewOwnerRouter([]peer.Peer{{ID: "cache-0", URL: peerServer.URL}})
	if err != nil {
		t.Fatalf("NewOwnerRouter() error = %v", err)
	}
	handler := New(Options{Router: router})

	req := httptest.NewRequest(http.MethodPut, "/photos/new.jpg", strings.NewReader("payload"))
	req.Host = "s3.example.test"
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 signed")
	req.Header.Set("X-Amz-Date", "20260617T023700Z")
	req.Header.Set("Connection", "close")
	req.Header.Set(peer.ForwardedHeader, "client-spoof")
	req.Header.Set(peer.OwnerHeader, "client-spoof")
	req.Header.Set(peer.FromHeader, "client-spoof")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
	}
	if got := rec.Header().Get("ETag"); got != `"abc"` {
		t.Fatalf("ETag = %q, want response header from peer", got)
	}
	if got := rec.Body.String(); got != "created" {
		t.Fatalf("body = %q, want peer response body", got)
	}
}

func TestGatewayRoutesBucketRequestToDefaultPeer(t *testing.T) {
	var hit bool
	cache0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cache0.Close()
	cache1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("bucket-level request reached non-default peer")
	}))
	defer cache1.Close()

	router, err := peer.NewOwnerRouter([]peer.Peer{
		{ID: "cache-1", URL: cache1.URL},
		{ID: "cache-0", URL: cache0.URL},
	})
	if err != nil {
		t.Fatalf("NewOwnerRouter() error = %v", err)
	}
	handler := New(Options{Router: router})

	req := httptest.NewRequest(http.MethodGet, "/photos?list-type=2", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if !hit {
		t.Fatal("default peer was not hit")
	}
}

func TestGatewayRejectsAmbiguousDeleteObjects(t *testing.T) {
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("ambiguous delete request should not be forwarded")
	}))
	defer peerServer.Close()

	router, err := peer.NewOwnerRouter([]peer.Peer{{ID: "cache-0", URL: peerServer.URL}})
	if err != nil {
		t.Fatalf("NewOwnerRouter() error = %v", err)
	}
	handler := New(Options{Router: router})

	req := httptest.NewRequest(http.MethodPost, "/photos?delete", strings.NewReader("<Delete/>"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}
