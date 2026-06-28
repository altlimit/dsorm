package cloudflare

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

// NewMockHandler returns an in-memory http.Handler implementing the same batch
// protocol as the Durable Object Worker. It is a faithful reference of the DO's
// semantics (per-tenant isolation, lazy TTL expiry, atomic add/cas/increment)
// intended for tests and local development without Cloudflare. It is not a
// production cache: state lives only in process memory.
//
// Each request's dispatch runs while holding a single mutex (see ServeHTTP),
// which mirrors the DO's per-op transactionSync: the read-modify-write in
// add/cas/increment cannot interleave with another request.
func NewMockHandler() http.Handler {
	return &mockStore{tenants: make(map[string]map[string]mockEntry)}
}

type mockEntry struct {
	blob     []byte
	expireAt time.Time // zero = no expiry
}

func (e mockEntry) expired(now time.Time) bool {
	return !e.expireAt.IsZero() && now.After(e.expireAt)
}

type mockStore struct {
	mu      sync.Mutex
	tenants map[string]map[string]mockEntry
}

func (m *mockStore) bucket(tenant string) map[string]mockEntry {
	b := m.tenants[tenant]
	if b == nil {
		b = make(map[string]mockEntry)
		m.tenants[tenant] = b
	}
	return b
}

func expiryFrom(now time.Time, ttlMs int64) time.Time {
	if ttlMs <= 0 {
		return time.Time{}
	}
	return now.Add(time.Duration(ttlMs) * time.Millisecond)
}

func (m *mockStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Path: /v1/{tenant}/{op}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "v1" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	tenant, op := parts[1], parts[2]

	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	resp, ok := m.dispatch(tenant, op, &req)
	m.mu.Unlock()
	if !ok {
		http.Error(w, "unknown op", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// dispatch executes one op. Caller holds m.mu.
func (m *mockStore) dispatch(tenant, op string, req *batchRequest) (*batchResponse, bool) {
	now := time.Now()
	bucket := m.bucket(tenant)
	resp := &batchResponse{}

	switch op {
	case opGet:
		for _, key := range req.Keys {
			e, ok := bucket[key]
			if !ok {
				continue
			}
			if e.expired(now) {
				delete(bucket, key)
				continue
			}
			resp.Items = append(resp.Items, wireItem{Key: key, Blob: e.blob})
		}

	case opSet:
		resp.Codes = make([]string, len(req.Items))
		for i, it := range req.Items {
			bucket[it.Key] = mockEntry{blob: it.Blob, expireAt: expiryFrom(now, it.TTLMs)}
			resp.Codes[i] = codeOK
		}

	case opAdd:
		resp.Codes = make([]string, len(req.Items))
		for i, it := range req.Items {
			if e, ok := bucket[it.Key]; ok && !e.expired(now) {
				resp.Codes[i] = codeNotStored
				continue
			}
			bucket[it.Key] = mockEntry{blob: it.Blob, expireAt: expiryFrom(now, it.TTLMs)}
			resp.Codes[i] = codeOK
		}

	case opCAS:
		resp.Codes = make([]string, len(req.Items))
		for i, it := range req.Items {
			e, ok := bucket[it.Key]
			if !ok || e.expired(now) {
				delete(bucket, it.Key)
				resp.Codes[i] = codeNotStored
				continue
			}
			if !bytesEqual(e.blob, it.CAS) {
				resp.Codes[i] = codeConflict
				continue
			}
			bucket[it.Key] = mockEntry{blob: it.Blob, expireAt: expiryFrom(now, it.TTLMs)}
			resp.Codes[i] = codeOK
		}

	case opDelete:
		for _, key := range req.Keys {
			e, ok := bucket[key]
			if !ok {
				continue
			}
			delete(bucket, key)
			if !e.expired(now) {
				resp.Count++
			}
		}

	case opIncrement:
		var n int64
		if e, ok := bucket[req.Key]; ok && !e.expired(now) && len(e.blob) == 8 {
			n = int64(binary.LittleEndian.Uint64(e.blob))
		}
		n += req.Delta
		blob := make([]byte, 8)
		binary.LittleEndian.PutUint64(blob, uint64(n))
		entry := mockEntry{blob: blob}
		if req.TTLMs > 0 {
			entry.expireAt = expiryFrom(now, req.TTLMs)
		} else if e, ok := bucket[req.Key]; ok {
			entry.expireAt = e.expireAt // preserve existing expiry
		}
		bucket[req.Key] = entry
		resp.Value = n

	case opFlush:
		// Drop the whole tenant bucket; a fresh one is created lazily on next use.
		delete(m.tenants, tenant)

	default:
		return nil, false
	}
	return resp, true
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
