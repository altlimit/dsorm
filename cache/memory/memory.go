// Package memory provides an in-memory cache implementation backed by an
// LRU eviction policy with per-item TTL support.
//
// This is suitable for single-instance applications and testing.
// For distributed systems, use the redis cache backend instead.
package memory

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"runtime"
	"sync"
	"time"
	"unsafe"

	lru "github.com/hashicorp/golang-lru/v2"

	dsorm "github.com/altlimit/dsorm/ds"
)

// Option configures a memory cache.
type Option func(*memory)

// WithMaxEntries overrides the auto-detected maximum number of cache entries.
func WithMaxEntries(n int) Option {
	return func(m *memory) {
		m.maxEntries = n
	}
}

// NewCache initializes a new in-memory LRU cache.
// By default it auto-detects available system memory and sizes the cache
// accordingly. Use WithMaxEntries to override.
func NewCache(opts ...Option) dsorm.Cache {
	m := &memory{}
	for _, opt := range opts {
		opt(m)
	}
	if m.maxEntries <= 0 {
		m.maxEntries = autoDetectMaxEntries()
	}
	m.store, _ = lru.New[string, *object](m.maxEntries)
	return m
}

// autoDetectMaxEntries picks a reasonable cache size based on system memory.
// Uses ~1% of available RAM assuming ~1KB per entry, clamped to [1000, 500000].
func autoDetectMaxEntries() int {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	// Use Sys (total memory obtained from OS) as a rough upper bound
	totalBytes := ms.Sys
	if totalBytes == 0 {
		return 10000 // fallback default
	}
	// Assume ~1KB per cache entry, use 1% of memory
	estimatedEntrySize := uint64(unsafe.Sizeof(object{})) + 256 // key + overhead estimate
	if estimatedEntrySize < 512 {
		estimatedEntrySize = 512
	}
	entries := int(totalBytes / 100 / estimatedEntrySize)
	if entries < 10000 {
		entries = 10000
	}
	if entries > 500000 {
		entries = 500000
	}
	return entries
}

type object struct {
	flags     uint32
	value     []byte
	expiresAt time.Time // zero means no expiration
}

func (o *object) expired() bool {
	return !o.expiresAt.IsZero() && time.Now().After(o.expiresAt)
}

type memory struct {
	store      *lru.Cache[string, *object]
	maxEntries int
	mu         sync.Mutex // protects CAS and Increment operations
}

func expiresAt(d time.Duration) time.Time {
	if d == 0 {
		return time.Time{}
	}
	return time.Now().Add(d)
}

func (m *memory) AddMulti(ctx context.Context, items []*dsorm.Item) error {
	me := make(dsorm.MultiError, len(items))
	hasErr := false
	for i, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, ok := m.getValid(item.Key); ok {
			me[i] = dsorm.ErrNotStored
			hasErr = true
		} else {
			m.store.Add(item.Key, &object{
				flags:     item.Flags,
				value:     append([]byte(nil), item.Value...),
				expiresAt: expiresAt(item.Expiration),
			})
		}
	}
	if hasErr {
		return me
	}
	return nil
}

func (m *memory) CompareAndSwapMulti(ctx context.Context, items []*dsorm.Item) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	me := make(dsorm.MultiError, len(items))
	hasErr := false
	for i, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if obj, ok := m.getValid(item.Key); ok {
			dsoItem := &dsorm.Item{
				Flags: obj.flags,
				Value: append([]byte(nil), obj.value...),
			}
			hasher := sha1.New()
			_ = binary.Write(hasher, binary.LittleEndian, dsoItem.Flags)
			_, _ = hasher.Write(dsoItem.Value)
			if casBytes, casOk := item.GetCASInfo().([]byte); casOk && equal(casBytes, hasher.Sum(nil)) {
				m.store.Add(item.Key, &object{
					flags:     item.Flags,
					value:     append([]byte(nil), item.Value...),
					expiresAt: expiresAt(item.Expiration),
				})
			} else {
				hasErr = true
				me[i] = dsorm.ErrCASConflict
			}
		} else {
			hasErr = true
			me[i] = dsorm.ErrNotStored
		}
	}
	if hasErr {
		return me
	}
	return nil
}

func (m *memory) DeleteMulti(ctx context.Context, keys []string) error {
	me := make(dsorm.MultiError, len(keys))
	hasErr := false
	for i, key := range keys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, ok := m.getValid(key); !ok {
			me[i] = dsorm.ErrCacheMiss
			hasErr = true
		}
		m.store.Remove(key)
	}
	if hasErr {
		return me
	}
	return nil
}

func (m *memory) GetMulti(ctx context.Context, keys []string) (map[string]*dsorm.Item, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	result := make(map[string]*dsorm.Item)
	for _, key := range keys {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if obj, ok := m.getValid(key); ok {
			dsoItem := &dsorm.Item{
				Key:   key,
				Flags: obj.flags,
				Value: append([]byte(nil), obj.value...),
			}
			hasher := sha1.New()
			_ = binary.Write(hasher, binary.LittleEndian, dsoItem.Flags)
			_, _ = hasher.Write(dsoItem.Value)
			dsoItem.SetCASInfo(hasher.Sum(nil))
			result[key] = dsoItem
		}
	}
	return result, nil
}

func (m *memory) SetMulti(ctx context.Context, items []*dsorm.Item) error {
	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		m.store.Add(item.Key, &object{
			flags:     item.Flags,
			value:     append([]byte(nil), item.Value...),
			expiresAt: expiresAt(item.Expiration),
		})
	}
	return nil
}

func (m *memory) Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var current int64
	if obj, ok := m.getValid(key); ok {
		// Parse existing value as int64 (stored as little-endian 8 bytes)
		if len(obj.value) == 8 {
			current = int64(binary.LittleEndian.Uint64(obj.value))
		}
	}

	newVal := current + delta
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(newVal))
	m.store.Add(key, &object{
		value:     buf,
		expiresAt: expiresAt(expiration),
	})
	return newVal, nil
}

// getValid returns the object for key if it exists and is not expired.
// Expired entries are cleaned up on access.
func (m *memory) getValid(key string) (*object, bool) {
	obj, ok := m.store.Get(key)
	if !ok {
		return nil, false
	}
	if obj.expired() {
		m.store.Remove(key)
		return nil, false
	}
	return obj, true
}

func equal(a, b []byte) bool {
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
