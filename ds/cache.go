package ds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MultiError maps errors to input elements.
type MultiError []error

func (m MultiError) Error() string {
	s, n := "", 0
	for _, e := range m {
		if e != nil {
			if n == 0 {
				s = e.Error()
			}
			n++
		}
	}
	switch n {
	case 0:
		return "(0 errors)"
	case 1:
		return s
	case 2:
		return s + " (and 1 other error)"
	}
	return fmt.Sprintf("%s (and %d other errors)", s, n-1)
}

var (
	ErrCacheMiss   = errors.New("dsorm: cache miss")
	ErrCASConflict = errors.New("dsorm: cas conflict")
	ErrNotStored   = errors.New("dsorm: not stored")
)

type contextKey string

const tenantContextKey contextKey = "dsorm_cache_tenant"

// WithTenant returns a copy of ctx carrying the given cache tenant
// identifier. Cache backends that support multi-tenancy (e.g. the Cloudflare
// Durable Object backend, where each tenant maps to its own object) route all
// operations performed with this context to that tenant's isolated namespace.
// Backends that do not support tenancy ignore it.
//
// When no tenant is set, such backends fall back to a single shared default
// tenant.
func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantContextKey, tenant)
}

// TenantFromContext returns the cache tenant carried by ctx, or "" if none.
func TenantFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(tenantContextKey).(string); ok {
		return v
	}
	return ""
}

// Cache interface for backend storage.
type Cache interface {
	// AddMulti adds items if keys are not in use. Returns ErrNotStored on conflict.
	AddMulti(ctx context.Context, items []*Item) error

	// CompareAndSwapMulti updates items if unchanged since Get. Returns ErrCASConflict or ErrNotStored.
	CompareAndSwapMulti(ctx context.Context, items []*Item) error

	// DeleteMulti removes keys.
	DeleteMulti(ctx context.Context, keys []string) error

	// GetMulti fetches keys. Returns map of found items. No error on miss.
	GetMulti(ctx context.Context, keys []string) (map[string]*Item, error)

	// SetMulti sets items blindly. Returns error per item on failure.
	SetMulti(ctx context.Context, items []*Item) error

	// Increment atomically increments a key's integer value by delta.
	// Returns the new value after incrementing. Creates the key with
	// value=delta and the given expiration if the key does not exist.
	Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error)

	// Flush removes all keys from the cache. Backends that support
	// multi-tenancy flush only the tenant carried by ctx (see WithTenant).
	Flush(ctx context.Context) error
}

// Item cache entry.
type Item struct {
	Key        string
	Value      []byte
	Flags      uint32
	Expiration time.Duration

	casInfo interface{}
	casOnce sync.Once
}

// SetCASInfo sets opaque CAS value once.
func (i *Item) SetCASInfo(value interface{}) {
	i.casOnce.Do(func() {
		i.casInfo = value
	})
}

// GetCASInfo gets opaque CAS value.
func (i *Item) GetCASInfo() interface{} {
	return i.casInfo
}
