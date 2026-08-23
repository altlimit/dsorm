package dsorm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	ds "github.com/altlimit/dsorm/ds"
)

// requestScopeMaxAge bounds how long a request scope keeps serving pinned
// generation counters. A scope is meant to live for one request; past this age
// it stops answering, so attaching one to a long-lived context degrades to
// ordinary per-query reads instead of pinning a version forever.
const requestScopeMaxAge = 30 * time.Second

var requestScopeKey contextKey = "dsorm_request_scope"

// requestScope pins each kind's generation counter for the life of a request.
type requestScope struct {
	mu       sync.RWMutex
	versions map[string]int64
	created  time.Time
}

// WithRequestScope returns a copy of ctx that reads each kind's query cache
// generation counter at most once, instead of once per query.
//
// A page that queries five kinds pays five round trips for five small integers
// without this; with it, and with [Client.PrefetchVersions], it pays one. That
// is the difference between the local query cache costing one round trip per
// kind and one per request.
//
// The trade is explicit: two queries of the same kind within one request see
// the same generation counter, so a write by another instance that lands
// between them is not observed until the next request. In exchange the request
// is a consistent snapshot — a header and a footer rendering the same kind
// cannot disagree. Writes made through this client are still observed
// immediately: every write updates the pinned counter for the kinds it
// touched, and clears it outright if the shared counter could not be bumped.
//
// Attach it once per request, typically in middleware. It is safe to call on a
// context that already carries a scope, which is then returned unchanged. A
// scope older than 30 seconds stops pinning, so a context that outlives its
// request loses the optimization rather than serving stale data indefinitely.
func WithRequestScope(ctx context.Context) context.Context {
	if requestScopeFrom(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, requestScopeKey, &requestScope{
		versions: make(map[string]int64),
		created:  time.Now(),
	})
}

// requestScopeFrom returns the live scope carried by ctx, or nil when there is
// none or it has aged out.
func requestScopeFrom(ctx context.Context) *requestScope {
	rs, _ := ctx.Value(requestScopeKey).(*requestScope)
	if rs == nil || time.Since(rs.created) > requestScopeMaxAge {
		return nil
	}
	return rs
}

func (r *requestScope) version(vKey string) (int64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.versions[vKey]
	return v, ok
}

func (r *requestScope) setVersion(vKey string, version int64) {
	r.mu.Lock()
	r.versions[vKey] = version
	r.mu.Unlock()
}

func (r *requestScope) clearVersion(vKey string) {
	r.mu.Lock()
	delete(r.versions, vKey)
	r.mu.Unlock()
}

// noteVersion records a version into the request scope, if ctx carries one.
func noteVersion(ctx context.Context, vKey string, version int64) {
	if rs := requestScopeFrom(ctx); rs != nil {
		rs.setVersion(vKey, version)
	}
}

// forgetVersion drops a pinned version, forcing the next query to re-read it.
func forgetVersion(ctx context.Context, vKey string) {
	if rs := requestScopeFrom(ctx); rs != nil {
		rs.clearVersion(vKey)
	}
}

// parseKindVersion decodes a generation counter stored either as a decimal
// string or as 8 little-endian bytes. It returns 0 when neither applies.
func parseKindVersion(b []byte) int64 {
	if v, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		return v
	}
	if len(b) == 8 {
		return int64(binary.LittleEndian.Uint64(b))
	}
	return 0
}

// kindVersion returns the generation counter for a kind, and whether it had to
// be restored because the shared cache no longer had it.
//
// A restored counter is the signal that this client does not actually know the
// current generation: it re-pins its own last known value, which is correct
// for the shared cache (whatever dropped the counter dropped the cached
// queries with it) but not for an in-process cache, whose entries survive the
// event with data intact. Callers use the flag to drop the kind locally.
func (c *Client) kindVersion(ctx context.Context, ns, kind string) (int64, bool) {
	vKey := kindVersionKey(ns, kind)
	if rs := requestScopeFrom(ctx); rs != nil {
		if v, ok := rs.version(vKey); ok {
			return v, false
		}
	}
	version, restored := c.readKindVersion(ctx, vKey)
	noteVersion(ctx, vKey, version)
	return version, restored
}

// readKindVersion reads a generation counter from the shared cache, restoring
// it when absent.
func (c *Client) readKindVersion(ctx context.Context, vKey string) (int64, bool) {
	versionItem, err := c.Cache().Get(ctx, vKey)
	if err == nil && versionItem != nil {
		version := parseKindVersion(versionItem.Value)
		if version > 0 {
			c.localVersions.Store(vKey, version)
		}
		return version, false
	}
	if err != nil && !errors.Is(err, ds.ErrCacheMiss) {
		c.onError(ctx, fmt.Errorf("dsorm: read query cache version %q: %w", vKey, err))
	}
	return c.restoreKindVersion(ctx, vKey), true
}

// restoreKindVersion re-establishes a lost generation counter. It must not
// start from 0, which would collide with generations already used, so it
// resumes from this client's last known value, or from a wall-clock stamp when
// this process has never seen the kind.
func (c *Client) restoreKindVersion(ctx context.Context, vKey string) int64 {
	var version int64
	if v, ok := c.localVersions.Load(vKey); ok {
		version = v.(int64)
	} else {
		// Initialize with UnixMilli to ensure uniqueness across app restarts
		version = time.Now().UnixMilli()
	}
	newV, err := c.Cache().Increment(ctx, vKey, version, 0)
	if err != nil {
		c.onError(ctx, fmt.Errorf("dsorm: restore query cache version %q: %w", vKey, err))
		return version
	}
	c.localVersions.Store(vKey, newV)
	return newV
}

// PrefetchVersions reads the query cache generation counters for several kinds
// in a single round trip and pins them on the request scope carried by ctx, so
// the queries that follow do not read them one at a time.
//
// Call it once per request with the kinds that request will query:
//
//	ctx = dsorm.WithRequestScope(ctx)
//	client.PrefetchVersions(ctx, tenant, "Shop", "Theme", "Plugin")
//
// A kind that was not prefetched still works: its counter is read on first use
// and pinned from then on. ns is the datastore namespace the kinds are queried
// in — pass "" for the default namespace.
//
// It requires [WithRequestScope]; without a scope there is nowhere to pin the
// result, so it does nothing and reports the misuse through the client's error
// handler rather than spending a round trip that no query would benefit from.
func (c *Client) PrefetchVersions(ctx context.Context, ns string, kinds ...string) error {
	if !c.queryCache || len(kinds) == 0 {
		return nil
	}
	ctx = c.context(ctx)
	if requestScopeFrom(ctx) == nil {
		c.onError(ctx, errors.New("dsorm: PrefetchVersions without a request scope — wrap the request context with dsorm.WithRequestScope"))
		return nil
	}
	cacher := c.client.Cacher()
	if cacher == nil {
		return nil
	}

	vKeys := make([]string, 0, len(kinds))
	byVKey := make(map[string]string, len(kinds))
	for _, kind := range kinds {
		vKey := kindVersionKey(ns, kind)
		if _, dup := byVKey[vKey]; dup {
			continue
		}
		byVKey[vKey] = kind
		vKeys = append(vKeys, vKey)
	}

	items, err := cacher.GetMulti(ctx, vKeys)
	if err != nil {
		return fmt.Errorf("dsorm: prefetch query cache versions: %w", err)
	}

	var missing []string
	for _, vKey := range vKeys {
		item, ok := items[vKey]
		if !ok || item == nil {
			missing = append(missing, vKey)
			continue
		}
		version := parseKindVersion(item.Value)
		if version <= 0 {
			missing = append(missing, vKey)
			continue
		}
		c.localVersions.Store(vKey, version)
		noteVersion(ctx, vKey, version)
	}

	// A counter the shared cache no longer has means this client cannot know
	// the current generation for that kind, so its local entries go too.
	for _, vKey := range missing {
		noteVersion(ctx, vKey, c.restoreKindVersion(ctx, vKey))
		c.localCache.dropKind(ns, byVKey[vKey])
	}
	return nil
}
