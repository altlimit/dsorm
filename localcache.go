package dsorm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm/cache"
	ds "github.com/altlimit/dsorm/ds"
	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	// localCacheSep separates the components of a local cache entry key. NUL
	// cannot appear in a namespace, a kind, or a hex query hash, so the
	// per-kind prefix scan in dropKind can never match across components.
	localCacheSep = "\x00"

	defaultLocalCacheMaxBytes   = 32 << 20
	defaultLocalCacheMaxEntries = 4096
)

// LocalCacheConfig configures the in-process (L1) query cache enabled with
// [WithLocalQueryCache].
type LocalCacheConfig struct {
	// Kinds are the kinds to cache in-process. This is deliberately opt-in and
	// has no "all kinds" setting: entries are scoped to a kind's generation
	// counter, so any write to a kind in a namespace drops that kind's entire
	// local cache for that namespace. That is ideal for read-on-every-request,
	// write-rarely kinds and pathological for write-hot ones, where it buys a
	// near-zero hit rate in exchange for constant churn.
	Kinds []string

	// MaxBytes is the hard budget for cached entry payloads. It defaults to
	// 32MiB. The accounting is an estimate of retained size (encoded property
	// bytes plus per-key overhead), tracked incrementally on insert and
	// eviction, never by scanning the cache.
	MaxBytes int64

	// MaxEntries caps the number of cached queries and defaults to 4096. The
	// LRU needs an entry bound in addition to the byte budget; whichever limit
	// is reached first evicts.
	MaxEntries int

	// TTL bounds how long an entry may be served. It defaults to the query
	// cache TTL passed to [WithQueryCache].
	//
	// Entries are invalidated exactly, by generation counter, so this is not
	// the primary invalidation mechanism — it is a backstop for the case where
	// the shared cache loses a generation counter (eviction, restart, flush)
	// and the client restores its last known value, which would otherwise let
	// an already-resident local entry outlive the data it was built from.
	TTL time.Duration
}

// LocalKindStats reports local query cache activity for a single kind.
type LocalKindStats struct {
	// Hits are queries answered from the local cache.
	Hits int64
	// Misses are queries that had to consult the shared cache or datastore.
	Misses int64
	// Evictions are entries dropped to stay within MaxBytes or MaxEntries. A
	// sustained eviction rate means the kind is competing for the budget and
	// its hit rate will suffer.
	Evictions int64
	// Drops are entries discarded because they became invalid: a write to the
	// kind, a lost generation counter, an expired TTL, or a cache flush.
	Drops int64
}

// LocalCacheStats reports the state of the local query cache.
type LocalCacheStats struct {
	// Bytes is the estimated payload size currently held.
	Bytes int64
	// Entries is the number of cached queries currently held.
	Entries int
	// Kinds holds per-kind counters, keyed by kind name. Only kinds enabled in
	// [LocalCacheConfig] appear.
	Kinds map[string]LocalKindStats
}

// localEntry is one cached query result: the key set, the trailing cursor, and
// each entity in its cached wire form.
type localEntry struct {
	keys []*datastore.Key
	// props holds the gob-encoded property list per key, positionally aligned
	// with keys. A nil element is an entity that did not exist, matching the
	// nil that GetMulti leaves in the slice. A nil props slice means the entry
	// carries keys only and cannot answer a hydrating query.
	props [][]byte
	next  string
	kind  string
	bytes int64

	expiresAt time.Time
	// dropped marks an entry removed because it became invalid rather than to
	// reclaim space, so the eviction callback can tell the two apart.
	dropped atomic.Bool
}

// localKindCounters are the per-kind counters behind [LocalKindStats].
type localKindCounters struct {
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	drops     atomic.Int64
}

// localCache is the in-process query cache. Entries are keyed by namespace,
// kind, the kind's shared generation counter, a local generation counter, and
// the query hash, so a write anywhere in the fleet makes every instance
// compute a key it does not have.
type localCache struct {
	kinds      map[string]bool
	maxBytes   int64
	ttl        time.Duration
	entryLimit int

	lru   *lru.Cache[string, *localEntry]
	bytes atomic.Int64

	// mu serializes mutation of the cache. Lookups do not take it; the LRU has
	// its own lock. The eviction callback must never take it: it runs inside
	// Add and RemoveOldest.
	mu sync.Mutex

	// genMu guards gens, the per-(namespace, kind) local generation counter
	// bumped on every local write regardless of whether the shared counter
	// could be incremented.
	genMu sync.RWMutex
	gens  map[string]int64

	counters map[string]*localKindCounters
	flight   flightGroup
}

func newLocalCache(cfg LocalCacheConfig, defaultTTL time.Duration) (*localCache, error) {
	l := &localCache{
		kinds:      make(map[string]bool, len(cfg.Kinds)),
		maxBytes:   cfg.MaxBytes,
		ttl:        cfg.TTL,
		entryLimit: cfg.MaxEntries,
		gens:       make(map[string]int64),
		counters:   make(map[string]*localKindCounters, len(cfg.Kinds)),
	}
	if l.maxBytes <= 0 {
		l.maxBytes = defaultLocalCacheMaxBytes
	}
	if l.entryLimit <= 0 {
		l.entryLimit = defaultLocalCacheMaxEntries
	}
	if l.ttl <= 0 {
		l.ttl = defaultTTL
	}
	for _, k := range cfg.Kinds {
		if k == "" {
			continue
		}
		l.kinds[k] = true
		l.counters[k] = &localKindCounters{}
	}
	if len(l.kinds) == 0 {
		return nil, fmt.Errorf("dsorm: local query cache needs at least one kind")
	}
	c, err := lru.NewWithEvict[string, *localEntry](l.entryLimit, l.onEvicted)
	if err != nil {
		return nil, err
	}
	l.lru = c
	return l, nil
}

// onEvicted returns an entry's bytes to the budget. It runs outside the LRU's
// lock on every removal path, so it must stay allocation-light and lock-free.
func (l *localCache) onEvicted(key string, e *localEntry) {
	l.bytes.Add(-e.bytes)
	if c := l.counters[e.kind]; c != nil {
		if e.dropped.Load() {
			c.drops.Add(1)
		} else {
			c.evictions.Add(1)
			_ = ds.RecordLocalCacheStats(context.Background(), e.kind, 0, 0, 1)
		}
	}
}

func (l *localCache) enabled(kind string) bool { return l != nil && l.kinds[kind] }

// entryKey builds the cache key for one query result.
func (l *localCache) entryKey(ns, kind string, version, gen int64, hash string) string {
	return strings.Join([]string{ns, kind, fmt.Sprint(version), fmt.Sprint(gen), hash}, localCacheSep)
}

// kindPrefix is the key prefix shared by every entry of a kind in a namespace.
func (l *localCache) kindPrefix(ns, kind string) string {
	return ns + localCacheSep + kind + localCacheSep
}

func (l *localCache) genKey(ns, kind string) string { return ns + localCacheSep + kind }

// generation returns the local generation counter for a kind.
func (l *localCache) generation(ns, kind string) int64 {
	l.genMu.RLock()
	defer l.genMu.RUnlock()
	return l.gens[l.genKey(ns, kind)]
}

// bumpGeneration advances the local generation counter for a kind and drops
// the kind's entries. It is called on every local write to the kind, whether
// or not the shared generation counter could be incremented, so an instance
// always reads its own writes even when the shared cache is unreachable.
func (l *localCache) bumpGeneration(ns, kind string) {
	l.genMu.Lock()
	l.gens[l.genKey(ns, kind)]++
	l.genMu.Unlock()
	l.dropKind(ns, kind)
}

func (l *localCache) get(ctx context.Context, key, kind string) *localEntry {
	e, ok := l.lru.Get(key)
	if !ok {
		l.recordMiss(ctx, kind)
		return nil
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		l.dropKey(key)
		l.recordMiss(ctx, kind)
		return nil
	}
	if c := l.counters[kind]; c != nil {
		c.hits.Add(1)
	}
	_ = ds.RecordLocalCacheStats(ctx, kind, 1, 0, 0)
	return e
}

func (l *localCache) recordMiss(ctx context.Context, kind string) {
	if c := l.counters[kind]; c != nil {
		c.misses.Add(1)
	}
	_ = ds.RecordLocalCacheStats(ctx, kind, 0, 1, 0)
}

// add stores an entry, then evicts oldest-first until the byte budget is met.
// Both sides of the accounting are incremental: the insert adds, the eviction
// callback subtracts. Nothing here scans the cache.
func (l *localCache) add(key string, e *localEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if old, ok := l.lru.Peek(key); ok {
		// Overwriting a key does not fire the eviction callback.
		l.bytes.Add(-old.bytes)
	}
	l.bytes.Add(e.bytes)
	l.lru.Add(key, e)
	for l.bytes.Load() > l.maxBytes {
		if _, _, ok := l.lru.RemoveOldest(); !ok {
			break
		}
	}
}

func (l *localCache) dropKey(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.lru.Peek(key); ok {
		e.dropped.Store(true)
	}
	l.lru.Remove(key)
}

// dropKind removes every entry for a kind in a namespace. It scans the key
// set, which is O(entries) — acceptable because it runs on writes and on lost
// generation counters, never on the read path.
func (l *localCache) dropKind(ns, kind string) {
	if l == nil {
		return
	}
	prefix := l.kindPrefix(ns, kind)
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range l.lru.Keys() {
		if strings.HasPrefix(k, prefix) {
			if e, ok := l.lru.Peek(k); ok {
				e.dropped.Store(true)
			}
			l.lru.Remove(k)
		}
	}
}

// dropAll empties the cache. It is called when the shared cache is flushed,
// which would otherwise leave this instance serving pre-flush data under a
// generation counter it is about to restore to its pre-flush value.
func (l *localCache) dropAll() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range l.lru.Keys() {
		if e, ok := l.lru.Peek(k); ok {
			e.dropped.Store(true)
		}
		l.lru.Remove(k)
	}
}

func (l *localCache) stats() LocalCacheStats {
	s := LocalCacheStats{
		Bytes:   l.bytes.Load(),
		Entries: l.lru.Len(),
		Kinds:   make(map[string]LocalKindStats, len(l.counters)),
	}
	for kind, c := range l.counters {
		s.Kinds[kind] = LocalKindStats{
			Hits:      c.hits.Load(),
			Misses:    c.misses.Load(),
			Evictions: c.evictions.Load(),
			Drops:     c.drops.Load(),
		}
	}
	return s
}

// newEntry captures a query result in cacheable form. The property lists come
// straight off the loaded entities: loadModel stores the properties it was
// handed as the entity's change-tracking baseline, so they are exactly the
// wire form the entity was built from, before decryption and before any
// marshaled field was decoded.
func (l *localCache) newEntry(key, kind string, keys []*datastore.Key, next string, slice reflect.Value) (*localEntry, error) {
	props := make([][]byte, len(keys))
	for i := range keys {
		if i >= slice.Len() {
			break
		}
		elem := slice.Index(i)
		if elem.Kind() == reflect.Ptr && elem.IsNil() {
			// Entity did not exist; GetMulti leaves nil here.
			continue
		}
		m, ok := elem.Interface().(modeler)
		if !ok {
			return nil, fmt.Errorf("dsorm: local query cache: %T does not embed dsorm.Base", elem.Interface())
		}
		b, err := ds.MarshalPropertyList(datastore.PropertyList(m.getInitProps()))
		if err != nil {
			return nil, fmt.Errorf("dsorm: local query cache encode %q: %w", kind, err)
		}
		props[i] = b
	}
	e := &localEntry{keys: keys, props: props, next: next, kind: kind}
	e.bytes = localEntryBytes(key, keys, next, props)
	if l.ttl > 0 {
		e.expiresAt = time.Now().Add(l.ttl)
	}
	return e, nil
}

// localEntryBytes estimates an entry's retained size. Keys are measured rather
// than encoded so the estimate costs O(1) per key.
func localEntryBytes(key string, keys []*datastore.Key, next string, props [][]byte) int64 {
	b := int64(len(key) + len(next) + 96)
	for _, k := range keys {
		b += localKeyBytes(k)
	}
	for _, p := range props {
		b += int64(len(p)) + 8
	}
	return b
}

func localKeyBytes(k *datastore.Key) int64 {
	var n int64
	for ; k != nil; k = k.Parent {
		n += int64(len(k.Kind)+len(k.Name)+len(k.Namespace)) + 56
	}
	return n
}

// flightGroup collapses concurrent identical misses into a single execution.
// An in-process cache is worthless against a cold burst without it: N
// concurrent requests would all miss and all issue the same query.
type flightGroup struct {
	mu    sync.Mutex
	calls map[string]*flightCall
}

type flightCall struct {
	wg  sync.WaitGroup
	e   *localEntry
	err error
}

// Do runs fn unless an identical call is already in flight, in which case it
// waits and reports shared=true. Nothing is memoized beyond the call itself,
// so a failed fill is never cached.
func (g *flightGroup) Do(key string, fn func() (*localEntry, error)) (e *localEntry, shared bool, err error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*flightCall)
	}
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.e, true, c.err
	}
	c := &flightCall{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	// Deferred so a panic in fn releases the waiters instead of stranding them.
	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		c.wg.Done()
	}()

	c.e, c.err = fn()
	return c.e, false, c.err
}

// localFlushCache clears the local cache when the shared cache is flushed.
// Flushing drops the generation counters, and the client restores them from
// its own last known values (see readKindVersion), so without this hook a
// flush would leave every local entry both resident and reachable.
type localFlushCache struct {
	cache.Cache
	lc *localCache
}

func (f *localFlushCache) Flush(ctx context.Context) error {
	err := f.Cache.Flush(ctx)
	f.lc.dropAll()
	return err
}

// WithLocalQueryCache enables an in-process (L1) cache of query results for
// the kinds named in cfg. It requires [WithQueryCache]: entries are keyed by
// the kind's generation counter, and without that counter there is nothing to
// invalidate them.
//
// A cached query still reads its kind's generation counter from the shared
// cache on every call — that read is what makes the cache exact — but a hit
// then skips both the shared query cache lookup and the entity fetch. The cost
// of a repeated query drops from three round trips to one; use
// [Client.PrefetchVersions] with [WithRequestScope] to make it one per request
// rather than one per kind.
//
// Entities are held in their cached wire form and a fresh struct is built for
// each call, so a result served from the local cache is indistinguishable from
// a freshly loaded one: correct key, correct change-tracking baseline, correct
// request context, IsNew reporting false, and OnLoad invoked. Results are
// never shared between callers.
//
// Queries run with vals set to nil populate nothing, since there are no
// entities to capture; they can still be answered from an entry another call
// populated.
func WithLocalQueryCache(cfg LocalCacheConfig) Option {
	return func(o *options) {
		o.localCache = &cfg
	}
}

// LocalCacheStats returns local query cache counters, or the zero value when
// [WithLocalQueryCache] is not in use. Counters are cumulative for the life of
// the process.
//
// Evictions per kind are the number worth watching: a kind with a high
// eviction count is being pushed out of the byte budget by other kinds or
// other namespaces, and its hit rate will be poor no matter how rarely it is
// written.
func (c *Client) LocalCacheStats() LocalCacheStats {
	if c.localCache == nil {
		return LocalCacheStats{}
	}
	return c.localCache.stats()
}
