package dsorm_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/altlimit/dsorm"
	"github.com/altlimit/dsorm/cache/memory"
	"github.com/altlimit/dsorm/ds"
	"github.com/altlimit/dsorm/ds/local"
)

// countingCache records every GetMulti key set so tests can count round trips
// per cache-key prefix, and can gate query cache reads to make a cold burst
// deterministic.
type countingCache struct {
	ds.Cache

	mu    sync.Mutex
	calls [][]string

	gate chan struct{}
}

func (c *countingCache) GetMulti(ctx context.Context, keys []string) (map[string]*ds.Item, error) {
	c.mu.Lock()
	c.calls = append(c.calls, append([]string(nil), keys...))
	gate := c.gate
	c.mu.Unlock()

	if gate != nil && hasPrefix(keys, "qcache:") {
		<-gate
	}
	return c.Cache.GetMulti(ctx, keys)
}

func hasPrefix(keys []string, prefix string) bool {
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

// batches returns the GetMulti calls that asked for at least one key with the
// given prefix.
func (c *countingCache) batches(prefix string) [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out [][]string
	for _, call := range c.calls {
		if hasPrefix(call, prefix) {
			out = append(out, call)
		}
	}
	return out
}

func (c *countingCache) reset() {
	c.mu.Lock()
	c.calls = nil
	c.mu.Unlock()
}

type LCModel struct {
	dsorm.Base
	ID    string `model:"id"`
	Value string `datastore:"value"`
}

type LCOtherModel struct {
	dsorm.Base
	ID    string `model:"id"`
	Value string `datastore:"value"`
}

// lcHookSeen records what BeforeSave observed as the previous state.
var lcHookSeen atomic.Value

type LCHookModel struct {
	dsorm.Base
	ID     string `model:"id"`
	Value  string `datastore:"value"`
	loaded bool
}

func (m *LCHookModel) BeforeSave(ctx context.Context, old dsorm.Model) error {
	o, _ := old.(*LCHookModel)
	if o == nil {
		lcHookSeen.Store("<nil>")
		return nil
	}
	lcHookSeen.Store(o.Value)
	return nil
}

func (m *LCHookModel) OnLoad(ctx context.Context) error {
	m.loaded = true
	return nil
}

// newLocalCacheClient builds a client with the query cache and an in-process
// cache over a throwaway local store.
func newLocalCacheClient(t *testing.T, cfg dsorm.LocalCacheConfig, opts ...dsorm.Option) (*dsorm.Client, *countingCache, string) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "dsorm_l1_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tempDir) })

	store := local.NewStore(tempDir)
	t.Cleanup(func() { store.Close() })

	counting := &countingCache{Cache: memory.NewCache()}
	opts = append([]dsorm.Option{
		dsorm.WithStore(store),
		dsorm.WithCache(counting),
		dsorm.WithQueryCache(time.Minute),
		dsorm.WithLocalQueryCache(cfg),
	}, opts...)

	client, err := dsorm.New(context.Background(), opts...)
	if err != nil {
		t.Fatalf("Failed to init client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client, counting, tempDir
}

// bypassClient returns a client over the same store that neither reads nor
// invalidates the shared cache, for simulating another writer.
func bypassClient(t *testing.T, tempDir string) *dsorm.Client {
	t.Helper()
	store := local.NewStore(tempDir)
	t.Cleanup(func() { store.Close() })
	client, err := dsorm.New(context.Background(), dsorm.WithStore(store), dsorm.WithNoCache())
	if err != nil {
		t.Fatalf("Failed to init bypass client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// TestLocalQueryCacheSkipsSharedHops verifies the point of the cache: a repeat
// query is answered in-process, skipping the shared query cache lookup and the
// entity fetch, while still reading the kind's generation counter every time —
// that read is what keeps invalidation exact.
func TestLocalQueryCacheSkipsSharedHops(t *testing.T) {
	ctx := context.Background()
	db, counting, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}})

	if err := db.PutMulti(ctx, []*LCModel{{ID: "m1", Value: "v1"}, {ID: "m2", Value: "v2"}}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	q := dsorm.NewQuery("LCModel").Order("value")
	first, _, err := dsorm.Query[*LCModel](ctx, db, q, "")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(first))
	}

	counting.reset()

	second, _, err := dsorm.Query[*LCModel](ctx, db, q, "")
	if err != nil {
		t.Fatalf("Query2 failed: %v", err)
	}
	if len(second) != 2 || second[0].ID != "m1" || second[1].ID != "m2" {
		t.Fatalf("Expected [m1 m2] from the local cache, got %+v", second)
	}

	if got := len(counting.batches("qcache:")); got != 0 {
		t.Errorf("Expected 0 shared query cache reads on a local hit, got %d", got)
	}
	if got := len(counting.batches("kind_v:")); got != 1 {
		t.Errorf("Expected exactly 1 generation counter read, got %d", got)
	}

	stats := db.LocalCacheStats()
	if stats.Kinds["LCModel"].Hits != 1 {
		t.Errorf("Expected 1 local hit, got %d", stats.Kinds["LCModel"].Hits)
	}
	if stats.Kinds["LCModel"].Misses != 1 {
		t.Errorf("Expected 1 local miss, got %d", stats.Kinds["LCModel"].Misses)
	}
	if stats.Entries != 1 || stats.Bytes <= 0 {
		t.Errorf("Expected 1 entry with non-zero bytes, got %d entries / %d bytes", stats.Entries, stats.Bytes)
	}
}

// TestLocalQueryCacheOptInPerKind verifies a kind that was not opted in is
// never cached in-process.
func TestLocalQueryCacheOptInPerKind(t *testing.T) {
	ctx := context.Background()
	db, _, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}})

	if err := db.Put(ctx, &LCOtherModel{ID: "o1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	q := dsorm.NewQuery("LCOtherModel")
	for i := 0; i < 3; i++ {
		if _, _, err := dsorm.Query[*LCOtherModel](ctx, db, q, ""); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
	}

	stats := db.LocalCacheStats()
	if _, ok := stats.Kinds["LCOtherModel"]; ok {
		t.Errorf("Expected no counters for a kind that was not opted in, got %+v", stats.Kinds)
	}
	if stats.Entries != 0 {
		t.Errorf("Expected 0 cached entries, got %d", stats.Entries)
	}
}

// TestLocalQueryCacheInvalidatesOnWrite verifies a write through the client
// makes the next query miss the local cache.
func TestLocalQueryCacheInvalidatesOnWrite(t *testing.T) {
	ctx := context.Background()
	db, _, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}})

	if err := db.Put(ctx, &LCModel{ID: "w1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	q := dsorm.NewQuery("LCModel").Order("value")
	if _, _, err := dsorm.Query[*LCModel](ctx, db, q, ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if err := db.Put(ctx, &LCModel{ID: "w2", Value: "v2"}); err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	got, _, err := dsorm.Query[*LCModel](ctx, db, q, "")
	if err != nil {
		t.Fatalf("Query2 failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Expected 2 results after a write, got %d", len(got))
	}
	if hits := db.LocalCacheStats().Kinds["LCModel"].Hits; hits != 0 {
		t.Errorf("Expected no local hit after a write, got %d", hits)
	}
}

// TestLocalQueryCacheRehydratedEntityIsLoadable is the requirement that makes
// this belong in dsorm rather than in a caller: an entity served from the
// local cache must be indistinguishable from a freshly loaded one, so that
// mutating and Putting it diffs against the right baseline.
func TestLocalQueryCacheRehydratedEntityIsLoadable(t *testing.T) {
	ctx := context.Background()
	db, _, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCHookModel"}})

	if err := db.Put(ctx, &LCHookModel{ID: "h1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	q := dsorm.NewQuery("LCHookModel")
	// Fills the local cache.
	if _, _, err := dsorm.Query[*LCHookModel](ctx, db, q, ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	// Served from the local cache.
	got, _, err := dsorm.Query[*LCHookModel](ctx, db, q, "")
	if err != nil {
		t.Fatalf("Query2 failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(got))
	}
	if db.LocalCacheStats().Kinds["LCHookModel"].Hits != 1 {
		t.Fatal("Expected the second query to be served locally")
	}

	m := got[0]
	if m.IsNew() {
		t.Error("Expected IsNew() == false on a rehydrated entity")
	}
	if m.Key() == nil || m.Key().Name != "h1" {
		t.Errorf("Expected key h1, got %v", m.Key())
	}
	if m.Value != "v1" {
		t.Errorf("Expected Value v1, got %q", m.Value)
	}
	if !m.loaded {
		t.Error("Expected OnLoad to run on a rehydrated entity")
	}

	// Mutate and save: BeforeSave must see the pre-mutation state, which is
	// only possible if the rehydrated entity carries the right initProps.
	lcHookSeen.Store("")
	m.Value = "v2"
	if err := db.Put(ctx, m); err != nil {
		t.Fatalf("Put of rehydrated entity failed: %v", err)
	}
	if seen, _ := lcHookSeen.Load().(string); seen != "v1" {
		t.Errorf("Expected BeforeSave to see old value %q, got %q", "v1", seen)
	}

	// And the write actually landed.
	reload := &LCHookModel{ID: "h1"}
	if err := db.Get(ctx, reload); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if reload.Value != "v2" {
		t.Errorf("Expected persisted value v2, got %q", reload.Value)
	}
}

// TestLocalQueryCacheFlushClearsEntries verifies that flushing the shared cache
// also clears the local one. A flush drops the generation counters, and the
// client restores them from its own last known values, so without this the
// local cache would keep serving pre-flush data under a counter it just
// re-pinned to its pre-flush value.
func TestLocalQueryCacheFlushClearsEntries(t *testing.T) {
	ctx := context.Background()
	db, _, tempDir := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}})

	if err := db.PutMulti(ctx, []*LCModel{{ID: "f1", Value: "v1"}, {ID: "f2", Value: "v2"}}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	q := dsorm.NewQuery("LCModel").Order("value")
	if _, _, err := dsorm.Query[*LCModel](ctx, db, q, ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Another writer removes an entity without invalidating anything.
	bypassClient(t, tempDir).Delete(ctx, &LCModel{ID: "f2"})

	if err := db.Cache().Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	got, _, err := dsorm.Query[*LCModel](ctx, db, q, "")
	if err != nil {
		t.Fatalf("Query2 failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Expected 1 result after a flush, got %d — the local cache survived the flush", len(got))
	}
}

// TestLocalQueryCacheDropsOnLostGeneration covers the shared cache losing its
// generation counters — a restart, a failover, an eviction. The client
// restores its own last known counter, which is the right call for the shared
// cache but would leave in-process entries reachable with data that outlived
// them.
func TestLocalQueryCacheDropsOnLostGeneration(t *testing.T) {
	ctx := context.Background()
	db, counting, tempDir := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}})

	if err := db.PutMulti(ctx, []*LCModel{{ID: "g1", Value: "v1"}, {ID: "g2", Value: "v2"}}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	q := dsorm.NewQuery("LCModel").Order("value")
	if _, _, err := dsorm.Query[*LCModel](ctx, db, q, ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	bypassClient(t, tempDir).Delete(ctx, &LCModel{ID: "g2"})

	// The shared cache loses everything, without going through the client.
	if err := counting.Cache.Flush(ctx); err != nil {
		t.Fatalf("backend flush failed: %v", err)
	}

	got, _, err := dsorm.Query[*LCModel](ctx, db, q, "")
	if err != nil {
		t.Fatalf("Query2 failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Expected 1 result after the shared cache was lost, got %d — a stale local entry was served", len(got))
	}
	if drops := db.LocalCacheStats().Kinds["LCModel"].Drops; drops == 0 {
		t.Error("Expected the lost generation counter to be reported as a drop")
	}
}

// TestLocalQueryCacheSingleflight verifies a cold burst of identical queries
// collapses into one, and that every caller still gets its own entities.
func TestLocalQueryCacheSingleflight(t *testing.T) {
	ctx := context.Background()
	db, counting, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}})

	if err := db.PutMulti(ctx, []*LCModel{{ID: "s1", Value: "v1"}, {ID: "s2", Value: "v2"}}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	gate := make(chan struct{})
	counting.mu.Lock()
	counting.gate = gate
	counting.mu.Unlock()
	counting.reset()

	const workers = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make([]*LCModel, 0, workers)
		errs    = make([]error, workers)
	)
	q := dsorm.NewQuery("LCModel").Order("value")
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, _, err := dsorm.Query[*LCModel](ctx, db, q, "")
			if err != nil {
				errs[i] = err
				return
			}
			mu.Lock()
			if len(got) > 0 {
				results = append(results, got[0])
			}
			mu.Unlock()
		}(i)
	}

	// Every worker gets far enough to join the in-flight call before the one
	// doing the work is allowed to proceed.
	time.Sleep(150 * time.Millisecond)
	close(gate)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d failed: %v", i, err)
		}
	}
	if got := len(counting.batches("qcache:")); got != 1 {
		t.Errorf("Expected %d concurrent misses to collapse into 1 query, got %d", workers, got)
	}
	if len(results) != workers {
		t.Fatalf("Expected %d results, got %d", workers, len(results))
	}
	seen := make(map[*LCModel]bool, workers)
	for _, m := range results {
		if seen[m] {
			t.Fatal("Two callers were handed the same entity pointer")
		}
		seen[m] = true
		if m.ID != "s1" || m.Value != "v1" {
			t.Errorf("Unexpected entity %+v", m)
		}
	}
}

// TestLocalQueryCacheByteBudget verifies the byte budget is enforced and that
// the accounting is incremental and balanced: it must return to zero when the
// cache is emptied, which it cannot do if inserts and evictions disagree.
func TestLocalQueryCacheByteBudget(t *testing.T) {
	ctx := context.Background()
	const maxBytes = 2048
	db, _, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{
		Kinds:      []string{"LCModel"},
		MaxBytes:   maxBytes,
		MaxEntries: 500,
	})

	if err := db.Put(ctx, &LCModel{ID: "b1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Distinct queries, so each one is its own entry.
	for i := 0; i < 40; i++ {
		q := dsorm.NewQuery("LCModel").FilterField("value", "=", fmt.Sprintf("v%d", i))
		if _, _, err := dsorm.Query[*LCModel](ctx, db, q, ""); err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
		if b := db.LocalCacheStats().Bytes; b > maxBytes {
			t.Fatalf("Byte budget exceeded after %d queries: %d > %d", i+1, b, maxBytes)
		}
	}

	stats := db.LocalCacheStats()
	if stats.Kinds["LCModel"].Evictions == 0 {
		t.Error("Expected evictions once the byte budget was reached")
	}
	if stats.Entries == 0 {
		t.Error("Expected the cache to still hold entries")
	}

	if err := db.Cache().Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if stats := db.LocalCacheStats(); stats.Bytes != 0 || stats.Entries != 0 {
		t.Errorf("Expected an emptied cache to account for 0 bytes, got %d bytes / %d entries", stats.Bytes, stats.Entries)
	}
}

// TestPrefetchVersionsBatchesGenerationReads verifies that a request scope plus
// a prefetch turns one generation read per kind per query into one batched read
// per request.
func TestPrefetchVersionsBatchesGenerationReads(t *testing.T) {
	ctx := context.Background()
	db, counting, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{
		Kinds: []string{"LCModel", "LCOtherModel", "LCHookModel"},
	})

	if err := db.Put(ctx, &LCModel{ID: "p1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(ctx, &LCOtherModel{ID: "p2", Value: "v2"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(ctx, &LCHookModel{ID: "p3", Value: "v3"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	rctx := dsorm.WithRequestScope(ctx)
	counting.reset()
	if err := db.PrefetchVersions(rctx, "", "LCModel", "LCOtherModel", "LCHookModel"); err != nil {
		t.Fatalf("PrefetchVersions failed: %v", err)
	}

	batches := counting.batches("kind_v:")
	if len(batches) != 1 {
		t.Fatalf("Expected 1 batched generation read, got %d", len(batches))
	}
	if len(batches[0]) != 3 {
		t.Errorf("Expected 3 keys in the batch, got %v", batches[0])
	}

	// Every query in this request now runs without reading a counter again.
	for i := 0; i < 2; i++ {
		if _, _, err := dsorm.Query[*LCModel](rctx, db, dsorm.NewQuery("LCModel"), ""); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if _, _, err := dsorm.Query[*LCOtherModel](rctx, db, dsorm.NewQuery("LCOtherModel"), ""); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if _, _, err := dsorm.Query[*LCHookModel](rctx, db, dsorm.NewQuery("LCHookModel"), ""); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
	}
	if got := len(counting.batches("kind_v:")); got != 1 {
		t.Errorf("Expected no further generation reads within the request, got %d total", got)
	}

	// A fresh request reads them again.
	if _, _, err := dsorm.Query[*LCModel](ctx, db, dsorm.NewQuery("LCModel"), ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if got := len(counting.batches("kind_v:")); got != 2 {
		t.Errorf("Expected a new request to read the counter again, got %d total", got)
	}
}

// TestRequestScopeReadYourWrites verifies that pinning a generation counter for
// the life of a request does not hide the request's own writes.
func TestRequestScopeReadYourWrites(t *testing.T) {
	ctx := context.Background()
	db, _, _ := newLocalCacheClient(t, dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}})

	if err := db.Put(ctx, &LCModel{ID: "r1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	rctx := dsorm.WithRequestScope(ctx)
	q := dsorm.NewQuery("LCModel").Order("value")
	if _, _, err := dsorm.Query[*LCModel](rctx, db, q, ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if err := db.Put(rctx, &LCModel{ID: "r2", Value: "v2"}); err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	got, _, err := dsorm.Query[*LCModel](rctx, db, q, "")
	if err != nil {
		t.Fatalf("Query2 failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Expected a request to see its own write, got %d results", len(got))
	}
}

// TestLocalQueryCacheYieldsWhenInvalidationFails covers a write whose
// generation bump could not be applied at all. The local cache cannot repair
// that — the shared query cache is keyed by the same counter and still holds
// the pre-write result, which is why a failed invalidation is reported as an
// *InvalidateError for the caller to retry. What the local cache must
// guarantee is that it never serves data older than the shared layer would:
// the write drops it regardless of whether the counter could be bumped.
func TestLocalQueryCacheYieldsWhenInvalidationFails(t *testing.T) {
	ctx := context.Background()

	tempDir, err := os.MkdirTemp("", "dsorm_l1_flaky_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	store := local.NewStore(tempDir)
	defer store.Close()

	flaky := &flakyIncrementCache{Cache: memory.NewCache()}
	db, err := dsorm.New(ctx,
		dsorm.WithStore(store),
		dsorm.WithCache(flaky),
		dsorm.WithQueryCache(time.Minute),
		dsorm.WithLocalQueryCache(dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}}),
		dsorm.WithOnErrorFunc(func(context.Context, error) {}),
	)
	if err != nil {
		t.Fatalf("Failed to init client: %v", err)
	}
	defer db.Close()

	if err := db.Put(ctx, &LCModel{ID: "i1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	rctx := dsorm.WithRequestScope(ctx)
	q := dsorm.NewQuery("LCModel").Order("value")
	if _, _, err := dsorm.Query[*LCModel](rctx, db, q, ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if db.LocalCacheStats().Kinds["LCModel"].Misses != 1 {
		t.Fatal("Expected the first query to populate the local cache")
	}

	// Invalidation is now impossible; the write itself still succeeds.
	flaky.fail.Store(true)
	if err := db.Put(rctx, &LCModel{ID: "i2", Value: "v2"}); err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	if _, _, err := dsorm.Query[*LCModel](rctx, db, q, ""); err != nil {
		t.Fatalf("Query2 failed: %v", err)
	}
	if hits := db.LocalCacheStats().Kinds["LCModel"].Hits; hits != 0 {
		t.Errorf("Expected the local cache to yield after a write it could not invalidate, got %d hits", hits)
	}

	// Retrying the invalidation, which is what *InvalidateError asks the
	// caller to do, restores read-your-writes.
	flaky.fail.Store(false)
	if err := db.InvalidateKinds(ctx, "LCModel"); err != nil {
		t.Fatalf("InvalidateKinds failed: %v", err)
	}
	got, _, err := dsorm.Query[*LCModel](dsorm.WithRequestScope(ctx), db, q, "")
	if err != nil {
		t.Fatalf("Query3 failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Expected 2 results after retrying the invalidation, got %d", len(got))
	}
}

// TestLocalQueryCacheRequiresQueryCache verifies the local cache refuses to run
// without the generation counters it depends on, and says so.
func TestLocalQueryCacheRequiresQueryCache(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "dsorm_l1_req_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	store := local.NewStore(tempDir)
	defer store.Close()

	var caught []string
	db, err := dsorm.New(ctx,
		dsorm.WithStore(store),
		dsorm.WithLocalQueryCache(dsorm.LocalCacheConfig{Kinds: []string{"LCModel"}}),
		dsorm.WithOnErrorFunc(func(_ context.Context, err error) { caught = append(caught, err.Error()) }),
	)
	if err != nil {
		t.Fatalf("Failed to init client: %v", err)
	}
	defer db.Close()

	if len(caught) != 1 || !strings.Contains(caught[0], "requires WithQueryCache") {
		t.Fatalf("Expected a disabled-cache report, got %v", caught)
	}
	if err := db.Put(ctx, &LCModel{ID: "q1", Value: "v1"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if _, _, err := dsorm.Query[*LCModel](ctx, db, dsorm.NewQuery("LCModel"), ""); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if stats := db.LocalCacheStats(); stats.Entries != 0 || len(stats.Kinds) != 0 {
		t.Errorf("Expected an inert local cache, got %+v", stats)
	}
}
