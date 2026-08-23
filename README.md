![Run Tests](https://github.com/altlimit/dsorm/actions/workflows/test.yml/badge.svg)

# dsorm

`dsorm` is a high-performance Go ORM for Google Cloud Datastore with built-in caching support (Memory, Redis, Memcache). It extends the official client with lifecycle hooks, struct tags for keys, field encryption, a robust caching layer to minimize Datastore costs and latency, and an application-level cache utility with rate limiting and JSON helpers.

## Features

- **Auto-Caching**: Transparently caches keys/entities in Memory, Redis, or Memcache.
- **Query Caching**: Generation-based query cache layer with auto-invalidation on Puts/Deletes.
- **In-Process Query Cache**: Optional per-kind L1 cache of decoded query results, invalidated exactly by the same generation counters.
- **Model Hooks**: `BeforeSave`, `AfterSave`, `OnLoad`, `BeforeDelete`, `AfterDelete` lifecycle methods.
- **Key Mapping**: Use struct tags (e.g., `model:"id"`) to map keys to struct fields.
- **Field Encryption**: Built-in encryption for sensitive string fields via `model:"name,encrypt"` tag.
- **JSON Marshaling**: Store complex structs/maps as compact JSON strings via `model:"name,marshal"` tag.
- **SQLite Backend**: High-performance SQLite-backed store — build local/embedded apps or develop offline using the same datastore API.
- **QueryBuilder**: Fluent query API with filters, ordering, pagination, ancestor, and namespace support.
- **API Parity**: Wraps standard `datastore` methods (`Put`, `Get`, `RunInTransaction`) for easy migration.

## Installation

```bash
go get github.com/altlimit/dsorm
```

## Usage

### 1. Initialization

Initialize the client with your context. `dsorm` automatically detects the best cache backend:
- **App Engine**: Uses Memcache.
- **Redis (`REDIS_ADDR` env)**: Uses Redis.
- **Default**: Uses in-memory cache.

```go
ctx := context.Background()

// Basic Init (Auto-detects)
client, err := dsorm.New(ctx)

// With Options
client, err = dsorm.New(ctx,
    dsorm.WithProjectID("my-project"),
    dsorm.WithEncryptionKey([]byte("my-32-byte-secret-key-here......")),
    dsorm.WithQueryCache(time.Hour), // Enable query cache with 1-hour TTL
)

// With Local SQLite Store (same API, no cloud dependency)
// NewStore accepts a DIRECTORY path, not a database file path.
// It creates separate .db files per namespace inside this directory.
store := local.NewStore("/tmp/myapp")   // import "github.com/altlimit/dsorm/ds/local"
client, err = dsorm.New(ctx, dsorm.WithStore(store))
defer client.Close()  // Close when done (closes underlying store connections)
```

### 2. Defining Models

Embed `dsorm.Base` and use tags for keys, properties, and lifecycle management.

```go
type User struct {
    dsorm.Base
    ID        string            `model:"id"`                              // Key Name (auto-excluded from datastore)
    Namespace string            `model:"ns"`                              // Key Namespace (auto-excluded)
    Parent    *datastore.Key    `model:"parent"`                          // Key Parent (auto-excluded)
    Username  string
    Email     string            `datastore:"email"`                       // Indexed property
    Bio       string            `datastore:"bio,noindex"`                 // Not indexed
    Secret    string            `model:"secret,encrypt"`                  // Encrypted + JSON-stored (auto-excluded)
    Profile   map[string]string `model:"profile,marshal" datastore:"-"`   // JSON-marshaled (datastore:"-" needed for maps)
    Tags      []string          `datastore:"tag"`                         // Multi-valued (each element indexed)
    CreatedAt time.Time         `model:"created"`                         // Auto-set on creation
    UpdatedAt time.Time         `model:"modified"`                        // Auto-set on every save
}
```

#### Tag Reference

| Tag | Purpose | Example |
|-----|---------|---------|
| `model:"id"` | Maps field to key ID/Name (`string` or `int64`). Auto-excluded from datastore. | `ID string \`model:"id"\`` |
| `model:"parent"` | Maps to key parent. Auto-excluded. (`*datastore.Key` or `*ParentModel`) | `Parent *datastore.Key \`model:"parent"\`` |
| `model:"ns"` | Maps to key namespace. Auto-excluded. | `NS string \`model:"ns"\`` |
| `model:"id,store"` | Maps to key ID AND stores as datastore property | `ID string \`model:"id,store"\`` |
| `model:"created"` | Auto-set `time.Time` on first Put | `CreatedAt time.Time \`model:"created"\`` |
| `model:"modified"` | Auto-set `time.Time` on every Put | `UpdatedAt time.Time \`model:"modified"\`` |
| `model:"name,marshal"` | JSON-marshal into a property. Auto-excluded from SaveStruct. | `Data map[string]string \`model:"data,marshal"\`` |
| `model:"name,encrypt"` | JSON-marshal + AES encrypt. Auto-excluded. | `Secret string \`model:"secret,encrypt"\`` |
| `datastore:"name"` | Property name for Datastore | `Email string \`datastore:"email"\`` |
| `datastore:"-"` | Exclude from Datastore | `Temp string \`datastore:"-"\`` |
| `datastore:",noindex"` | Store without indexing | `Bio string \`datastore:",noindex"\`` |

> **Note:** Fields tagged with `model:"id"`, `model:"ns"`, and `model:"parent"` are automatically excluded from datastore storage — no need for `datastore:"-"`. Use `,store` (e.g., `model:"id,store"`) to opt-in. For `model:"...,marshal"` and `model:"...,encrypt"`, auto-exclusion works for simple types (`string`, `int64`), but fields with types unsupported by `datastore.SaveStruct` (e.g., `map`, custom structs) still require `datastore:"-"`.

### 3. CRUD Operations

You don't need to manually construct keys. Just set the ID field.

```go
// Create
user := &User{ID: "alice", Username: "Alice"}
err := client.Put(ctx, user) // Key auto-constructed from ID

// Read
fetched := &User{ID: "alice"}
err := client.Get(ctx, fetched)

// Update
fetched.Username = "Alice_Updated"
err := client.Put(ctx, fetched) // UpdatedAt auto-updated

// Delete
err := client.Delete(ctx, fetched)

// Batch Operations
users := []*User{{ID: "a", Username: "A"}, {ID: "b", Username: "B"}}
err := client.PutMulti(ctx, users)
err = client.GetMulti(ctx, users)
err = client.DeleteMulti(ctx, users)
```

### 4. Queries

Use `dsorm.NewQuery()` with the fluent `QueryBuilder` API:

```go
// Basic query
q := dsorm.NewQuery("User").FilterField("email", "=", "alice@example.com")
users, nextCursor, err := dsorm.Query[*User](ctx, client, q, "")

// Filters, ordering, and pagination
q = dsorm.NewQuery("User").
    FilterField("score", ">=", 50).
    Order("score").
    Limit(10)
page1, cursor, err := dsorm.Query[*User](ctx, client, q, "")
page2, cursor, err := dsorm.Query[*User](ctx, client, q, cursor)

// Ancestor queries
parentKey := datastore.NameKey("Team", "engineering", nil)
q = dsorm.NewQuery("User").Ancestor(parentKey)

// Namespace queries
q = dsorm.NewQuery("User").Namespace("tenant-1")

// GetMulti by IDs (string, int64, *datastore.Key)
users, err := dsorm.GetMulti[*User](ctx, client, []string{"alice", "bob"})
```

#### QueryBuilder Methods

| Method | Description |
|--------|-------------|
| `FilterField(field, op, value)` | Add filter (`=`, `>`, `>=`, `<`, `<=`, `in`, `not-in`) |
| `Order(field)` | Sort ascending; prefix with `-` for descending |
| `Limit(n)` | Maximum results |
| `Offset(n)` | Skip first N results |
| `Ancestor(key)` | Scope to ancestor |
| `Namespace(ns)` | Scope to namespace |
| `Start(cursor)` | Resume from cursor |
| `KeysOnly()` | Return only keys |

### 5. Transactions

```go
_, err := client.Transact(ctx, func(tx *dsorm.Transaction) error {
    user := &User{ID: "bob"}
    if err := tx.Get(user); err != nil {
        return err
    }
    user.Username = "Bob (Verified)"
    return tx.Put(user)
})

// Batch operations in transactions
_, err = client.Transact(ctx, func(tx *dsorm.Transaction) error {
    // tx.PutMulti, tx.GetMulti, tx.Delete, tx.DeleteMulti all available
    return tx.PutMulti(users)
})
```

### 6. Lifecycle Hooks

Implement any of these interfaces on your model:

```go
func (u *User) BeforeSave(ctx context.Context, old dsorm.Model) error  { ... }
func (u *User) AfterSave(ctx context.Context, old dsorm.Model) error   { ... }
func (u *User) BeforeDelete(ctx context.Context) error                 { ... }
func (u *User) AfterDelete(ctx context.Context) error                  { ... }
func (u *User) OnLoad(ctx context.Context) error                       { ... }
```

### 7. Cache Utility

The `cache` package (`github.com/altlimit/dsorm/cache`) provides an application-level caching layer on top of any `ds.Cache` backend (Memory, Redis, Memcache). It simplifies single-key operations, adds atomic increment, typed JSON helpers, and built-in rate limiting.

```go
import (
    dscache "github.com/altlimit/dsorm/cache"
    "github.com/altlimit/dsorm/cache/memory"
    "github.com/altlimit/dsorm/cache/redis"
)

// Wrap any ds.Cache backend
c := dscache.New(memory.NewCache())
// or
redisCache, _ := redis.NewCache("localhost:6379")
c = dscache.New(redisCache)

// Single-key operations
err := c.Set(ctx, "user:alice", []byte(`{"name":"Alice"}`), 5*time.Minute)
item, err := c.Get(ctx, "user:alice")  // returns *ds.Item or ds.ErrCacheMiss
err = c.Delete(ctx, "user:alice")

// Atomic increment (creates key if missing)
count, err := c.Increment(ctx, "page:views", 1, 24*time.Hour)

// Typed JSON helpers (generics)
type Profile struct { Name string; Score int }
err = dscache.Save(ctx, c, "profile:alice", Profile{Name: "Alice", Score: 42}, time.Hour)
profile, err := dscache.Load[Profile](ctx, c, "profile:alice")

// Rate limiting
result, err := c.RateLimit(ctx, "api:user:alice", 100, time.Minute)
if !result.Allowed {
    // result.Remaining == 0, result.ResetAt tells when the window resets
}

// Access the underlying ds.Cache for batch operations
raw := c.Unwrap()
```

### 8. Query Caching

Enable query caching at initialization using `dsorm.WithQueryCache(ttl)`. The ORM hashes query parameters to create a deterministic cache key. Cache invalidation is atomic and automatic: saving (`Put`, `PutMulti`, `Transact`) or deleting entities bumps a generation counter for the affected `Kind`, instantly invalidating related query caches across the cluster without needing expensive sweep operations.

Generation counters are scoped per namespace, so in multi-tenant setups (one namespace per tenant) a write for one tenant only invalidates that tenant's cached queries.

```go
// Initialize with query cache enabled (default TTL is 1 hour if duration is omitted)
client, _ := dsorm.New(ctx, dsorm.WithQueryCache(time.Hour))

// First run (Miss: runs on datastore, saves results to cache)
q := dsorm.NewQuery("User").FilterField("status", "=", "active")
users, _, _ := dsorm.Query[*User](ctx, client, q, "")

// Second run (Hit: hydrates from cached keys, avoiding expensive query costs)
users2, _, _ := dsorm.Query[*User](ctx, client, q, "")

// Modify an entity. This atomically bumps the "User" version for that
// entity's namespace, invalidating all cached "User" queries in it instantly.
// Other namespaces keep their cached queries.
client.Put(ctx, &User{ID: "new-user"})

// If you bypass the client (e.g. writing through Store() directly), invalidate
// manually — InvalidateKinds targets the default namespace, InvalidateKindsNS
// a specific one.
client.InvalidateKinds(ctx, "User")
client.InvalidateKindsNS(ctx, "tenant-a", "User")
```

Invalidation is best-effort: if the cache backend is unreachable the write still
succeeds and the failure is reported to the handler set with
`dsorm.WithOnErrorFunc(fn)`, defaulting to `log.Println`. The same handler
receives non-fatal errors from the entity cache layer.

Invalidation failures arrive as `*dsorm.InvalidateError`, which carries the
namespace and the kinds that still need invalidating — the exact arguments to
`InvalidateKindsNS` — so a handler can retry rather than parse the message:

```go
var client *dsorm.Client

client, _ = dsorm.New(ctx,
    dsorm.WithQueryCache(time.Hour),
    dsorm.WithOnErrorFunc(func(ctx context.Context, err error) {
        slog.ErrorContext(ctx, "dsorm cache", "err", err)

        // Retry backstop: repair only what actually failed.
        for _, ie := range dsorm.InvalidateErrors(err) {
            go retryWithBackoff(func() error {
                return client.InvalidateKindsNS(ctx, ie.NS, ie.Kinds...)
            })
        }
    }),
)
```

Each kind is invalidated independently, so `Kinds` names exactly the ones that
failed — a working kind is never retried because a sibling failed. When a single
write spans several namespaces the failures are joined, one `InvalidateError`
per namespace; `dsorm.InvalidateErrors(err)` returns all of them (`errors.As`
returns only the first). Retrying is idempotent: generations are opaque
monotonic counters, so an extra bump costs at most one extra cache miss and
never serves stale results.

Note that `WithNoCache` (or `DSORM_NO_CACHE`) turns the query cache off — it has
nowhere to store generations — and reports that through the same handler.

### 9. In-Process Query Cache (L1)

`dsorm.WithLocalQueryCache` adds an in-process cache of **decoded query
results** on top of the query cache. It is aimed at the handful of kinds that
are read on every request and written rarely — a tenant record, its active
theme, its plugin list.

Without it, each such query costs three round trips to the shared cache: the
kind's generation counter, the cached key set, then the entity bodies. A local
hit skips the last two and the decode, leaving one round trip. That remaining
read is not an oversight — it is what keeps invalidation exact.

```go
client, _ := dsorm.New(ctx,
    dsorm.WithQueryCache(time.Hour),
    dsorm.WithLocalQueryCache(dsorm.LocalCacheConfig{
        Kinds:      []string{"Tenant", "Theme", "Plugin"}, // opt-in, per kind
        MaxBytes:   32 << 20,
        MaxEntries: 4096,
    }),
)
```

Entries are keyed by namespace, kind, the kind's generation counter, and the
query hash, so any write anywhere in the fleet makes every instance compute a
key it does not have. There is no TTL-based staleness in the normal case.

**Opt in per kind.** There is deliberately no "all kinds" setting. Version
scoping means any write to a kind in a namespace drops that kind's entire local
cache for that namespace — ideal for read-often/write-rarely kinds,
pathological for write-hot ones.

**Entities are rebuilt, never shared.** Results are held in their cached wire
form and a fresh struct is built per call, so an entity served from the local
cache is indistinguishable from a freshly loaded one — correct key, correct
change-tracking baseline, `IsNew()` false, `OnLoad` invoked, and the *current*
request's context. It is safe to mutate and `Put`:

```go
themes, _, _ := dsorm.Query[*Theme](ctx, client, q, "") // served locally
themes[0].Name = "updated"
client.Put(ctx, themes[0])                              // BeforeSave sees the right old value
```

**Bounded and observable.** A byte budget and an entry cap bound the cache
(`MaxBytes` defaults to 32MiB, `MaxEntries` to 4096), tracked incrementally so
the read path never scans. Concurrent identical misses collapse into one query.
`client.LocalCacheStats()` reports per-kind hits, misses, evictions, and drops —
a kind with a high eviction count is being squeezed out of the budget and will
not hit well no matter how rarely it is written.

The cache is dropped for a kind whenever it is written, and dropped entirely
when the shared cache is flushed or loses a generation counter (a restart, a
failover, an eviction) — in that case the client re-pins its last known counter,
which is correct for the shared cache but would otherwise leave local entries
reachable with data that outlived them. `LocalCacheConfig.TTL` (defaulting to
the query cache TTL) is a backstop for the same case, not the primary
invalidation mechanism.

It requires `WithQueryCache`; without generation counters there is nothing to
invalidate it, and it reports that through `WithOnErrorFunc` and stays off.

### 10. Per-Request Generation Reads

A page that queries five cached kinds still pays five round trips for five small
integers. `dsorm.WithRequestScope` reads each kind's generation counter at most
once per request, and `Client.PrefetchVersions` fetches them for several kinds
in a single batch:

```go
func middleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        ctx := dsorm.WithRequestScope(r.Context())
        client.PrefetchVersions(ctx, tenant, "Tenant", "Theme", "Plugin")
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

That takes a warm page from one round trip per kind to one per request. A kind
that was not prefetched still works: its counter is read on first use and pinned
from then on.

The trade is explicit. Two queries of the same kind within one request see the
same generation counter, so a write by another instance landing between them is
not observed until the next request. In exchange the request is a consistent
snapshot — a header and a footer rendering the same kind cannot disagree. The
request's own writes are always visible: every write updates the pinned counter
for the kinds it touched, and clears it if the counter could not be bumped. A
scope older than 30 seconds stops pinning, so a context that outlives its
request loses the optimization rather than serving stale data indefinitely.

## Configuration

| Variable | Description |
|----------|-------------|
| `DATASTORE_PROJECT_ID` | Google Cloud Project ID |
| `GOOGLE_CLOUD_PROJECT` | Fallback Project ID |
| `DATASTORE_EMULATOR_HOST` | Local emulator address |
| `DATASTORE_ENCRYPTION_KEY` | 32-byte key for field encryption (fallback) |
| `REDIS_ADDR` | Address for Redis cache (e.g., `localhost:6379`) |

## License

MIT
