---
name: dsorm
description: Go ORM for Google Cloud Datastore with caching, lifecycle hooks, encryption, and local SQLite backend
---

# dsorm — AI Usage Guide

## Overview

`dsorm` is a Go ORM wrapping Google Cloud Datastore. It provides:
- Auto key management via struct tags
- Transparent caching (Memory, Redis, Memcache)
- Lifecycle hooks (BeforeSave, AfterSave, OnLoad, BeforeDelete, AfterDelete)
- Field encryption and JSON marshaling via `model` tag options
- SQLite backend — build local/embedded apps or develop offline using the same datastore API
- Generic helper functions `Query[T]` and `GetMulti[T]`

## Project Structure

```
dsorm/
├── orm.go          # Core client, CRUD, transactions, generic helpers
├── query.go        # QueryBuilder API
├── orm_test.go     # All tests
├── ds/
│   ├── store.go      # Store/TransactionStore/Iterator interfaces
│   ├── client.go     # ds.Client (caching layer)
│   ├── cloud_store.go # Cloud Datastore adapter
│   ├── ds.go         # Shared utilities
│   └── local/
│       └── local.go  # SQLite adapter (separate package, optional dependency)
├── cache/
│   ├── cache.go      # Application-level Cache interface, New(), Load/Save, RateLimit
│   ├── memory/       # In-memory LRU backend
│   ├── redis/        # Redis/Valkey backend
│   └── memcache/     # AppEngine Memcache backend
└── internal/         # encryption, structtag, util
```

## Model Definition

Every model embeds `dsorm.Base` and uses struct tags:

```go
type User struct {
    dsorm.Base
    ID        string            `model:"id"`                        // Key name (auto-excluded from datastore)
    Namespace string            `model:"ns"`                        // Key namespace (auto-excluded)
    Parent    *datastore.Key    `model:"parent"`                    // Key parent (auto-excluded)
    CreatedAt time.Time         `model:"created"`                   // Auto-set on first save
    UpdatedAt time.Time         `model:"modified"`                  // Auto-set on every save
    Email     string            `datastore:"email"`                 // Indexed property
    Bio       string            `datastore:"bio,noindex"`           // Not indexed
    Ignored   string            `datastore:"-"`                     // Excluded from datastore
    Secret    string            `model:"secret,encrypt"`            // Encrypted JSON property (auto-excluded)
    Profile   map[string]string `model:"profile,marshal" datastore:"-"`  // JSON-marshaled (datastore:"-" needed for maps)
    Tags      []string          `datastore:"tag"`                   // Multi-valued (indexed per element)
}
```

### Tag Reference

| Tag | Purpose | Example |
|-----|---------|---------|
| `model:"id"` | Maps field to datastore key ID/Name. Auto-excluded from datastore properties. | `ID string \`model:"id"\`` |
| `model:"parent"` | Maps to key's parent. Auto-excluded. Can be `*datastore.Key` or `*ParentModel` | `Parent *datastore.Key \`model:"parent"\`` |
| `model:"ns"` | Maps to key's namespace. Auto-excluded. | `NS string \`model:"ns"\`` |
| `model:"id,store"` | Maps to key ID AND stores in datastore properties | `ID string \`model:"id,store"\`` |
| `model:"created"` | Auto-set `time.Time` on first Put | `CreatedAt time.Time \`model:"created"\`` |
| `model:"modified"` | Auto-set `time.Time` on every Put | `UpdatedAt time.Time \`model:"modified"\`` |
| `model:"name,marshal"` | JSON-marshal to a datastore property. Auto-excluded from SaveStruct. | `Data map[string]string \`model:"data,marshal"\`` |
| `model:"name,encrypt"` | JSON-marshal + encrypt (implies marshal). Auto-excluded. | `Secret string \`model:"secret,encrypt"\`` |
| `datastore:"name"` | Standard datastore property name | `Email string \`datastore:"email"\`` |
| `datastore:"-"` | Exclude from datastore | `Temp string \`datastore:"-"\`` |
| `datastore:",noindex"` | Store but don't index | `Bio string \`datastore:",noindex"\`` |

> **Note:** Fields tagged with `model:"id"`, `model:"ns"`, and `model:"parent"` are automatically excluded from datastore storage — no need for `datastore:"-"`. Use `,store` (e.g., `model:"id,store"`) to opt-in. For `model:"...,marshal"` and `model:"...,encrypt"`, auto-exclusion works for simple types (`string`, `int64`), but fields with types unsupported by `datastore.SaveStruct` (e.g., `map`, custom structs) still require `datastore:"-"`.

### Parent as Struct

You can use a struct pointer instead of `*datastore.Key` for parent:

```go
type ParentModel struct {
    dsorm.Base
    ID string `model:"id"`
}

type ChildModel struct {
    dsorm.Base
    ID     string       `model:"id"`
    Parent *ParentModel `model:"parent" datastore:"-"`
}
```

## Client Initialization

```go
// Auto-detect cache (AppEngine→Memcache, REDIS_ADDR→Redis, default→Memory)
client, err := dsorm.New(ctx)

// With options
client, err := dsorm.New(ctx,
    dsorm.WithProjectID("my-project"),
    dsorm.WithEncryptionKey([]byte("32-byte-key-here................")),
)

// With local SQLite store (same API, no cloud dependency)
// NewStore accepts a DIRECTORY path, not a database file path.
// It creates separate .db files per namespace inside this directory.
localStore := local.NewStore("/tmp/myapp")  // import "github.com/altlimit/dsorm/ds/local"
client, err := dsorm.New(ctx, dsorm.WithStore(localStore))

// Close the client when done (closes underlying store connections)
defer client.Close()
```

## CRUD Operations

```go
// Put (create or update)
user := &User{ID: "alice", Email: "alice@example.com"}
err := client.Put(ctx, user)

// Get
fetched := &User{ID: "alice"}
err := client.Get(ctx, fetched)

// PutMulti
users := []*User{{ID: "a"}, {ID: "b"}}
err := client.PutMulti(ctx, users)

// GetMulti
toFetch := []*User{{ID: "a"}, {ID: "b"}}
err := client.GetMulti(ctx, toFetch)

// Delete
err := client.Delete(ctx, user)

// DeleteMulti
err := client.DeleteMulti(ctx, users)
```

### Generic Helpers

```go
// Query[T] — type-safe query with automatic pagination
q := dsorm.NewQuery("User").FilterField("email", "=", "alice@example.com")
users, nextCursor, err := dsorm.Query[*User](ctx, client, q, "")

// GetMulti[T] — fetch by IDs (string, int64, int, *datastore.Key, or structs)
users, err := dsorm.GetMulti[*User](ctx, client, []string{"alice", "bob"})
users, err := dsorm.GetMulti[*User](ctx, client, []int64{1, 2, 3})
```

## QueryBuilder API

Use `dsorm.NewQuery()` (not `datastore.NewQuery`):

```go
q := dsorm.NewQuery("User").
    FilterField("email", "=", "alice@example.com").  // Equality
    FilterField("score", ">", 50).                   // Inequality: >, >=, <, <=
    FilterField("tag", "in", []interface{}{"go", "rust"}). // IN filter
    Order("score").                                   // Ascending
    Order("-created").                                // Descending (prefix with -)
    Limit(10).                                        // Max results
    Offset(5).                                        // Skip first N
    Namespace("tenant-1").                            // Namespace scoping
    Ancestor(parentKey)                               // Ancestor scoping

results, cursor, err := dsorm.Query[*User](ctx, client, q, "")

// Pagination — pass cursor from previous query
page2, cursor2, err := dsorm.Query[*User](ctx, client, q, cursor)
```

## Transactions

```go
_, err := client.Transact(ctx, func(tx *dsorm.Transaction) error {
    user := &User{ID: "alice"}
    if err := tx.Get(user); err != nil {
        return err
    }
    user.Email = "new@example.com"
    return tx.Put(user)
})

// Multi operations in transactions
_, err := client.Transact(ctx, func(tx *dsorm.Transaction) error {
    // tx.PutMulti, tx.GetMulti, tx.Delete, tx.DeleteMulti all available
    return tx.PutMulti(users)
})
```

## Lifecycle Hooks

Implement any of these interfaces on your model:

```go
type BeforeSave interface {
    BeforeSave(ctx context.Context, old Model) error  // old is nil on create
}
type AfterSave interface {
    AfterSave(ctx context.Context, old Model) error
}
type BeforeDelete interface {
    BeforeDelete(ctx context.Context) error
}
type AfterDelete interface {
    AfterDelete(ctx context.Context) error
}
type OnLoad interface {
    OnLoad(ctx context.Context) error  // Called after Get/GetMulti
}
```

## Cache Utility

The `cache` package (`github.com/altlimit/dsorm/cache`) wraps any `ds.Cache` backend into a convenient application-level API with single-key operations, atomic increment, typed JSON helpers, and rate limiting.

```go
import (
    dscache "github.com/altlimit/dsorm/cache"
    "github.com/altlimit/dsorm/cache/memory"
    "github.com/altlimit/dsorm/cache/redis"
)

// Wrap any ds.Cache backend
c := dscache.New(memory.NewCache())
// or with Redis
redisCache, _ := redis.NewCache("localhost:6379")
c = dscache.New(redisCache)
```

### Single-Key Operations

```go
err := c.Set(ctx, "user:alice", []byte(`{"name":"Alice"}`), 5*time.Minute)
item, err := c.Get(ctx, "user:alice")  // returns *ds.Item; ds.ErrCacheMiss if missing
err = c.Delete(ctx, "user:alice")
count, err := c.Increment(ctx, "views", 1, 24*time.Hour)  // atomic; creates key if missing
```

### Typed JSON Helpers (Generics)

```go
type Profile struct { Name string; Score int }
err := dscache.Save(ctx, c, "profile:alice", Profile{Name: "Alice", Score: 42}, time.Hour)
profile, err := dscache.Load[Profile](ctx, c, "profile:alice")
```

### Rate Limiting

```go
result, err := c.RateLimit(ctx, "api:user:alice", 100, time.Minute)
if !result.Allowed {
    // result.Remaining == 0, result.ResetAt tells when the window resets
}
```

### Unwrap

Access the underlying `ds.Cache` for batch operations:

```go
raw := c.Unwrap() // returns ds.Cache
```

## SQLite Backend

The SQLite store (`local.NewStore`) is a high-performance alternative to Cloud Datastore, suitable for local/embedded applications or offline development. It implements the full `ds.Store` interface:
- CRUD, queries with filters/ordering/pagination
- Transactions
- Namespace isolation (separate DB files per namespace)
- Slice property indexing (each element indexed separately)

```go
// Same API, no cloud dependency
// NewStore accepts a DIRECTORY path, not a database file path.
// It creates separate .db files per namespace inside this directory.
store := local.NewStore("/tmp/dev")   // import "github.com/altlimit/dsorm/ds/local"
client, err := dsorm.New(ctx, dsorm.WithStore(store))
defer client.Close()
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `DATASTORE_PROJECT_ID` | Project ID for Cloud Datastore |
| `GOOGLE_CLOUD_PROJECT` | Fallback project ID |
| `DATASTORE_EMULATOR_HOST` | Point to local emulator |
| `DATASTORE_ENCRYPTION_KEY` | Fallback encryption key |
| `REDIS_ADDR` | Redis address for caching |

## Testing

Run all tests (requires Docker Compose for emulator + Redis):

```bash
cd test && docker compose up -d
cd .. && go test -v ./...
```
