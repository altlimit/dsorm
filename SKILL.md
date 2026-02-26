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
- Field encryption and JSON marshaling via `marshal` tag
- Local SQLite backend for development (no Cloud Datastore needed)
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
│   ├── local_store.go # SQLite adapter for local dev
│   └── ds.go         # Shared utilities
├── cache/            # Cache backends (memory, redis, memcache)
└── internal/         # encryption, structtag, util
```

## Model Definition

Every model embeds `dsorm.Base` and uses struct tags:

```go
type User struct {
    dsorm.Base
    ID        string            `model:"id"`                        // Key name
    Namespace string            `model:"ns"`                        // Key namespace
    Parent    *datastore.Key    `model:"parent"`                    // Key parent
    CreatedAt time.Time         `model:"created"`                   // Auto-set on first save
    UpdatedAt time.Time         `model:"modified"`                  // Auto-set on every save
    Email     string            `datastore:"email"`                 // Indexed property
    Bio       string            `datastore:"bio,noindex"`           // Not indexed
    Ignored   string            `datastore:"-"`                     // Excluded from datastore
    Secret    string            `marshal:"secret,encrypt" datastore:"-"` // Encrypted JSON property
    Profile   map[string]string `marshal:"profile" datastore:"-"`   // JSON-marshaled property
    Tags      []string          `datastore:"tag"`                   // Multi-valued (indexed per element)
}
```

### Tag Reference

| Tag | Purpose | Example |
|-----|---------|---------|
| `model:"id"` | Maps field to datastore key ID/Name | `ID string \`model:"id"\`` |
| `model:"parent"` | Maps to key's parent. Can be `*datastore.Key` or `*ParentModel` | `Parent *datastore.Key \`model:"parent"\`` |
| `model:"ns"` | Maps to key's namespace | `NS string \`model:"ns"\`` |
| `model:"created"` | Auto-set `time.Time` on first Put | `CreatedAt time.Time \`model:"created"\`` |
| `model:"modified"` | Auto-set `time.Time` on every Put | `UpdatedAt time.Time \`model:"modified"\`` |
| `datastore:"name"` | Standard datastore property name | `Email string \`datastore:"email"\`` |
| `datastore:"-"` | Exclude from datastore | `Temp string \`datastore:"-"\`` |
| `datastore:",noindex"` | Store but don't index | `Bio string \`datastore:",noindex"\`` |
| `marshal:"name"` | JSON-marshal to a datastore property | `Data map[string]string \`marshal:"data" datastore:"-"\`` |
| `marshal:"name,encrypt"` | JSON-marshal + encrypt | `Secret string \`marshal:"secret,encrypt" datastore:"-"\`` |

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

// With local SQLite store (no Cloud Datastore needed)
localStore, _ := ds.NewLocalStore("/tmp/myapp.db")
client, err := dsorm.New(ctx, dsorm.WithStore(localStore))
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

## Local Development

The SQLite backend (`ds.NewLocalStore`) supports all features:
- CRUD, queries with filters/ordering/pagination
- Transactions
- Namespace isolation (separate DB files per namespace)
- Slice property indexing (each element indexed separately)

```go
store, _ := ds.NewLocalStore("/tmp/dev.db")
client, _ := dsorm.New(ctx, dsorm.WithStore(store))
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
