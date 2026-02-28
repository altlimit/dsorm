![Run Tests](https://github.com/altlimit/dsorm/actions/workflows/test.yml/badge.svg)

# dsorm

`dsorm` is a high-performance Go ORM for Google Cloud Datastore with built-in caching support (Memory, Redis, Memcache). It extends the official client with lifecycle hooks, struct tags for keys, field encryption, a robust caching layer to minimize Datastore costs and latency, and an application-level cache utility with rate limiting and JSON helpers.

## Features

- **Auto-Caching**: Transparently caches keys/entities in Memory, Redis, or Memcache.
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
