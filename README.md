![Run Tests](https://github.com/altlimit/dsorm/actions/workflows/test.yml/badge.svg)

# dsorm

`dsorm` is a high-performance Go ORM for Google Cloud Datastore with built-in caching support (Memory, Redis, Memcache). It extends the official client with lifecycle hooks, struct tags for keys, field encryption, and a robust caching layer to minimize Datastore costs and latency.

## Features

- **Auto-Caching**: Transparently caches keys/entities in Memory, Redis, or Memcache.
- **Model Hooks**: `BeforeSave`, `AfterSave`, `OnLoad`, `BeforeDelete`, `AfterDelete` lifecycle methods.
- **Key Mapping**: Use struct tags (e.g., `model:"id"`) to map keys to struct fields.
- **Field Encryption**: Built-in encryption for sensitive string fields via `marshal:"name,encrypt"` tag.
- **JSON Marshaling**: Store complex structs/maps as compact JSON strings via `marshal` tag.
- **Local Development**: SQLite-backed local store — no Cloud Datastore needed for development.
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

// With Local SQLite Store (no Cloud Datastore needed)
store, _ := ds.NewLocalStore("/tmp/dev.db")
client, err = dsorm.New(ctx, dsorm.WithStore(store))
```

### 2. Defining Models

Embed `dsorm.Base` and use tags for keys, properties, and lifecycle management.

```go
type User struct {
    dsorm.Base
    ID        string            `model:"id"`                              // Key Name
    Namespace string            `model:"ns"`                              // Key Namespace
    Parent    *datastore.Key    `model:"parent"`                          // Key Parent
    Username  string
    Email     string            `datastore:"email"`                       // Indexed property
    Bio       string            `datastore:"bio,noindex"`                 // Not indexed
    Secret    string            `marshal:"secret,encrypt" datastore:"-"`  // Encrypted + JSON-stored
    Profile   map[string]string `marshal:"profile" datastore:"-"`         // JSON-marshaled
    Tags      []string          `datastore:"tag"`                         // Multi-valued (each element indexed)
    CreatedAt time.Time         `model:"created"`                         // Auto-set on creation
    UpdatedAt time.Time         `model:"modified"`                        // Auto-set on every save
}
```

#### Tag Reference

| Tag | Purpose | Example |
|-----|---------|---------|
| `model:"id"` | Maps field to key ID/Name (`string` or `int64`) | `ID string \`model:"id"\`` |
| `model:"parent"` | Maps to key parent (`*datastore.Key` or `*ParentModel`) | `Parent *datastore.Key \`model:"parent"\`` |
| `model:"ns"` | Maps to key namespace | `NS string \`model:"ns"\`` |
| `model:"created"` | Auto-set `time.Time` on first Put | `CreatedAt time.Time \`model:"created"\`` |
| `model:"modified"` | Auto-set `time.Time` on every Put | `UpdatedAt time.Time \`model:"modified"\`` |
| `datastore:"name"` | Property name for Datastore | `Email string \`datastore:"email"\`` |
| `datastore:"-"` | Exclude from Datastore | `Temp string \`datastore:"-"\`` |
| `datastore:",noindex"` | Store without indexing | `Bio string \`datastore:",noindex"\`` |
| `marshal:"name"` | JSON-marshal into a property | `Data map[string]string \`marshal:"data" datastore:"-"\`` |
| `marshal:"name,encrypt"` | JSON-marshal + AES encrypt | `Secret string \`marshal:"secret,encrypt" datastore:"-"\`` |

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
