![Run Tests](https://github.com/altlimit/dsorm/actions/workflows/test.yml/badge.svg)

# dsorm

`dsorm` is a high-performance Go ORM for Google Cloud Datastore with built-in caching support (Memory, Redis, Memcache). It extends the official client with lifecycle hooks, struct tags for keys, field encryption, and a robust caching layer to minimize Datastore costs and latency.

## Features

- **Auto-Caching**: Transparently caches keys/entities in Memory, Redis, or Memcache.
- **Model Hooks**: `BeforeSave`, `AfterSave`, `OnLoad` lifecycle methods.
- **Key Mapping**: Use struct tags (e.g., `model:"id"`) to map keys to struct fields.
- **Field Encryption**: Built-in encryption for sensitive string fields via `encrypt` tag.
- **JSON Marshaling**: Store complex structs/maps as compact JSON strings via `marshal` tag.
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
if err != nil {
    panic(err)
}



// With Options
client, err = dsorm.New(ctx, 
    dsorm.WithProjectID("my-project"),
    dsorm.WithCachePrefix("myapp:"), 
)
```

### 2. Defining Models

Embed `dsorm.Base` and use tags for keys, properties, and lifecycle management.

```go
type User struct {
    dsorm.Base
    ID        string         `model:"id"`       // Auto-used for Key Name
    Namespace string         `model:"ns"`       // Auto-used for Key Namespace
    Parent    *datastore.Key `model:"parent"`   // Auto-used for Key Parent (can also use *ParentModel)
    Username  string
    Email     string    `datastore:"email"`
    Secret    string    `encrypt:""`
    Profile   map[string]string `marshal:"profile"` 
    CreatedAt time.Time `model:"created"`  // Auto-set on creation
    UpdatedAt time.Time `model:"modified"` // Auto-set on save
}

// Optional: Lifecycle Hooks
func (u *User) BeforeSave(ctx context.Context, m dsorm.Model) error {
    u.Namespace = "my-app" // Set defaults
    return nil
}

func (u *User) AfterSave(ctx context.Context, old dsorm.Model) error {
    // old is nil if creating new entity
    if old != nil {
        oldUser := old.(*User)
        fmt.Printf("Changed from %s to %s\n", oldUser.Username, u.Username)
    }
    return nil
}

func (u *User) BeforeDelete(ctx context.Context) error {
    return nil
}

func (u *User) AfterDelete(ctx context.Context) error {
    return nil
}
```

### 3. CRUD Operations

You don't need to manually construct keys. Just set the ID field.

```go
// Create
user := &User{
    ID:       "alice",
    Username: "Alice",
}
// Key is auto-constructed from ID and Namespace tags during Put
err := client.Put(ctx, user) 

// Read
fetched := &User{ID: "alice"}
// Key is auto-reconstructed from ID for the Get lookup
err := client.Get(ctx, fetched)

// Update
fetched.Username = "Alice_Updated"
err := client.Put(ctx, fetched) // UpdatedAt will be auto-updated

// Delete
err := client.Delete(ctx, fetched)
```

### 4. Transactions

Pass models to `Transact` to ensure they are initialized and locked correctly.

```go
user := &User{ID: "bob"}

_, err := client.Transact(ctx, func(tx *dsorm.Transaction) error {
    if err := tx.Get(user); err != nil {
        return err
    }
    user.Username = "Bob (Verified)"
    return tx.Put(user)
})
```

### 5. Queries

Query keys-only and auto-fetch entities in batches (of 1000) for performance.

```go
q := datastore.NewQuery("User").FilterField("Username", "=", "Alice")

// Wrapper helper
users, nextCursor, err := dsorm.Query[*User](ctx, client, q, "")

// GetMulti Helper (Get by IDs, Keys, or Structs)
ids := []string{"alice", "bob"}
users, err := dsorm.GetMulti[*User](ctx, client, ids)
```

## Configuration

Set environment variables to configure defaults:

| Variable | Description |
|----------|-------------|
| `DATASTORE_PROJECT_ID` | Google Cloud Project ID. |
| `DATASTORE_ENCRYPTION_KEY` | 32-byte key for field encryption. |
| `REDIS_ADDR` | Address for Redis cache (e.g., `localhost:6379`). |

## License

MIT
