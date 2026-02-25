package ds

import (
	"context"

	"cloud.google.com/go/datastore"
)

// Iterator is an interface that abstracts datastore.Iterator
type Iterator interface {
	Next(dst interface{}) (*datastore.Key, error)
	Cursor() (string, error)
}

// Store is the interface that abstracts the underlying datastore.
// It allows replacing Google Cloud Datastore with implementations like LocalDB.
type Store interface {
	Get(ctx context.Context, key *datastore.Key, dst interface{}) error
	GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error
	Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error)
	PutMulti(ctx context.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error)
	Delete(ctx context.Context, key *datastore.Key) error
	DeleteMulti(ctx context.Context, keys []*datastore.Key) error
	Mutate(ctx context.Context, muts ...*datastore.Mutation) ([]*datastore.Key, error)
}

// Filter represents a query filter natively in the ds package.
type Filter struct {
	Field string
	Op    string
	Value interface{}
}

// Order represents a query order natively in the ds package.
type Order struct {
	Field     string
	Direction string // "asc" or "desc"
}

// Query defines the methods required for a backend to translate a generic dsorm query.
type Query interface {
	Kind() string
	Filters() []Filter
	Orders() []Order
	GetLimit() int
	GetOffset() int
	IsKeysOnly() bool
	GetAncestor() *datastore.Key
	GetCursor() string
	GetNamespace() string
}

// Queryer is implemented by Stores that support querying.
// It accepts a ds.Query interface to avoid import cycles with dsorm.
type Queryer interface {
	Run(ctx context.Context, q Query) Iterator
}

// Transactioner abstracts transactions
type Transactioner interface {
	NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (TransactionStore, error)
	RunInTransaction(ctx context.Context, f func(tx TransactionStore) error, opts ...datastore.TransactionOption) (*datastore.Commit, error)
}

// TransactionStore abstracts datastore.Transaction
type TransactionStore interface {
	Get(key *datastore.Key, dst interface{}) error
	GetMulti(keys []*datastore.Key, dst interface{}) error
	Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error)
	PutMulti(keys []*datastore.Key, src interface{}) ([]*datastore.PendingKey, error)
	Delete(key *datastore.Key) error
	DeleteMulti(keys []*datastore.Key) error
	Commit() (*datastore.Commit, error)
	Rollback() error
	Mutate(muts ...*datastore.Mutation) ([]*datastore.PendingKey, error)
}
