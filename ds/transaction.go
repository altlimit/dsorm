package ds

import (
	"context"
	"sync"

	"cloud.google.com/go/datastore"
	"go.opencensus.io/trace"
)

// Transaction wraps ds.TransactionStore with caching.
type Transaction struct {
	c   *Client
	ctx context.Context
	tx  TransactionStore
	sync.Mutex
	lockCacheItems []*Item
}

func (t *Transaction) lockKey(key *datastore.Key) {
	t.lockKeys([]*datastore.Key{key})
}

func (t *Transaction) lockKeys(keys []*datastore.Key) {
	if t.c.cacher != nil {
		_, lockCacheItems := getCacheLocks(t.c.cachePrefix, keys)
		t.Lock()
		t.lockCacheItems = append(t.lockCacheItems,
			lockCacheItems...)
		t.Unlock()
	}
}

// NewTransaction starts a transaction with cache-aware context.
func (c *Client) NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (*Transaction, error) {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.NewTransaction")
	defer span.End()
	tx, err := c.Transactioner.NewTransaction(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &Transaction{c: c, ctx: ctx, tx: tx}, nil
}

func (t *Transaction) Get(key *datastore.Key, dst interface{}) error {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.Get")
	defer span.End()
	return t.tx.Get(key, dst)
}

// GetMulti batch get. Bypasses cache in TX.
func (t *Transaction) GetMulti(keys []*datastore.Key, dst interface{}) error {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.GetMulti")
	defer span.End()
	return t.tx.GetMulti(keys, dst)
}

func (t *Transaction) Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error) {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.Put")
	defer span.End()
	t.lockKey(key)
	return t.tx.Put(key, src)
}

// PutMulti batch put. Locks keys.
func (t *Transaction) PutMulti(keys []*datastore.Key, src interface{}) (ret []*datastore.PendingKey, err error) {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.PutMulti")
	defer span.End()
	t.lockKeys(keys)
	return t.tx.PutMulti(keys, src)
}

func (t *Transaction) Delete(key *datastore.Key) error {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.Delete")
	defer span.End()
	t.lockKey(key)
	return t.tx.Delete(key)
}

// DeleteMulti batch delete. Locks keys.
func (t *Transaction) DeleteMulti(keys []*datastore.Key) (err error) {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.DeleteMulti")
	defer span.End()
	t.lockKeys(keys)
	return t.tx.DeleteMulti(keys)
}

// Commit commits cache changes then TX.
func (t *Transaction) Commit() (*datastore.Commit, error) {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.Commit")
	defer span.End()

	if err := t.commitCache(); err != nil {
		return nil, err
	}
	return t.tx.Commit()
}

// Rollback passes through to TX.
func (t *Transaction) Rollback() (err error) {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.Rollback")
	defer span.End()
	// Block future use of this object
	t.Lock()
	return t.tx.Rollback()
}

// Mutate locks keys from mutations.
func (t *Transaction) Mutate(muts ...*Mutation) ([]*datastore.PendingKey, error) {
	var span *trace.Span
	t.ctx, span = trace.StartSpan(t.ctx, "github.com/altlimit/dsorm.Transaction.Mutate")
	defer span.End()
	mutations := make([]*datastore.Mutation, len(muts))
	keys := make([]*datastore.Key, len(muts))
	for i, mut := range muts {
		mutations[i] = mut.mut
		keys[i] = mut.k
	}
	t.lockKeys(keys)
	return t.tx.Mutate(mutations...)
}

// RunInTransaction wrapper ensuring cache interaction.
func (c *Client) RunInTransaction(ctx context.Context, f func(tx *Transaction) error, opts ...datastore.TransactionOption) (cmt *datastore.Commit, err error) {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.RunInTransaction")
	defer span.End()

	return c.Transactioner.RunInTransaction(ctx, func(tx TransactionStore) error {
		txn := &Transaction{c: c, ctx: ctx, tx: tx}
		if err := f(txn); err != nil {
			return err
		}

		return txn.commitCache()
	}, opts...)
}

// commitCache applies aggregated cache locks.
func (t *Transaction) commitCache() error {
	t.Lock()
	if t.c.cacher != nil {
		return t.c.cacher.SetMulti(t.ctx, t.lockCacheItems)
	}
	return nil
}
