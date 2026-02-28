package ds

import (
	"context"

	"cloud.google.com/go/datastore"
)

// ensure cloudStore implements the interfaces
var _ Store = (*cloudStore)(nil)
var _ Mutator = (*cloudStore)(nil)
var _ CloudAccess = (*cloudStore)(nil)
var _ TransactionMutator = (*cloudTransaction)(nil)

type cloudStore struct {
	client *datastore.Client
}

// NewCloudStore wraps a datastore.Client
func NewCloudStore(client *datastore.Client) *cloudStore {
	return &cloudStore{client: client}
}

// DatastoreClient returns the underlying *datastore.Client.
func (b *cloudStore) DatastoreClient() *datastore.Client {
	return b.client
}

func (b *cloudStore) Close() error {
	return b.client.Close()
}

func (b *cloudStore) Get(ctx context.Context, key *datastore.Key, dst interface{}) error {
	return b.client.Get(ctx, key, dst)
}

func (b *cloudStore) GetMulti(ctx context.Context, keys []*datastore.Key, dst interface{}) error {
	return b.client.GetMulti(ctx, keys, dst)
}

func (b *cloudStore) Put(ctx context.Context, key *datastore.Key, src interface{}) (*datastore.Key, error) {
	return b.client.Put(ctx, key, src)
}

func (b *cloudStore) PutMulti(ctx context.Context, keys []*datastore.Key, src interface{}) ([]*datastore.Key, error) {
	return b.client.PutMulti(ctx, keys, src)
}

func (b *cloudStore) Delete(ctx context.Context, key *datastore.Key) error {
	return b.client.Delete(ctx, key)
}

func (b *cloudStore) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	return b.client.DeleteMulti(ctx, keys)
}

func (b *cloudStore) Mutate(ctx context.Context, muts ...*datastore.Mutation) ([]*datastore.Key, error) {
	return b.client.Mutate(ctx, muts...)
}

func (b *cloudStore) Run(ctx context.Context, q Query) Iterator {
	dq := datastore.NewQuery(q.Kind())

	if q.IsKeysOnly() {
		dq = dq.KeysOnly()
	}
	if ns := q.GetNamespace(); ns != "" {
		dq = dq.Namespace(ns)
	}
	if ancestor := q.GetAncestor(); ancestor != nil {
		dq = dq.Ancestor(ancestor)
	}
	if limit := q.GetLimit(); limit > 0 {
		dq = dq.Limit(limit)
	}
	if offset := q.GetOffset(); offset > 0 {
		dq = dq.Offset(offset)
	}

	for _, filter := range q.Filters() {
		// e.g., FilterField("Field", ">", 10)
		dq = dq.FilterField(filter.Field, filter.Op, filter.Value)
	}

	for _, order := range q.Orders() {
		if order.Direction == "desc" {
			dq = dq.Order("-" + order.Field)
		} else {
			dq = dq.Order(order.Field)
		}
	}

	if cursorStr := q.GetCursor(); cursorStr != "" {
		c, err := datastore.DecodeCursor(cursorStr)
		if err == nil {
			dq = dq.Start(c)
		}
	}

	it := b.client.Run(ctx, dq)
	return &cloudIterator{it: it}
}

func (b *cloudStore) RunInTransaction(ctx context.Context, f func(tx TransactionStore) error, opts ...datastore.TransactionOption) (*datastore.Commit, error) {
	return b.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		gtx := &cloudTransaction{tx: tx}
		return f(gtx)
	}, opts...)
}

func (b *cloudStore) NewTransaction(ctx context.Context, opts ...datastore.TransactionOption) (TransactionStore, error) {
	tx, err := b.client.NewTransaction(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &cloudTransaction{tx: tx}, nil
}

type cloudIterator struct {
	it  *datastore.Iterator
	err error
}

func (i *cloudIterator) Next(dst interface{}) (*datastore.Key, error) {
	if i.err != nil {
		return nil, i.err
	}
	return i.it.Next(dst)
}

func (i *cloudIterator) Cursor() (string, error) {
	if i.err != nil {
		return "", i.err
	}
	c, err := i.it.Cursor()
	if err != nil {
		return "", err
	}
	return c.String(), nil
}

type cloudTransaction struct {
	tx *datastore.Transaction
}

func (t *cloudTransaction) Get(key *datastore.Key, dst interface{}) error {
	return t.tx.Get(key, dst)
}

func (t *cloudTransaction) GetMulti(keys []*datastore.Key, dst interface{}) error {
	return t.tx.GetMulti(keys, dst)
}

func (t *cloudTransaction) Put(key *datastore.Key, src interface{}) (*datastore.PendingKey, error) {
	return t.tx.Put(key, src)
}

func (t *cloudTransaction) PutMulti(keys []*datastore.Key, src interface{}) ([]*datastore.PendingKey, error) {
	return t.tx.PutMulti(keys, src)
}

func (t *cloudTransaction) Delete(key *datastore.Key) error {
	return t.tx.Delete(key)
}

func (t *cloudTransaction) DeleteMulti(keys []*datastore.Key) error {
	return t.tx.DeleteMulti(keys)
}

func (t *cloudTransaction) Commit() (*datastore.Commit, error) {
	return t.tx.Commit()
}

func (t *cloudTransaction) Rollback() error {
	return t.tx.Rollback()
}

func (t *cloudTransaction) Mutate(muts ...*datastore.Mutation) ([]*datastore.PendingKey, error) {
	return t.tx.Mutate(muts...)
}
