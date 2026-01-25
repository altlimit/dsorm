package ds

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

var (
	// This exists purely for testing
	mutateHook func() error
)

// mutationType enum.
type mutationType byte

const (
	insertMutation mutationType = iota
	upsertMutation
	updateMutation
	deleteMutation
)

// Mutation represents a datastore mutation.
type Mutation struct {
	typ mutationType
	k   *datastore.Key
	mut *datastore.Mutation
}

func NewDelete(k *datastore.Key) *Mutation {
	return &Mutation{
		typ: deleteMutation,
		k:   k,
		mut: datastore.NewDelete(k),
	}
}

func NewInsert(k *datastore.Key, src interface{}) *Mutation {
	return &Mutation{
		typ: insertMutation,
		k:   k,
		mut: datastore.NewInsert(k, src),
	}
}

func NewUpdate(k *datastore.Key, src interface{}) *Mutation {
	return &Mutation{
		typ: updateMutation,
		k:   k,
		mut: datastore.NewUpdate(k, src),
	}
}

func NewUpsert(k *datastore.Key, src interface{}) *Mutation {
	return &Mutation{
		typ: upsertMutation,
		k:   k,
		mut: datastore.NewUpsert(k, src),
	}
}

// Mutate applies mutations.
// - Locks affecting keys in cache.
// - Invalidates cache on success.
func (c *Client) Mutate(ctx context.Context, muts ...*Mutation) ([]*datastore.Key, error) {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.Mutate")
	defer span.End()

	toLock := make([]*datastore.Key, 0, len(muts))
	toLockRelease := make([]*datastore.Key, 0, len(muts))
	mutations := make([]*datastore.Mutation, len(muts))
	for i, mutation := range muts {
		mutations[i] = mutation.mut

		switch mutation.typ {
		case insertMutation, upsertMutation, updateMutation:
			// Insert/Upsert/Update: lock and release (like Put)
			toLockRelease = append(toLockRelease, mutation.k)
		case deleteMutation:
			// Delete: lock only (let expire)
			toLock = append(toLock, mutation.k)
		}
	}

	if c.cacher != nil {
		releaseCacheKeys, lockCacheItems := getCacheLocks(c.cachePrefix, toLockRelease)
		_, moreLockCacheItems := getCacheLocks(c.cachePrefix, toLock)
		lockCacheItems = append(lockCacheItems, moreLockCacheItems...)

		defer func() {
			// Optimistcally remove the locks.
			if err := c.cacher.DeleteMulti(ctx,
				releaseCacheKeys); err != nil {
				c.onError(ctx, errors.Wrap(err, "Mutate cache.DeleteMulti"))
			}
		}()

		if err := c.cacher.SetMulti(ctx,
			lockCacheItems); err != nil {
			return nil, err
		}

		if mutateHook != nil {
			if err := mutateHook(); err != nil {
				return nil, err
			}
		}
	}

	return c.Client.Mutate(ctx, mutations...)
}
