package ds

import (
	"context"
	"sync"

	"cloud.google.com/go/datastore"
	"go.opencensus.io/trace"
)

// deleteMultiLimit is batch size for DeleteMulti (500).
const deleteMultiLimit = 500

// DeleteMulti deletes multiple entities.
// - Batches requests (limit 500).
// - Manages cache locks directly.
func (c *Client) DeleteMulti(ctx context.Context, keys []*datastore.Key) error {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.DeleteMulti")
	defer span.End()

	callCount := (len(keys)-1)/deleteMultiLimit + 1
	errs := make([]error, callCount)

	var wg sync.WaitGroup
	wg.Add(callCount)
	for i := 0; i < callCount; i++ {
		lo := i * deleteMultiLimit
		hi := (i + 1) * deleteMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}

		go func(i int, keys []*datastore.Key) {
			errs[i] = c.deleteMulti(ctx, keys)
			wg.Done()
		}(i, keys[lo:hi])
	}
	wg.Wait()

	if isErrorsNil(errs) {
		return nil
	}

	return groupErrors(errs, len(keys), deleteMultiLimit)
}

// Delete deletes a single entity.
func (c *Client) Delete(ctx context.Context, key *datastore.Key) error {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.Delete")
	defer span.End()
	err := c.deleteMulti(ctx, []*datastore.Key{key})
	if me, ok := err.(datastore.MultiError); ok {
		return me[0]
	}
	return err
}

// deleteMulti locks cache, deletes from datastore, leaves locks to expire.
func (c *Client) deleteMulti(ctx context.Context, keys []*datastore.Key) error {
	if c.cacher != nil {
		_, lockCacheItems := getCacheLocks(c.cachePrefix, keys)

		// Make sure we can lock the cache with no errors before deleting.
		if err := c.cacher.SetMulti(ctx,
			lockCacheItems); err != nil {
			return err
		}
	}

	return c.Client.DeleteMulti(ctx, keys)
}
