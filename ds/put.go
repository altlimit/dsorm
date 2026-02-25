package ds

import (
	"context"
	"reflect"
	"sync"

	"cloud.google.com/go/datastore"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

// putMultiLimit is the batch size for PutMulti (500).
const putMultiLimit = 500

var (
	// putMultiHook testing hook
	putMultiHook func() error
)

// PutMulti saves entities.
// - Batches requests (limit 500).
// - Invalidates/Updates cache locks.
func (c *Client) PutMulti(ctx context.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.PutMulti")
	defer span.End()

	if len(keys) == 0 {
		return nil, nil
	}

	v := reflect.ValueOf(vals)
	if err := checkKeysValues(keys, v); err != nil {
		return nil, err
	}

	callCount := (len(keys)-1)/putMultiLimit + 1
	putKeys := make([][]*datastore.Key, callCount)
	errs := make([]error, callCount)

	var wg sync.WaitGroup
	wg.Add(callCount)
	for i := 0; i < callCount; i++ {
		lo := i * putMultiLimit
		hi := (i + 1) * putMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}

		go func(i int, keys []*datastore.Key, vals reflect.Value) {
			putKeys[i], errs[i] = c.putMulti(ctx, keys, vals.Interface())
			wg.Done()
		}(i, keys[lo:hi], v.Slice(lo, hi))
	}
	wg.Wait()

	if isErrorsNil(errs) {
		groupedKeys := make([]*datastore.Key, len(keys))
		for i, k := range putKeys {
			lo := i * putMultiLimit
			hi := (i + 1) * putMultiLimit
			if hi > len(keys) {
				hi = len(keys)
			}
			copy(groupedKeys[lo:hi], k)
		}
		return groupedKeys, nil
	}

	groupedKeys := make([]*datastore.Key, len(keys))
	groupedErrs := make(datastore.MultiError, len(keys))
	for i, err := range errs {
		lo := i * putMultiLimit
		hi := (i + 1) * putMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}
		if me, ok := err.(datastore.MultiError); ok {
			for j, e := range me {
				if e == nil {
					groupedKeys[lo+j] = putKeys[i][j]
				} else {
					groupedErrs[lo+j] = e
				}
			}
		} else if err != nil {
			for j := lo; j < hi; j++ {
				groupedErrs[j] = err
			}
		}
	}

	return groupedKeys, groupedErrs
}

// Put saves an entity.
func (c *Client) Put(ctx context.Context,
	key *datastore.Key, val interface{}) (*datastore.Key, error) {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.Put")
	defer span.End()

	keys := []*datastore.Key{key}
	vals := []interface{}{val}
	if err := checkKeysValues(keys, reflect.ValueOf(vals)); err != nil {
		return nil, err
	}

	keys, err := c.putMulti(ctx, keys, vals)
	switch e := err.(type) {
	case nil:
		return keys[0], nil
	case datastore.MultiError:
		return nil, e[0]
	default:
		return nil, err
	}
}

// putMulti locks cache, puts to datastore, releases locks.
func (c *Client) putMulti(ctx context.Context,
	keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {
	if c.cacher != nil {
		lockCacheKeys, lockCacheItems := getCacheLocks(c.cachePrefix, keys)

		defer func() {
			// Remove the locks.
			if err := c.cacher.DeleteMulti(ctx,
				lockCacheKeys); err != nil {
				c.onError(ctx, errors.Wrap(err, "putMulti cache.DeleteMulti"))
			}
		}()

		if err := c.cacher.SetMulti(ctx,
			lockCacheItems); err != nil {
			return nil, err
		}

		if putMultiHook != nil {
			if err := putMultiHook(); err != nil {
				return keys, err
			}
		}
	}
	return c.Store.PutMulti(ctx, keys, vals)
}
