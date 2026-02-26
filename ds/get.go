package ds

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

// getMultiLimit is the batch size for GetMulti.
const getMultiLimit = 1000

var (
	// getMultiHook testing hook.
	getMultiHook func(ctx context.Context, keys []*datastore.Key, vals interface{}) error
)

// GetMulti retrieves entities by keys.
// - Batches requests (limit 1000).
// - Uses cache transparently.
// - Falls back to datastore on cache miss/fail.
func (c *Client) GetMulti(ctx context.Context,
	keys []*datastore.Key, vals interface{}) error {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.GetMulti")
	defer span.End()
	v := reflect.ValueOf(vals)
	if err := checkKeysValues(keys, v); err != nil {
		return err
	}

	callCount := (len(keys)-1)/getMultiLimit + 1
	errs := make([]error, callCount)

	var wg sync.WaitGroup
	wg.Add(callCount)
	for i := 0; i < callCount; i++ {
		lo := i * getMultiLimit
		hi := (i + 1) * getMultiLimit
		if hi > len(keys) {
			hi = len(keys)
		}

		go func(i int, keys []*datastore.Key, vals reflect.Value) {
			errs[i] = c.getMulti(ctx, keys, vals)
			wg.Done()
		}(i, keys[lo:hi], v.Slice(lo, hi))
	}
	wg.Wait()

	if isErrorsNil(errs) {
		return nil
	}

	return groupErrors(errs, len(keys), getMultiLimit)
}

// Get retrieves entity by key into val (struct pointer).
func (c *Client) Get(ctx context.Context, key *datastore.Key, val interface{}) error {
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, "github.com/altlimit/dsorm.Get")
	defer span.End()
	// GetMulti catches nil interface; we need to catch nil ptr here.
	if val == nil {
		return datastore.ErrInvalidEntityType
	}

	keys := []*datastore.Key{key}
	vals := []interface{}{val}
	v := reflect.ValueOf(vals)
	if err := checkKeysValues(keys, v); err != nil {
		return err
	}

	err := c.getMulti(ctx, keys, v)
	if me, ok := err.(datastore.MultiError); ok {
		return me[0]
	}
	return err
}

type cacheState byte

const (
	miss cacheState = iota
	internalLock
	externalLock
	done
)

type cacheItem struct {
	key      *datastore.Key
	cacheKey string

	val reflect.Value
	err error

	item *Item

	state cacheState
}

// getMulti handles cache/datastore fetch logic.
func (c *Client) getMulti(ctx context.Context,
	keys []*datastore.Key, vals reflect.Value) error {

	if c.cacher != nil {
		num := len(keys)
		cacheItems := make([]cacheItem, num)
		for i, key := range keys {
			cacheItems[i].key = key
			cacheItems[i].cacheKey = createCacheKey(c.cachePrefix, key)
			cacheItems[i].val = vals.Index(i)
			cacheItems[i].state = miss
		}

		c.loadCache(ctx, cacheItems)
		if err := cacheStatsByKind(ctx, cacheItems); err != nil {
			c.onError(ctx, errors.Wrapf(err, "dsorm:getMulti cacheStatsByKind"))
		}

		c.lockCache(ctx, cacheItems)

		if err := c.loadDatastore(ctx, cacheItems, vals.Type()); err != nil {
			return err
		}

		c.saveCache(ctx, cacheItems)

		me, errsNil := make(datastore.MultiError, len(cacheItems)), true
		for i, cacheItem := range cacheItems {
			if cacheItem.err != nil {
				me[i] = cacheItem.err
				errsNil = false
			}
		}

		if errsNil {
			return nil
		}
		return me
	}
	return c.Client.GetMulti(ctx, keys, vals.Interface())
}

// loadCache attempts to fetch items from cache.
func (c *Client) loadCache(ctx context.Context, cacheItems []cacheItem) {

	cacheKeys := make([]string, len(cacheItems))
	for i, cacheItem := range cacheItems {
		cacheKeys[i] = cacheItem.cacheKey
	}

	items, err := c.cacher.GetMulti(ctx, cacheKeys)
	if err != nil {
		for i := range cacheItems {
			cacheItems[i].state = externalLock
		}
		c.onError(ctx, errors.Wrapf(err, "dsorm:loadCache GetMulti"))
		return
	}

	for i, cacheKey := range cacheKeys {
		if item, ok := items[cacheKey]; ok {
			switch item.Flags {
			case lockItem:
				cacheItems[i].state = externalLock
			case noneItem:
				cacheItems[i].state = done
				cacheItems[i].err = datastore.ErrNoSuchEntity
			case entityItem:
				pl := datastore.PropertyList{}
				if err := unmarshal(item.Value, &pl); err != nil {
					c.onError(ctx, errors.Wrapf(err, "dsorm:loadCache unmarshal"))
					cacheItems[i].state = externalLock
					break
				}
				if err := setValue(cacheItems[i].val, pl, cacheItems[i].key); err == nil {
					cacheItems[i].state = done
				} else {
					c.onError(ctx, errors.Wrapf(err, "dsorm:loadCache setValue"))
					cacheItems[i].state = externalLock
				}
			default:
				c.onError(ctx, errors.Errorf("dsorm:loadCache unknown item.Flags %d", item.Flags))
				cacheItems[i].state = externalLock
			}
		}
	}
}

// itemLock generates a random lock value.
func itemLock() []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rand.Uint32())
	return b
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (c *Client) lockCache(ctx context.Context, cacheItems []cacheItem) {

	lockItems := make([]*Item, 0, len(cacheItems))
	lockCacheKeys := make([]string, 0, len(cacheItems))
	for i, cacheItem := range cacheItems {
		if cacheItem.state == miss {

			item := &Item{
				Key:        cacheItem.cacheKey,
				Flags:      lockItem,
				Value:      itemLock(),
				Expiration: cacheLockTime,
			}
			cacheItems[i].item = item
			lockItems = append(lockItems, item)
			lockCacheKeys = append(lockCacheKeys, cacheItem.cacheKey)
		}
	}

	if len(lockItems) > 0 {
		// We don't care if there are errors here.
		if err := c.cacher.AddMulti(ctx, lockItems); err != nil {
			c.onError(ctx, errors.Wrap(err, "dsorm:lockCache AddMulti"))
		}

		// Get the items again so we can use CAS when updating the cache.
		items, err := c.cacher.GetMulti(ctx, lockCacheKeys)

		// Cache failed so forget about it and just use the datastore.
		if err != nil {
			for i, cacheItem := range cacheItems {
				if cacheItem.state == miss {
					cacheItems[i].state = externalLock
				}
			}
			c.onError(ctx, errors.Wrap(err, "dsorm:lockCache GetMulti"))
			return
		}

		// Cache worked so figure out what items we got.
		for i, cacheItem := range cacheItems {
			if cacheItem.state == miss {
				if item, ok := items[cacheItem.cacheKey]; ok {
					switch item.Flags {
					case lockItem:
						if bytes.Equal(item.Value, cacheItem.item.Value) {
							cacheItems[i].item = item
							cacheItems[i].state = internalLock
						} else {
							cacheItems[i].state = externalLock
						}
					case noneItem:
						cacheItems[i].state = done
						cacheItems[i].err = datastore.ErrNoSuchEntity
					case entityItem:
						pl := datastore.PropertyList{}
						if err := unmarshal(item.Value, &pl); err != nil {
							c.onError(ctx, errors.Wrap(err, "dsorm:lockCache unmarshal"))
							cacheItems[i].state = externalLock
							break
						}
						if err := setValue(cacheItems[i].val, pl, cacheItems[i].key); err == nil {
							cacheItems[i].state = done
						} else {
							c.onError(ctx, errors.Wrap(err, "dsorm:lockCache setValue"))
							cacheItems[i].state = externalLock
						}
					default:
						c.onError(ctx, errors.Errorf("dsorm:lockCache unknown item.Flags %d",
							item.Flags))
						cacheItems[i].state = externalLock
					}
				} else {
					// We just added a cache item but it now isn't available so
					// treat it as an extarnal lock.
					cacheItems[i].state = externalLock
				}
			}
		}
	}
}

func (c *Client) loadDatastore(ctx context.Context, cacheItems []cacheItem,
	valsType reflect.Type) error {

	keys := make([]*datastore.Key, 0, len(cacheItems))
	vals := make([]datastore.PropertyList, 0, len(cacheItems))
	cacheItemsIndex := make([]int, 0, len(cacheItems))

	for i, cacheItem := range cacheItems {
		switch cacheItem.state {
		case internalLock, externalLock:
			keys = append(keys, cacheItem.key)
			vals = append(vals, datastore.PropertyList{})
			cacheItemsIndex = append(cacheItemsIndex, i)
		}
	}

	if getMultiHook != nil {
		if err := getMultiHook(ctx, keys, vals); err != nil {
			return err
		}
	}

	if len(keys) == 0 {
		return nil
	}

	var me datastore.MultiError
	if err := c.Client.GetMulti(ctx, keys, vals); err == nil {
		me = make(datastore.MultiError, len(keys))
	} else if e, ok := err.(datastore.MultiError); ok {
		me = e
	} else {
		return err
	}

	for i, index := range cacheItemsIndex {
		switch me[i] {
		case nil:
			pl := vals[i]
			val := cacheItems[index].val

			if cacheItems[index].state == internalLock {
				cacheItems[index].item.Flags = entityItem
				cacheItems[index].item.Expiration = 0
				if data, err := marshal(pl); err == nil {
					cacheItems[index].item.Value = data
				} else {
					cacheItems[index].state = externalLock
					c.onError(ctx, errors.Wrap(err, "dsorm:loadDatastore marshal"))
				}
			}

			if err := setValue(val, pl, cacheItems[index].key); err != nil {
				cacheItems[index].err = err
			}
		case datastore.ErrNoSuchEntity:
			if cacheItems[index].state == internalLock {
				cacheItems[index].item.Flags = noneItem
				cacheItems[index].item.Expiration = 0
				cacheItems[index].item.Value = []byte{}
			}
			cacheItems[index].err = datastore.ErrNoSuchEntity
		default:
			cacheItems[index].state = externalLock
			cacheItems[index].err = me[i]
		}
	}
	return nil
}

func (c *Client) saveCache(ctx context.Context, cacheItems []cacheItem) {
	saveItems := make([]*Item, 0, len(cacheItems))
	for _, cacheItem := range cacheItems {
		if cacheItem.state == internalLock {
			saveItems = append(saveItems, cacheItem.item)
		}
	}

	if len(saveItems) == 0 {
		return
	}

	if err := c.cacher.CompareAndSwapMulti(ctx, saveItems); err != nil {
		c.onError(ctx, errors.Wrap(err, "dsorm:saveCache CompareAndSwapMulti"))
	}
}
