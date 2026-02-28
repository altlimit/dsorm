package memcache

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/appengine/v2"
	"google.golang.org/appengine/v2/memcache"

	"github.com/altlimit/dsorm/ds"
)

type backend struct{}

// NewCache will return a ds.Cache backed by AppEngine's memcache.
func NewCache() ds.Cache {
	return &backend{}
}

func (m *backend) AddMulti(ctx context.Context, items []*ds.Item) error {
	return convertToDSMultiError(memcache.AddMulti(ctx, convertToMemcacheItems(items)))
}

func (m *backend) CompareAndSwapMulti(ctx context.Context, items []*ds.Item) error {
	return convertToDSMultiError(memcache.CompareAndSwapMulti(ctx, convertToMemcacheItems(items)))
}

func (m *backend) DeleteMulti(ctx context.Context, keys []string) error {
	return convertToDSMultiError(memcache.DeleteMulti(ctx, keys))
}

func (m *backend) GetMulti(ctx context.Context, keys []string) (map[string]*ds.Item, error) {
	items, err := memcache.GetMulti(ctx, keys)
	if err != nil {
		return nil, convertToDSMultiError(err)
	}
	return convertFromMemcacheItems(items), nil
}

func (m *backend) SetMulti(ctx context.Context, items []*ds.Item) error {
	return memcache.SetMulti(ctx, convertToMemcacheItems(items))
}

func (m *backend) Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error) {
	newVal, err := memcache.Increment(ctx, key, delta, 0)
	if err != nil {
		return 0, err
	}
	// If this was the initial creation (newVal equals delta) and an expiration
	// is requested, set the key with the TTL so it auto-expires.
	if expiration > 0 && newVal == uint64(delta) {
		if err := memcache.Set(ctx, &memcache.Item{
			Key:        key,
			Value:      []byte(fmt.Sprintf("%d", newVal)),
			Expiration: expiration,
		}); err != nil {
			return 0, err
		}
	}
	return int64(newVal), nil
}

func convertToMemcacheItems(items []*ds.Item) []*memcache.Item {
	newItems := make([]*memcache.Item, len(items))
	for i, item := range items {
		if memcacheItem, ok := item.GetCASInfo().(*memcache.Item); ok {
			memcacheItem.Value = item.Value
			memcacheItem.Flags = item.Flags
			memcacheItem.Expiration = item.Expiration
			memcacheItem.Key = item.Key
			newItems[i] = memcacheItem
		} else {
			newItems[i] = &memcache.Item{
				Expiration: item.Expiration,
				Flags:      item.Flags,
				Key:        item.Key,
				Value:      item.Value,
			}
		}
	}
	return newItems
}

func convertFromMemcacheItems(items map[string]*memcache.Item) map[string]*ds.Item {
	newItems := make(map[string]*ds.Item)
	for key, item := range items {
		newItems[key] = &ds.Item{
			Expiration: item.Expiration,
			Flags:      item.Flags,
			Key:        item.Key,
			Value:      item.Value,
		}
		newItems[key].SetCASInfo(item)
	}
	return newItems
}

func convertToDSMultiError(err error) error {
	if ame, ok := err.(appengine.MultiError); ok {
		me := make(ds.MultiError, len(ame))
		for i, aerr := range ame {
			switch aerr {
			case memcache.ErrNotStored:
				me[i] = ds.ErrNotStored
			case memcache.ErrCacheMiss:
				me[i] = ds.ErrCacheMiss
			case memcache.ErrCASConflict:
				me[i] = ds.ErrCASConflict
			default:
				me[i] = aerr
			}
		}
		return me
	}
	return err
}
