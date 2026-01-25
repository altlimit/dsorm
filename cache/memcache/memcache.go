package memcache

import (
	"context"

	"google.golang.org/appengine/v2"
	"google.golang.org/appengine/v2/memcache"

	dsorm "github.com/altlimit/dsorm/ds"
)

type backend struct{}

// NewCache will return a dsorm.Cache backed by AppEngine's memcache.
func NewCache() dsorm.Cache {
	return &backend{}
}

func (m *backend) AddMulti(ctx context.Context, items []*dsorm.Item) error {
	return convertToDSOMultiError(memcache.AddMulti(ctx, convertToMemcacheItems(items)))
}

func (m *backend) CompareAndSwapMulti(ctx context.Context, items []*dsorm.Item) error {
	return convertToDSOMultiError(memcache.CompareAndSwapMulti(ctx, convertToMemcacheItems(items)))
}

func (m *backend) DeleteMulti(ctx context.Context, keys []string) error {
	return convertToDSOMultiError(memcache.DeleteMulti(ctx, keys))
}

func (m *backend) GetMulti(ctx context.Context, keys []string) (map[string]*dsorm.Item, error) {
	items, err := memcache.GetMulti(ctx, keys)
	if err != nil {
		return nil, convertToDSOMultiError(err)
	}
	return convertFromMemcacheItems(items), nil
}

func (m *backend) SetMulti(ctx context.Context, items []*dsorm.Item) error {
	return memcache.SetMulti(ctx, convertToMemcacheItems(items))
}

func convertToMemcacheItems(items []*dsorm.Item) []*memcache.Item {
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

func convertFromMemcacheItems(items map[string]*memcache.Item) map[string]*dsorm.Item {
	newItems := make(map[string]*dsorm.Item)
	for key, item := range items {
		newItems[key] = &dsorm.Item{
			Expiration: item.Expiration,
			Flags:      item.Flags,
			Key:        item.Key,
			Value:      item.Value,
		}
		newItems[key].SetCASInfo(item)
	}
	return newItems
}

func convertToDSOMultiError(err error) error {
	if ame, ok := err.(appengine.MultiError); ok {
		me := make(dsorm.MultiError, len(ame))
		for i, aerr := range ame {
			switch aerr {
			case memcache.ErrNotStored:
				me[i] = dsorm.ErrNotStored
			case memcache.ErrCacheMiss:
				me[i] = dsorm.ErrCacheMiss
			case memcache.ErrCASConflict:
				me[i] = dsorm.ErrCASConflict
			default:
				me[i] = aerr
			}
		}
		return me
	}
	return err
}
