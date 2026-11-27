package ds_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm/cache/memory"
	ds "github.com/altlimit/dsorm/ds"
)

// A write locks each entity's cache entry and deletes the lock when it is done.
// Finding the lock already gone is normal: a concurrent write to the same entity
// released it first, or it expired or was evicted. That must not be reported as
// an internal error. A real failure to release must be.
func TestPutLockReleaseMissIsNotAnError(t *testing.T) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, "")
	if err != nil {
		t.Fatalf("datastore client: %v", err)
	}

	type entity struct{ V string }
	cases := map[string]struct {
		release    error
		wantReport bool
	}{
		"the lock was already gone":             {fmt.Errorf("redis: expected to remove 1 keys, but only removed 0: %w", ds.ErrCacheMiss), false},
		"the lock was already gone, per-key":    {ds.MultiError{ds.ErrCacheMiss}, false},
		"the release failed":                    {errors.New("connection reset"), true},
		"one lock gone, another release failed": {ds.MultiError{ds.ErrCacheMiss, errors.New("connection reset")}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cache := &mockCache{
				cacher:          memory.NewCache(),
				deleteMultiHook: func(context.Context, []string) error { return tc.release },
			}
			var mu sync.Mutex
			var reported []error
			c, err := ds.NewClient(ctx, cache, ds.WithDatastoreClient(dsClient),
				ds.WithOnErrorFunc(func(_ context.Context, err error) {
					mu.Lock()
					defer mu.Unlock()
					reported = append(reported, err)
				}))
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			key := datastore.NameKey("LockReleaseTest", strings.ReplaceAll(name, " ", "-"), nil)
			if _, err := c.Put(ctx, key, &entity{V: "x"}); err != nil {
				t.Fatalf("put: %v", err)
			}

			got := false
			for _, e := range reported {
				if strings.Contains(e.Error(), "putMulti cache.DeleteMulti") {
					got = true
				}
			}
			if got != tc.wantReport {
				t.Errorf("lock release reported = %v, want %v (reported: %v)", got, tc.wantReport, reported)
			}
		})
	}
}
