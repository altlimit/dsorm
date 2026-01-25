package ds_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"cloud.google.com/go/datastore"

	ds "github.com/altlimit/dsorm/ds"
)

func TestDeleteSuite(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("DeleteMultiTest", DeleteMultiTest(item.ctx, item.cacher))
			t.Run("DeleteNilKeyTest", DeleteNilKeyTest(item.ctx, item.cacher))
			t.Run("DeleteIncompleteKeyTest", DeleteIncompleteKeyTest(item.ctx, item.cacher))
			t.Run("DeleteCacheFailTest", DeleteCacheFailTest(item.ctx, item.cacher))
			t.Run("DeleteInTransactionTest", DeleteInTransactionTest(item.ctx, item.cacher))
		})
	}
}

func DeleteMultiTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type TestEntity struct {
			Value int
		}

		for _, count := range []int{499, 500, 501} {
			keys := make([]*datastore.Key, count)
			entities := make([]TestEntity, count)

			for i := range keys {
				keys[i] = datastore.NameKey("TestEntity", strconv.Itoa(i), nil)
				entities[i] = TestEntity{i}
			}

			if _, err := dsoClient.PutMulti(ctx, keys, entities); err != nil {
				t.Fatal(err)
			}

			// Prime cache.
			entities = make([]TestEntity, count)
			if err := dsoClient.GetMulti(ctx, keys, entities); err != nil {
				t.Fatal(err)
			}

			if err = dsoClient.DeleteMulti(ctx, keys); err != nil {
				t.Fatal(err)
			}

			err = dsoClient.GetMulti(ctx, keys, make([]TestEntity, count))
			if err == nil {
				t.Fatal("expect error")
			}

			me, ok := err.(datastore.MultiError)
			if !ok {
				t.Fatal("should be MultiError")
			}

			for _, e := range me {
				if e != datastore.ErrNoSuchEntity {
					t.Fatal("expected ErrNoSuchEntity")
				}
			}
		}
	}
}

func DeleteNilKeyTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := dsoClient.Delete(ctx, nil); err != datastore.ErrInvalidKey {
			t.Fatal("expected nil key error")
		}
	}
}

func DeleteIncompleteKeyTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		key := datastore.IncompleteKey("Entity", nil)

		if err := dsoClient.Delete(ctx, key); err == nil {
			t.Fatal("expected invalid key error")
		}
	}
}

func DeleteCacheFailTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		testCache := &mockCache{
			cacher: cacher,
		}
		dsoClient, err := NewClient(ctx, testCache, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		key := datastore.IDKey("Entity", 1, nil)
		keys := []*datastore.Key{key}
		entities := make([]testEntity, 1)
		entities[0].Val = 43

		if _, err := dsoClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		testCache.setMultiHook = func(_ context.Context, _ []*ds.Item) error {
			return errors.New("expected error")
		}

		if err := dsoClient.DeleteMulti(ctx, keys); err == nil {
			t.Fatal("expected DeleteMulti error")
		}
	}
}

func DeleteInTransactionTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		key := datastore.IDKey("TestEntity", 1, nil)
		if _, err := dsoClient.Put(ctx, key, &testEntity{2}); err != nil {
			t.Fatal(err)
		}

		// Prime cache.
		if err := dsoClient.Get(ctx, key, &testEntity{}); err != nil {
			t.Fatal(err)
		}

		if _, err = dsoClient.RunInTransaction(ctx, func(tx *ds.Transaction) error {
			return tx.DeleteMulti([]*datastore.Key{key})
		}); err != nil {
			t.Fatal(err)
		}

		if err = dsoClient.Get(ctx, key, &testEntity{}); err == nil {
			t.Fatal("expected no entity")
		} else if err != datastore.ErrNoSuchEntity {
			t.Fatal(err)
		}
	}
}
