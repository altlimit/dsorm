package ds_test

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/altlimit/dsorm/cache/memory"
	credis "github.com/altlimit/dsorm/cache/redis"
	ds "github.com/altlimit/dsorm/ds"
	"github.com/valkey-io/valkey-go"
)

var (
	cachers = []cacherTestItem{
		cacherTestItem{ctx: context.Background(), cacher: memory.NewCache()},
	}
	cachersGuard  sync.Mutex
	errNotDefined = errors.New("undefined")
)

type cacherTestItem struct {
	ctx    context.Context
	cacher ds.Cache
}

// mockCache will use the cacher configured and provide hooks for each call
type mockCache struct {
	// override hooks for corresponding cacher function calls
	addMultiHook       func(ctx context.Context, items []*ds.Item) error
	compareAndSwapHook func(ctx context.Context, items []*ds.Item) error
	deleteMultiHook    func(ctx context.Context, keys []string) error
	getMultiHook       func(ctx context.Context, keys []string) (map[string]*ds.Item, error)
	setMultiHook       func(ctx context.Context, items []*ds.Item) error
	// Fallback in case corresponding hook is not defined
	cacher ds.Cache
}

func (m *mockCache) AddMulti(ctx context.Context, items []*ds.Item) error {
	if m.addMultiHook != nil {
		return m.addMultiHook(ctx, items)
	}
	if m.cacher != nil {
		return m.cacher.AddMulti(ctx, items)
	}
	return errNotDefined
}

func (m *mockCache) CompareAndSwapMulti(ctx context.Context, items []*ds.Item) error {
	if m.compareAndSwapHook != nil {
		return m.compareAndSwapHook(ctx, items)
	}
	if m.cacher != nil {
		return m.cacher.CompareAndSwapMulti(ctx, items)
	}
	return errNotDefined
}

func (m *mockCache) DeleteMulti(ctx context.Context, keys []string) error {
	if m.deleteMultiHook != nil {
		return m.deleteMultiHook(ctx, keys)
	}
	if m.cacher != nil {
		return m.cacher.DeleteMulti(ctx, keys)
	}
	return errNotDefined
}

func (m *mockCache) GetMulti(ctx context.Context, keys []string) (map[string]*ds.Item, error) {
	if m.getMultiHook != nil {
		return m.getMultiHook(ctx, keys)
	}
	if m.cacher != nil {
		return m.cacher.GetMulti(ctx, keys)
	}
	return nil, errNotDefined
}

func (m *mockCache) SetMulti(ctx context.Context, items []*ds.Item) error {
	if m.setMultiHook != nil {
		return m.setMultiHook(ctx, items)
	}
	if m.cacher != nil {
		return m.cacher.SetMulti(ctx, items)
	}
	return errNotDefined
}

func (m *mockCache) Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error) {
	if m.cacher != nil {
		return m.cacher.Increment(ctx, key, delta, expiration)
	}
	return 0, errNotDefined
}

func initRedis() {
	if testing.Short() {
		return
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// Flush cache using valkey client
	vClient, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{redisAddr}})
	if err != nil {
		return // skip if cannot connect
	}
	ctx := context.Background()
	vClient.Do(ctx, vClient.B().Flushdb().Build())
	vClient.Close()

	cacher, err := credis.NewCache(redisAddr)
	if err != nil {
		return // skip if cannot connect
	}
	cachersGuard.Lock()
	defer cachersGuard.Unlock()
	cachers = append(cachers, cacherTestItem{ctx: context.Background(), cacher: cacher})
}

func TestMain(m *testing.M) {
	flag.Parse()

	os.Setenv("DATASTORE_EMULATOR_HOST", "localhost:8081")
	os.Setenv("DATASTORE_PROJECT_ID", "app-test")

	initRedis()

	retCode := m.Run()

	os.Exit(retCode)
}

func NewClient(ctx context.Context, cacher ds.Cache, t *testing.T, logOKTest func(err error) bool) (*ds.Client, error) {
	onErrorFn := func(_ context.Context, err error) {
		if logOKTest != nil && logOKTest(err) {
			t.Logf("%+v", err)
		} else {
			t.Errorf("%+v", err)
		}
	}
	return ds.NewClient(ctx, cacher, ds.WithOnErrorFunc(onErrorFn))
}

func TestCaches(t *testing.T) {
	for _, item := range cachers {
		t.Run(fmt.Sprintf("cacher=%T", item.cacher), func(t *testing.T) {
			t.Run("TestPutGetDelete", PutGetDeleteTest(item.ctx, item.cacher))
			t.Run("TestInterfaces", InterfacesTest(item.ctx, item.cacher))
			t.Run("TestGetMultiNoSuchEntity", GetMultiNoSuchEntityTest(item.ctx, item.cacher))
			t.Run("TestGetMultiNoErrors", GetMultiNoErrorsTest(item.ctx, item.cacher))
			t.Run("TestGetMultiErrorMix", GetMultiErrorMixTest(item.ctx, item.cacher))
			t.Run("TestMultiCache", MultiCacheTest(item.ctx, item.cacher))
			t.Run("TestRunInTransaction", RunInTransactionTest(item.ctx, item.cacher))
		})
	}

}

func PutGetDeleteTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		type testEntity struct {
			IntVal int
		}

		// Check we set the cache, put datastore and delete cache.
		seq := make(chan string, 3)
		testCache := &mockCache{
			cacher: cacher,
			setMultiHook: func(ctx context.Context, items []*ds.Item) error {
				seq <- "cache.SetMulti"
				return cacher.SetMulti(ctx, items)
			},
			deleteMultiHook: func(ctx context.Context, keys []string) error {
				seq <- "cache.DeleteMulti"
				close(seq)
				return cacher.DeleteMulti(ctx, keys)
			},
		}
		ds.SetDatastorePutMultiHook(func() error {
			seq <- "datastore.PutMulti"
			return nil
		})

		nsdClient, err := NewClient(ctx, testCache, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		incompleteKey := datastore.IncompleteKey("PutGetDeleteTest", nil)
		key, err := nsdClient.Put(ctx, incompleteKey, &testEntity{43})
		if err != nil {
			t.Fatal(err)
		}

		ds.SetDatastorePutMultiHook(nil)
		testCache.setMultiHook = nil
		testCache.deleteMultiHook = nil

		if s := <-seq; s != "cache.SetMulti" {
			t.Fatal("cache.SetMulti not", s)
		}
		if s := <-seq; s != "datastore.PutMulti" {
			t.Fatal("datastore.PutMulti not", s)
		}
		if s := <-seq; s != "cache.DeleteMulti" {
			t.Fatal("cache.DeleteMulti not", s)
		}
		// Check chan is closed.
		<-seq

		if key.Incomplete() {
			t.Fatal("Key is incomplete")
		}

		te := &testEntity{}
		if err := nsdClient.Get(ctx, key, te); err != nil {
			t.Fatal(err)
		}

		if te.IntVal != 43 {
			t.Fatal("te.Val != 43", te.IntVal)
		}

		// Get from cache.
		te = &testEntity{}
		if err := nsdClient.Get(ctx, key, te); err != nil {
			t.Fatal(err)
		}

		if te.IntVal != 43 {
			t.Fatal("te.Val != 43", te.IntVal)
		}

		// Change value.
		if _, err := nsdClient.Put(ctx, key, &testEntity{64}); err != nil {
			t.Fatal(err)
		}

		// Get from cache.
		te = &testEntity{}
		if err := nsdClient.Get(ctx, key, te); err != nil {
			t.Fatal(err)
		}

		if te.IntVal != 64 {
			t.Fatal("te.Val != 64", te.IntVal)
		}

		if err := nsdClient.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}

		if err := nsdClient.Get(ctx, key, &testEntity{}); err != datastore.ErrNoSuchEntity {
			t.Fatal("expected datastore.ErrNoSuchEntity")
		}
	}
}

func InterfacesTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		incompleteKey := datastore.IncompleteKey("InterfacesTest", nil)
		incompleteKeys := []*datastore.Key{incompleteKey}
		entities := []interface{}{&testEntity{43}}
		keys, err := dsoClient.PutMulti(ctx, incompleteKeys, entities)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 1 {
			t.Fatal("len(keys) != 1")
		}

		if keys[0].Incomplete() {
			t.Fatal("Key is incomplete")
		}

		entities = []interface{}{&testEntity{}}
		if err := dsoClient.GetMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		if entities[0].(*testEntity).Val != 43 {
			t.Fatal("te.Val != 43")
		}

		// Get from cache.
		entities = []interface{}{&testEntity{}}
		if err := dsoClient.GetMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		if entities[0].(*testEntity).Val != 43 {
			t.Fatal("te.Val != 43")
		}

		// Change value.
		entities = []interface{}{&testEntity{64}}
		if _, err := dsoClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		// Get from ds with struct.
		entities = []interface{}{&testEntity{}}
		if err := dsoClient.GetMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}

		if entities[0].(*testEntity).Val != 64 {
			t.Fatal("te.Val != 64")
		}

		if err := dsoClient.DeleteMulti(ctx, keys); err != nil {
			t.Fatal(err)
		}

		entities = []interface{}{testEntity{}}
		err = dsoClient.GetMulti(ctx, keys, entities)
		if me, ok := err.(datastore.MultiError); ok {

			if len(me) != 1 {
				t.Fatal("expected 1 datastore.MultiError")
			}
			if me[0] != datastore.ErrNoSuchEntity {
				t.Fatal("expected datastore.ErrNoSuchEntity")
			}
		} else {
			t.Fatal("expected datastore.ErrNoSuchEntity", err)
		}
	}
}

func GetMultiNoSuchEntityTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		// Test no such entity.
		for _, count := range []int{999, 1000, 1001} {

			keys := []*datastore.Key{}
			entities := []*testEntity{}
			for i := 0; i < count; i++ {
				keys = append(keys,
					datastore.NameKey("GetMultiNoSuchEntityTest", strconv.Itoa(i), nil))
				entities = append(entities, &testEntity{})
			}

			err := dsoClient.GetMulti(ctx, keys, entities)
			if me, ok := err.(datastore.MultiError); ok {
				if len(me) != count {
					t.Fatal("multi error length incorrect")
				}
				for _, e := range me {
					if e != datastore.ErrNoSuchEntity {
						t.Fatal("expecting datastore.ErrNoSuchEntity but got", e)
					}
				}
			}
		}
	}
}

func GetMultiNoErrorsTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		for _, count := range []int{999, 1000, 1001} {

			// Create entities.
			keys := []*datastore.Key{}
			entities := []*testEntity{}
			for i := 0; i < count; i++ {
				key := datastore.NameKey("GetMultiNoErrorsTest", strconv.Itoa(i), nil)
				keys = append(keys, key)
				entities = append(entities, &testEntity{i})
			}

			// Save entities.
			if _, err := dsoClient.PutMulti(ctx, keys, entities); err != nil {
				t.Fatal(err)
			}

			respEntities := []testEntity{}
			for range keys {
				respEntities = append(respEntities, testEntity{})
			}

			if err := dsoClient.GetMulti(ctx, keys, respEntities); err != nil {
				t.Fatal(err)
			}

			// Check respEntities are in order.
			for i, re := range respEntities {
				if re.Val != entities[i].Val {
					t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
						entities[i].Val)
				}
			}
		}
	}
}

func GetMultiErrorMixTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		for _, count := range []int{999, 1000, 1001} {

			// Create entities.
			keys := []*datastore.Key{}
			entities := []testEntity{}
			for i := 0; i < count; i++ {
				key := datastore.NameKey("GetMultiErrorMixTest", strconv.Itoa(i), nil)
				keys = append(keys, key)
				entities = append(entities, testEntity{i})
			}

			// Save every other entity.
			putKeys := []*datastore.Key{}
			putEntities := []testEntity{}
			for i, key := range keys {
				if i%2 == 0 {
					putKeys = append(putKeys, key)
					putEntities = append(putEntities, entities[i])
				}
			}

			if _, err := dsoClient.PutMulti(ctx, putKeys, putEntities); err != nil {
				t.Fatal(err)
			}

			respEntities := make([]testEntity, len(keys))
			err := dsoClient.GetMulti(ctx, keys, respEntities)
			if err == nil {
				t.Fatal("should be errors")
			}

			if me, ok := err.(datastore.MultiError); !ok {
				t.Fatal("not datastore.MultiError")
			} else if len(me) != len(keys) {
				t.Fatal("incorrect length datastore.MultiError")
			}

			// Check respEntities are in order.
			for i, re := range respEntities {
				if i%2 == 0 {
					if re.Val != entities[i].Val {
						t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
							entities[i].Val)
					}
				} else if me, ok := err.(datastore.MultiError); ok {
					if me[i] != datastore.ErrNoSuchEntity {
						t.Fatalf("incorrect error %+v, index %d, of %d",
							me, i, count)
					}
				} else {
					t.Fatalf("incorrect error, index %d", i)
				}
			}
		}
	}
}

func MultiCacheTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}
		const entityCount = 88

		// Create entities.
		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := 0; i < entityCount; i++ {
			key := datastore.NameKey("MultiCacheTest", strconv.Itoa(i), nil)
			keys = append(keys, key)
			entities = append(entities, testEntity{i})
		}

		// Save every other entity.
		putKeys := []*datastore.Key{}
		putEntities := []testEntity{}
		for i, key := range keys {
			if i%2 == 0 {
				putKeys = append(putKeys, key)
				putEntities = append(putEntities, entities[i])
			}
		}
		if keys, err := dsoClient.PutMulti(ctx, putKeys, putEntities); err != nil {
			t.Fatal(err)
		} else if len(keys) != len(putKeys) {
			t.Fatal("incorrect key len")
		}

		// Get from ds.
		respEntities := make([]testEntity, len(keys))
		err = dsoClient.GetMulti(ctx, keys, respEntities)
		if err == nil {
			t.Fatal("should be errors")
		}

		me, ok := err.(datastore.MultiError)
		if !ok {
			t.Fatalf("not an datastore.MultiError: %T, %s", err, err)
		}

		// Check respEntities are in order.
		for i, re := range respEntities {
			if i%2 == 0 {
				if re.Val != entities[i].Val {
					t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
						entities[i].Val)
				}
				if me[i] != nil {
					t.Fatalf("should be nil error: %s", me[i])
				}
			} else {
				if re.Val != 0 {
					t.Fatal("entity not zeroed")
				}
				if me[i] != datastore.ErrNoSuchEntity {
					t.Fatalf("incorrect error %+v, index %d, of %d",
						me, i, entityCount)
				}
			}
		}

		// Get from local cache.
		respEntities = make([]testEntity, len(keys))
		err = dsoClient.GetMulti(ctx, keys, respEntities)
		if err == nil {
			t.Fatal("should be errors")
		}

		me, ok = err.(datastore.MultiError)
		if !ok {
			t.Fatalf("not an datastore.MultiError: %s", err)
		}

		// Check respEntities are in order.
		for i, re := range respEntities {
			if i%2 == 0 {
				if re.Val != entities[i].Val {
					t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
						entities[i].Val)
				}
				if me[i] != nil {
					t.Fatal("should be nil error")
				}
			} else {
				if re.Val != 0 {
					t.Fatal("entity not zeroed")
				}
				if me[i] != datastore.ErrNoSuchEntity {
					t.Fatalf("incorrect error %+v, index %d, of %d",
						me, i, entityCount)
				}
			}
		}

		// Get from cache.
		respEntities = make([]testEntity, len(keys))
		err = dsoClient.GetMulti(ctx, keys, respEntities)
		if err == nil {
			t.Fatal("should be errors")
		}

		me, ok = err.(datastore.MultiError)
		if !ok {
			t.Fatalf("not a datastore.MultiError: %s", me)
		}

		// Check respEntities are in order.
		for i, re := range respEntities {
			if i%2 == 0 {
				if re.Val != entities[i].Val {
					t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
						entities[i].Val)
				}
				if me[i] != nil {
					t.Fatal("should be nil error")
				}
			} else {
				if re.Val != 0 {
					t.Fatal("entity not zeroed")
				}
				if me[i] != datastore.ErrNoSuchEntity {
					t.Fatalf("incorrect error %+v, index %d, of %d",
						me, i, entityCount)
				}
			}
		}
	}
}

func RunInTransactionTest(ctx context.Context, cacher ds.Cache) func(t *testing.T) {
	return func(t *testing.T) {
		dsoClient, err := NewClient(ctx, cacher, t, nil)
		if err != nil {
			t.Fatal(err)
		}

		type testEntity struct {
			Val int
		}

		key := datastore.IDKey("RunInTransactionTest", 3, nil)
		keys := []*datastore.Key{key}
		entity := testEntity{42}
		entities := []testEntity{entity}

		if _, err = dsoClient.PutMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}
		var putKey *datastore.PendingKey
		commit, err := dsoClient.RunInTransaction(ctx, func(tx *ds.Transaction) error {
			entities := make([]testEntity, 1)
			if err := tx.GetMulti(keys, entities); err != nil {
				t.Fatal(err)
			}
			entity := entities[0]

			if entity.Val != 42 {
				t.Fatalf("entity.Val != 42: %d", entity.Val)
			}

			entities[0].Val = 43

			putKeys, err := tx.PutMulti(keys, entities)
			if err != nil {
				t.Fatal(err)
			} else if len(putKeys) != 1 {
				t.Fatal("putKeys should be len 1")
			}
			putKey = putKeys[0]
			return nil

		})

		if err != nil {
			t.Fatal(err)
		}

		if !commit.Key(putKey).Equal(key) {
			t.Fatal("keys not equal")
		}

		entities = make([]testEntity, 1)
		if err := dsoClient.GetMulti(ctx, keys, entities); err != nil {
			t.Fatal(err)
		}
		entity = entities[0]
		if entity.Val != 43 {
			t.Fatalf("entity.Val != 43: %d", entity.Val)
		}
	}
}

func TestMarshalUnmarshalPropertyList(t *testing.T) {

	type Int struct {
		Int int64
	}

	timeVal := time.Now()
	timeProp := datastore.Property{Name: "Time",
		Value: timeVal, NoIndex: false}

	keyVal := datastore.NameKey("Entity", "stringID", nil)
	keyProp := datastore.Property{Name: "Key",
		Value: keyVal, NoIndex: false}

	geoPointVal := datastore.GeoPoint{Lat: 1, Lng: 2}
	geoPointProp := datastore.Property{Name: "GeoPoint",
		Value: geoPointVal, NoIndex: false}

	entityVal := &datastore.Entity{
		Properties: []datastore.Property{
			datastore.Property{
				Name:    "Int",
				Value:   int64(6),
				NoIndex: false,
			},
		},
	}
	entityProp := datastore.Property{Name: "Entity",
		Value: entityVal, NoIndex: false}

	pl := datastore.PropertyList{
		timeProp,
		keyProp,
		geoPointProp,
		entityProp,
	}
	data, err := ds.MarshalPropertyList(pl)
	if err != nil {
		t.Fatal(err)
	}

	testEntity := &struct {
		Time     time.Time
		Key      *datastore.Key
		GeoPoint datastore.GeoPoint
		Entity   Int
	}{}

	pl = datastore.PropertyList{}
	if err := ds.UnmarshalPropertyList(data, &pl); err != nil {
		t.Fatal(err)
	}
	if err := ds.SetValue(reflect.ValueOf(testEntity), pl, keyVal); err != nil {
		t.Fatal(err)
	}

	if !testEntity.Time.Equal(timeVal) {
		t.Fatal("timeVal not equal")
	}

	if !testEntity.Key.Equal(keyVal) {
		t.Fatal("keyVal not equal")
	}

	if !reflect.DeepEqual(testEntity.GeoPoint, geoPointVal) {
		t.Fatal("geoPointVal not equal")
	}

	if !reflect.DeepEqual(testEntity.Entity.Int, entityVal.Properties[0].Value) {
		t.Fatal("entityVal not equal")
	}
}

func TestMartialPropertyListError(t *testing.T) {

	type testEntity struct {
		IntVal int
	}

	pl := datastore.PropertyList{
		datastore.Property{Name: "Prop", Value: &testEntity{3}, NoIndex: false},
	}
	if _, err := ds.MarshalPropertyList(pl); err == nil {
		t.Fatal("expected error")
	}
}

func randHexString(length int) string {
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = byte(rand.Int())
	}
	return hex.EncodeToString(bytes)
}

func TestCreateCacheKey(t *testing.T) {
	// Check keys are hashed over ds.CacheMaxKeySize.
	maxKeySize := ds.CacheMaxKeySize
	key := datastore.NameKey("TestCreateCacheKey",
		randHexString(maxKeySize+10), nil)

	cacheKey := ds.CreateCacheKey(key)
	if len(cacheKey) > maxKeySize {
		t.Fatal("incorrect cache key size")
	}
}

func TestNilCache(t *testing.T) {
	ctx := context.Background()
	client, err := ds.NewClient(ctx, nil)

	if err != nil {
		t.Fatalf("could not get client due to error: %v", err)
	}

	type testEntity struct {
		IntVal int
	}

	key := datastore.NameKey("nilcacher", "test-ent", nil)
	ent := testEntity{32}

	if _, err = client.Put(ctx, key, &ent); err != nil {
		t.Fatalf("could not put: %v", err)
	}

	if _, err = client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		if _, err := tx.Put(key, &testEntity{65}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("could not execute transaction: %v", err)
	}

	ent.IntVal = 99

	if _, err = client.Mutate(ctx, ds.NewUpdate(key, &ent)); err != nil {
		t.Fatalf("could not mutate: %v", err)
	}

	getEnt := &testEntity{}

	if err = client.Get(ctx, key, getEnt); err != nil {
		t.Fatalf("could not get: %v", err)
	} else if getEnt.IntVal != ent.IntVal {
		t.Fatalf("got `%d`, wanted `%d`", getEnt.IntVal, ent.IntVal)
	}

	if err = client.Delete(ctx, key); err != nil {
		t.Fatalf("could not delete: %v", err)
	}

}
