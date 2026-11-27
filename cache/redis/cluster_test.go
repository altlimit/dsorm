package redis_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/altlimit/dsorm/cache/redis"
	dso "github.com/altlimit/dsorm/ds"
	"github.com/valkey-io/valkey-go"
)

// A cluster (Memorystore for Redis Cluster, Valkey cluster) is detected by the
// client on connect. In cluster mode the client refuses any one command whose
// keys hash to different slots, so a multi-key read or delete across unrelated
// keys must never be sent as a single MGET or DEL.
//
// Runs when REDIS_CLUSTER_ADDR points at a cluster node, for example a
// single-node cluster:
//
//	docker run -d -p 127.0.0.1:17001:17001 redis:7-alpine redis-server --port 17001 \
//	  --cluster-enabled yes --cluster-announce-ip 127.0.0.1 --save "" --appendonly no
//	docker exec <id> redis-cli -p 17001 cluster addslotsrange 0 16383
func TestRedisClusterCache(t *testing.T) {
	addr := os.Getenv("REDIS_CLUSTER_ADDR")
	if addr == "" {
		t.Skip("REDIS_CLUSTER_ADDR not set; skipping the cluster-mode tests")
	}
	ctx := context.Background()

	cache, err := redis.NewCache(addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := cache.(interface{ Close() }); ok {
			c.Close()
		}
	})

	// The premise: this really is a cluster client, or the test proves nothing.
	probe, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		t.Fatalf("probe client: %v", err)
	}
	defer probe.Close()
	if probe.Mode() != valkey.ClientModeCluster {
		t.Fatalf("REDIS_CLUSTER_ADDR is not a cluster node (client mode %v)", probe.Mode())
	}

	run := strconv.FormatInt(time.Now().UnixNano(), 10)
	keys := make([]string, 40) // forty unrelated keys: certain to span many slots
	items := make([]*dso.Item, len(keys))
	for i := range keys {
		keys[i] = fmt.Sprintf("cluster-%s-%d", run, i)
		items[i] = &dso.Item{Key: keys[i], Value: []byte("v" + strconv.Itoa(i)), Flags: 7}
	}
	if err := cache.SetMulti(ctx, items); err != nil {
		t.Fatalf("SetMulti: %v", err)
	}

	t.Run("GetMulti across slots", func(t *testing.T) {
		missing := "cluster-" + run + "-missing"
		got, err := cache.GetMulti(ctx, append(append([]string{}, keys...), missing))
		if err != nil {
			t.Fatalf("GetMulti: %v", err)
		}
		if len(got) != len(keys) {
			t.Fatalf("got %d items, want %d (the missing key is simply absent)", len(got), len(keys))
		}
		for i, k := range keys {
			it := got[k]
			if it == nil || string(it.Value) != "v"+strconv.Itoa(i) || it.Flags != 7 {
				t.Fatalf("item %s = %+v", k, it)
			}
		}
		if _, ok := got[missing]; ok {
			t.Error("a missing key came back")
		}
	})

	t.Run("CAS after a multi-slot read", func(t *testing.T) {
		got, err := cache.GetMulti(ctx, keys[:3])
		if err != nil {
			t.Fatalf("GetMulti: %v", err)
		}
		swap := make([]*dso.Item, 0, 3)
		for _, k := range keys[:3] {
			it := got[k]
			it.Value = []byte("swapped")
			swap = append(swap, it)
		}
		if err := cache.CompareAndSwapMulti(ctx, swap); err != nil {
			t.Fatalf("CompareAndSwapMulti: %v", err)
		}
	})

	t.Run("DeleteMulti across slots", func(t *testing.T) {
		if err := cache.DeleteMulti(ctx, keys[:20]); err != nil {
			t.Fatalf("DeleteMulti: %v", err)
		}
		got, err := cache.GetMulti(ctx, keys[:20])
		if err != nil {
			t.Fatalf("GetMulti after delete: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("%d deleted keys still readable", len(got))
		}
	})

	t.Run("DeleteMulti with a missing key reports a cache miss", func(t *testing.T) {
		err := cache.DeleteMulti(ctx, append([]string{"cluster-" + run + "-never"}, keys[20:25]...))
		if !errors.Is(err, dso.ErrCacheMiss) {
			t.Fatalf("DeleteMulti = %v, want ds.ErrCacheMiss", err)
		}
		// The keys that did exist are gone all the same.
		got, err := cache.GetMulti(ctx, keys[20:25])
		if err != nil {
			t.Fatalf("GetMulti: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("%d keys survived a delete that also named a missing one", len(got))
		}
	})

	t.Run("Flush clears every node", func(t *testing.T) {
		if err := cache.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		got, err := cache.GetMulti(ctx, keys[25:])
		if err != nil {
			t.Fatalf("GetMulti after flush: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("%d keys survived Flush", len(got))
		}
	})
}
