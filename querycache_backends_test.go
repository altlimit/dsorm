package dsorm_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/altlimit/dsorm"
	"github.com/altlimit/dsorm/cache/cloudflare"
	"github.com/altlimit/dsorm/cache/memory"
	"github.com/altlimit/dsorm/cache/redis"
	"github.com/altlimit/dsorm/ds"
	"github.com/altlimit/dsorm/ds/local"
)

type QCCounterModel struct {
	dsorm.Base
	ID    string `model:"id"`
	NS    string `model:"ns"`
	Value string `datastore:"value"`
}

// TestQueryCacheFreshNamespace writes and queries a kind in a namespace whose
// generation counter is first created by a write, against each cache backend.
// Every query must see every prior write. Backends store counters in their own
// layout (Redis INCRBY text, the Durable Object's int64), so GetMulti has to
// return them in a form the version reader understands; when it did not, the
// counter decoded to a constant and queries served stale results.
func TestQueryCacheFreshNamespace(t *testing.T) {
	backends := map[string]func(t *testing.T) ds.Cache{
		"memory": func(t *testing.T) ds.Cache { return memory.NewCache() },
		"cloudflare": func(t *testing.T) ds.Cache {
			srv := httptest.NewServer(cloudflare.NewMockHandler())
			t.Cleanup(srv.Close)
			c, err := cloudflare.NewCache(srv.URL)
			if err != nil {
				t.Fatalf("cloudflare.NewCache: %v", err)
			}
			return c
		},
		"redis": func(t *testing.T) ds.Cache {
			if testing.Short() {
				t.Skip("skipping redis in short mode")
			}
			addr := os.Getenv("REDIS_ADDR")
			if addr == "" {
				addr = "localhost:6379"
			}
			c, err := redis.NewCache(addr)
			if err != nil {
				t.Skipf("redis unavailable: %v", err)
			}
			return c
		},
	}

	for name, newCache := range backends {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := local.NewStore(t.TempDir())
			defer store.Close()

			client, err := dsorm.New(ctx,
				dsorm.WithStore(store),
				dsorm.WithCache(newCache(t)),
				dsorm.WithQueryCache(time.Minute),
				dsorm.WithOnErrorFunc(func(_ context.Context, err error) {
					t.Errorf("cache error: %v", err)
				}),
			)
			if err != nil {
				t.Fatalf("dsorm.New: %v", err)
			}

			ns := fmt.Sprintf("fresh-%d", time.Now().UnixNano())
			// Enough rounds for a counter that starts at 1 to pass through
			// every short decimal length.
			for i := 1; i <= 40; i++ {
				m := &QCCounterModel{ID: fmt.Sprintf("m%02d", i), NS: ns, Value: fmt.Sprint(i)}
				if err := client.Put(ctx, m); err != nil {
					t.Fatalf("Put %d: %v", i, err)
				}
				q := dsorm.NewQuery("QCCounterModel").Namespace(ns)
				got, _, err := dsorm.Query[*QCCounterModel](ctx, client, q, "")
				if err != nil {
					t.Fatalf("Query after put %d: %v", i, err)
				}
				if len(got) != i {
					t.Fatalf("Query after put %d returned %d results; served a stale cached list", i, len(got))
				}
			}
		})
	}
}
