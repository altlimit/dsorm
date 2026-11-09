package redis_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/altlimit/dsorm/cache/redis"
	dso "github.com/altlimit/dsorm/ds"
	"github.com/valkey-io/valkey-go"
)

var (
	redisAddr  = os.Getenv("REDIS_ADDR")
	goodClient dso.Cache
)

func TestRedisCache(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping redis tests...")
		return
	}

	// Setup Redis Connection
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	client, err := redis.NewCache(redisAddr)
	if err != nil {
		t.Fatalf("cannot test redis, error connecting: %v", err)
	}
	goodClient = client

	t.Run("TestNewCache", NewCacheTest())
	t.Run("TestIncrement", IncrementTest())
	t.Run("TestIncrementReadBack", IncrementReadBackTest())
	t.Run("TestDeleteMissingKey", DeleteMissingKeyTest())
}

func DeleteMissingKeyTest() func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		key := "delete-missing-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		if err := goodClient.DeleteMulti(ctx, []string{key}); !errors.Is(err, dso.ErrCacheMiss) {
			t.Errorf("DeleteMulti(missing) = %v, want ds.ErrCacheMiss", err)
		}
	}
}

func NewCacheTest() func(t *testing.T) {
	ctx := context.Background()
	type args struct {
		addr string
	}
	var tests = []struct {
		name      string
		in        args
		expectErr bool
	}{
		{
			"Good Client",
			args{
				addr: redisAddr,
			},
			false,
		},
		{
			"Bad Address",
			args{
				addr: "badaddress:999",
			},
			true,
		},
	}
	return func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				_ = ctx
				if _, err := redis.NewCache(tt.in.addr); (err != nil) != tt.expectErr {
					t.Errorf("expectErr = %v, err = %v", tt.expectErr, err)
				}
			})
		}
	}
}

func IncrementTest() func(t *testing.T) {
	return func(t *testing.T) {
		if goodClient == nil {
			t.Skip("no redis client")
		}
		ctx := context.Background()

		// Flush the test key
		vClient, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{redisAddr}})
		if err != nil {
			t.Fatalf("failed to create client for cleanup: %v", err)
		}
		defer vClient.Close()
		vClient.Do(ctx, vClient.B().Del().Key("test:incr").Build())

		// Test increment from zero
		val, err := goodClient.Increment(ctx, "test:incr", 1, time.Minute)
		if err != nil {
			t.Fatalf("Increment failed: %v", err)
		}
		if val != 1 {
			t.Errorf("expected 1, got %d", val)
		}

		// Test increment again
		val, err = goodClient.Increment(ctx, "test:incr", 5, time.Minute)
		if err != nil {
			t.Fatalf("Increment failed: %v", err)
		}
		if val != 6 {
			t.Errorf("expected 6, got %d", val)
		}

		// Test negative delta
		val, err = goodClient.Increment(ctx, "test:incr", -2, time.Minute)
		if err != nil {
			t.Fatalf("Increment failed: %v", err)
		}
		if val != 4 {
			t.Errorf("expected 4, got %d", val)
		}

		// Cleanup
		vClient.Do(ctx, vClient.B().Del().Key("test:incr").Build())
	}
}

// IncrementReadBackTest checks that a counter written by Increment reads back
// through GetMulti as its decimal text, whatever its length. Counters have no
// flags header, so stripping one used to error on short values and truncate
// longer ones.
func IncrementReadBackTest() func(t *testing.T) {
	return func(t *testing.T) {
		if goodClient == nil {
			t.Skip("no redis client")
		}
		ctx := context.Background()
		const key = "test:incr:readback"

		for _, n := range []int64{1, 1284, 1793414972625, -7} {
			_ = goodClient.DeleteMulti(ctx, []string{key})
			if _, err := goodClient.Increment(ctx, key, n, time.Minute); err != nil {
				t.Fatalf("Increment %d failed: %v", n, err)
			}
			items, err := goodClient.GetMulti(ctx, []string{key})
			if err != nil {
				t.Fatalf("GetMulti after Increment %d failed: %v", n, err)
			}
			it := items[key]
			if it == nil {
				t.Fatalf("GetMulti after Increment %d: missing item", n)
			}
			if want := strconv.FormatInt(n, 10); string(it.Value) != want || it.Flags != 0 {
				t.Errorf("read back flags=%d value=%q; want flags=0 value=%q", it.Flags, it.Value, want)
			}
		}
		_ = goodClient.DeleteMulti(ctx, []string{key})
	}
}
