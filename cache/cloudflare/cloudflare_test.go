package cloudflare_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/altlimit/dsorm/cache/cloudflare"
	ds "github.com/altlimit/dsorm/ds"
)

func newTestCache(t *testing.T) ds.Cache {
	t.Helper()
	srv := httptest.NewServer(cloudflare.NewMockHandler())
	t.Cleanup(srv.Close)
	c, err := cloudflare.NewCache(srv.URL)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	return c
}

func TestNewCacheValidation(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"valid http", "http://cache.do", false},
		{"valid https", "https://example.com/base", false},
		{"trailing slash", "http://cache.do/", false},
		{"no scheme", "cache.do", true},
		{"empty", "", true},
		{"scheme only", "http://", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cloudflare.NewCache(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCache(%q) err = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}

func TestIncrement(t *testing.T) {
	c := newTestCache(t)
	ctx := context.Background()

	if got, err := c.Increment(ctx, "counter", 5, 0); err != nil || got != 5 {
		t.Fatalf("first incr = %d, %v; want 5, nil", got, err)
	}
	if got, err := c.Increment(ctx, "counter", 3, 0); err != nil || got != 8 {
		t.Fatalf("second incr = %d, %v; want 8, nil", got, err)
	}
	if got, err := c.Increment(ctx, "counter", -2, 0); err != nil || got != 6 {
		t.Fatalf("decrement = %d, %v; want 6, nil", got, err)
	}
}

func TestIncrementExpiry(t *testing.T) {
	c := newTestCache(t)
	ctx := context.Background()

	if _, err := c.Increment(ctx, "ttlcounter", 1, 50*time.Millisecond); err != nil {
		t.Fatalf("incr: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	// After expiry the counter resets, so it starts from delta again.
	if got, err := c.Increment(ctx, "ttlcounter", 10, 0); err != nil || got != 10 {
		t.Fatalf("post-expiry incr = %d, %v; want 10, nil", got, err)
	}
}

func TestTenantIsolation(t *testing.T) {
	c := newTestCache(t)
	base := context.Background()
	ctxA := ds.WithTenant(base, "tenant-a")
	ctxB := ds.WithTenant(base, "tenant-b")

	if err := c.SetMulti(ctxA, []*ds.Item{{Key: "shared", Value: []byte("a")}}); err != nil {
		t.Fatalf("set A: %v", err)
	}
	if err := c.SetMulti(ctxB, []*ds.Item{{Key: "shared", Value: []byte("b")}}); err != nil {
		t.Fatalf("set B: %v", err)
	}

	got, err := c.GetMulti(ctxA, []string{"shared"})
	if err != nil {
		t.Fatalf("get A: %v", err)
	}
	if item, ok := got["shared"]; !ok || string(item.Value) != "a" {
		t.Errorf("tenant A saw %q (ok=%v); want \"a\"", valOf(item), ok)
	}

	got, err = c.GetMulti(ctxB, []string{"shared"})
	if err != nil {
		t.Fatalf("get B: %v", err)
	}
	if item, ok := got["shared"]; !ok || string(item.Value) != "b" {
		t.Errorf("tenant B saw %q (ok=%v); want \"b\"", valOf(item), ok)
	}

	// The default tenant is isolated from both.
	got, err = c.GetMulti(base, []string{"shared"})
	if err != nil {
		t.Fatalf("get default: %v", err)
	}
	if _, ok := got["shared"]; ok {
		t.Errorf("default tenant unexpectedly saw key from a named tenant")
	}
}

func TestFlush(t *testing.T) {
	c := newTestCache(t)
	base := context.Background()
	ctxA := ds.WithTenant(base, "tenant-a")
	ctxB := ds.WithTenant(base, "tenant-b")

	if err := c.SetMulti(ctxA, []*ds.Item{{Key: "k", Value: []byte("a")}}); err != nil {
		t.Fatalf("set A: %v", err)
	}
	if err := c.SetMulti(ctxB, []*ds.Item{{Key: "k", Value: []byte("b")}}); err != nil {
		t.Fatalf("set B: %v", err)
	}

	if err := c.Flush(ctxA); err != nil {
		t.Fatalf("flush A: %v", err)
	}

	// Tenant A is emptied.
	got, err := c.GetMulti(ctxA, []string{"k"})
	if err != nil {
		t.Fatalf("get A: %v", err)
	}
	if _, ok := got["k"]; ok {
		t.Errorf("tenant A still has key after flush")
	}

	// Tenant B is untouched.
	got, err = c.GetMulti(ctxB, []string{"k"})
	if err != nil {
		t.Fatalf("get B: %v", err)
	}
	if item, ok := got["k"]; !ok || string(item.Value) != "b" {
		t.Errorf("tenant B saw %q (ok=%v); want \"b\"", valOf(item), ok)
	}
}

func valOf(i *ds.Item) string {
	if i == nil {
		return ""
	}
	return string(i.Value)
}

// TestIntegration exercises a live Durable Object Worker. Start one with
// `wrangler dev` in the worker/ directory and run:
//
//	DSORM_CF_CACHE_URL=http://localhost:8787 go test ./cache/cloudflare/ -run Integration
//
// It is skipped when DSORM_CF_CACHE_URL is unset or under -short.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Worker integration test")
	}
	baseURL := os.Getenv("DSORM_CF_CACHE_URL")
	if baseURL == "" {
		t.Skip("set DSORM_CF_CACHE_URL to run the live Worker integration test")
	}
	c, err := cloudflare.NewCache(baseURL)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	ctx := ds.WithTenant(context.Background(), "integration")

	// set -> get
	if err := c.SetMulti(ctx, []*ds.Item{{Key: "k", Value: []byte("v"), Flags: 7}}); err != nil {
		t.Fatalf("SetMulti: %v", err)
	}
	got, err := c.GetMulti(ctx, []string{"k"})
	if err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	if item := got["k"]; item == nil || string(item.Value) != "v" || item.Flags != 7 {
		t.Fatalf("GetMulti returned %+v; want value=v flags=7", item)
	}

	// add conflict
	if err := c.AddMulti(ctx, []*ds.Item{{Key: "k", Value: []byte("x")}}); err == nil {
		t.Errorf("AddMulti on existing key: want error, got nil")
	}

	// cas round-trip
	got, _ = c.GetMulti(ctx, []string{"k"})
	got["k"].Value = []byte("v2")
	if err := c.CompareAndSwapMulti(ctx, []*ds.Item{got["k"]}); err != nil {
		t.Errorf("CompareAndSwapMulti: %v", err)
	}

	// increment
	if n, err := c.Increment(ctx, "ctr", 4, 0); err != nil || n != 4 {
		t.Errorf("Increment = %d, %v; want 4, nil", n, err)
	}

	// cleanup
	if err := c.DeleteMulti(ctx, []string{"k"}); err != nil {
		t.Errorf("DeleteMulti: %v", err)
	}
}
