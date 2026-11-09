package cache_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/altlimit/dsorm/cache"
	"github.com/altlimit/dsorm/cache/cloudflare"
	"github.com/altlimit/dsorm/cache/memory"
	ds "github.com/altlimit/dsorm/ds"
)

// failingDeleteCache is a backend whose deletes fail with a fixed error.
type failingDeleteCache struct {
	ds.Cache
	err error
}

func (f failingDeleteCache) DeleteMulti(context.Context, []string) error {
	return f.err
}

func TestDeleteMissingKeyIsCacheMiss(t *testing.T) {
	srv := httptest.NewServer(cloudflare.NewMockHandler())
	t.Cleanup(srv.Close)
	cf, err := cloudflare.NewCache(srv.URL)
	if err != nil {
		t.Fatalf("cloudflare.NewCache: %v", err)
	}

	backends := map[string]ds.Cache{
		"memory":     memory.NewCache(),
		"cloudflare": cf,
	}
	for name, backend := range backends {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			c := cache.New(backend)

			if err := c.Delete(ctx, "missing"); !errors.Is(err, ds.ErrCacheMiss) {
				t.Errorf("Delete(missing) = %v, want ds.ErrCacheMiss", err)
			}

			if err := c.Set(ctx, "present", []byte("v"), 0); err != nil {
				t.Fatalf("Set: %v", err)
			}
			if err := c.Delete(ctx, "present"); err != nil {
				t.Errorf("Delete(present) = %v, want nil", err)
			}
		})
	}
}

func TestDeleteKeepsOtherErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	tests := []struct {
		name string
		err  error
	}{
		{"inside a MultiError", ds.MultiError{boom}},
		{"plain", boom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cache.New(failingDeleteCache{err: tt.err}).Delete(ctx, "k")
			if !errors.Is(err, boom) {
				t.Errorf("Delete = %v, want boom", err)
			}
			if errors.Is(err, ds.ErrCacheMiss) {
				t.Errorf("Delete = %v, must not match ds.ErrCacheMiss", err)
			}
		})
	}
}
