// Package cache provides application-level caching utilities built on top
// of the dsorm cache backends (Redis, memory, memcache).
//
// Use [New] to wrap a [github.com/altlimit/dsorm/ds.Cache] into an
// application-friendly [Cache] interface with single-key operations,
// rate limiting, and JSON serialization helpers.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ds "github.com/altlimit/dsorm/ds"
)

// Cache provides application-level caching operations.
// It wraps the lower-level ds.Cache interface with convenient single-key
// methods, atomic increment, and rate limiting.
type Cache interface {
	// Get retrieves a single item from the cache by key.
	// Returns ds.ErrCacheMiss if the key is not found.
	Get(ctx context.Context, key string) (*ds.Item, error)

	// Set stores a single item in the cache with the given expiration.
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error

	// Delete removes a single key from the cache.
	Delete(ctx context.Context, key string) error

	// Increment atomically increments a key's integer value by delta.
	// Creates the key with value=delta if it does not exist.
	Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error)

	// RateLimit checks if a request is within the rate limit for the given key.
	// limit is the max number of requests per window.
	RateLimit(ctx context.Context, key string, limit int, window time.Duration) (*RateLimitResult, error)

	// Unwrap returns the underlying ds.Cache interface for advanced use.
	Unwrap() ds.Cache
}

// RateLimitResult contains the result of a rate limit check.
type RateLimitResult struct {
	// Allowed is true if the request is within the rate limit.
	Allowed bool
	// Remaining is the number of requests remaining in the current window.
	Remaining int
	// ResetAt is when the current rate limit window resets.
	ResetAt time.Time
}

// New wraps a ds.Cache with application-level caching utilities.
func New(c ds.Cache) Cache {
	return &wrapper{c: c}
}

type wrapper struct {
	c ds.Cache
}

func (w *wrapper) Get(ctx context.Context, key string) (*ds.Item, error) {
	result, err := w.c.GetMulti(ctx, []string{key})
	if err != nil {
		return nil, err
	}
	item, ok := result[key]
	if !ok {
		return nil, ds.ErrCacheMiss
	}
	return item, nil
}

func (w *wrapper) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return w.c.SetMulti(ctx, []*ds.Item{{
		Key:        key,
		Value:      value,
		Expiration: expiration,
	}})
}

func (w *wrapper) Delete(ctx context.Context, key string) error {
	return w.c.DeleteMulti(ctx, []string{key})
}

func (w *wrapper) Increment(ctx context.Context, key string, delta int64, expiration time.Duration) (int64, error) {
	return w.c.Increment(ctx, key, delta, expiration)
}

func (w *wrapper) RateLimit(ctx context.Context, key string, limit int, window time.Duration) (*RateLimitResult, error) {
	now := time.Now()
	windowStart := now.Truncate(window)
	windowKey := fmt.Sprintf("%s:%d", key, windowStart.UnixMilli())
	resetAt := windowStart.Add(window)

	// TTL = time remaining in this window + small buffer
	ttl := resetAt.Sub(now) + time.Second

	count, err := w.c.Increment(ctx, windowKey, 1, ttl)
	if err != nil {
		return nil, fmt.Errorf("dsorm: rate limit increment: %w", err)
	}

	remaining := limit - int(count)
	if remaining < 0 {
		remaining = 0
	}

	return &RateLimitResult{
		Allowed:   int(count) <= limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}, nil
}

func (w *wrapper) Unwrap() ds.Cache {
	return w.c
}

// Load retrieves a value from the cache and decodes it from JSON into T.
// Returns ds.ErrCacheMiss if the key is not found.
func Load[T any](ctx context.Context, c Cache, key string) (T, error) {
	var zero T
	item, err := c.Get(ctx, key)
	if err != nil {
		return zero, err
	}
	var val T
	if err := json.Unmarshal(item.Value, &val); err != nil {
		return zero, fmt.Errorf("dsorm: cache load unmarshal: %w", err)
	}
	return val, nil
}

// Save encodes a value as JSON and stores it in the cache.
func Save[T any](ctx context.Context, c Cache, key string, val T, expiration time.Duration) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("dsorm: cache save marshal: %w", err)
	}
	return c.Set(ctx, key, data, expiration)
}
