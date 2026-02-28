package memory

import (
	"context"
	"fmt"
	"testing"
	"time"

	dsorm "github.com/altlimit/dsorm/ds"
)

func TestMemoryCache(t *testing.T) {
	cacher := NewCache()
	ctx := context.Background()

	item := &dsorm.Item{
		Key:        "test-key",
		Value:      []byte("test-value"),
		Expiration: time.Minute,
	}

	// Test Set
	if err := cacher.SetMulti(ctx, []*dsorm.Item{item}); err != nil {
		t.Fatalf("SetMulti failed: %v", err)
	}

	// Test Get
	items, err := cacher.GetMulti(ctx, []string{"test-key"})
	if err != nil {
		t.Fatalf("GetMulti failed: %v", err)
	}
	if got, ok := items["test-key"]; !ok {
		t.Fatal("Item not found")
	} else if string(got.Value) != "test-value" {
		t.Errorf("Value mismatch, got %s, want test-value", got.Value)
	}

	// Test Add
	item2 := &dsorm.Item{
		Key:   "test-key-2",
		Value: []byte("val2"),
	}
	if err := cacher.AddMulti(ctx, []*dsorm.Item{item2}); err != nil {
		t.Fatalf("AddMulti failed: %v", err)
	}
	// Add existing should give ErrNotStored
	if err := cacher.AddMulti(ctx, []*dsorm.Item{item}); err == nil {
		t.Fatal("Expected error adding existing item")
	} else if me, ok := err.(dsorm.MultiError); !ok || me[0] != dsorm.ErrNotStored {
		t.Fatalf("Expected ErrNotStored, got %v", err)
	}

	// Test Delete
	if err := cacher.DeleteMulti(ctx, []string{"test-key"}); err != nil {
		t.Fatalf("DeleteMulti failed: %v", err)
	}
	items, err = cacher.GetMulti(ctx, []string{"test-key"})
	if err != nil {
		t.Fatalf("GetMulti after delete failed: %v", err)
	}
	if _, ok := items["test-key"]; ok {
		t.Fatal("Item still exists after delete")
	}
}

func TestMemoryCAS(t *testing.T) {
	cacher := NewCache()
	ctx := context.Background()

	item := &dsorm.Item{
		Key:   "cas-key",
		Value: []byte("initial"),
	}
	cacher.SetMulti(ctx, []*dsorm.Item{item})

	// Get to get CAS info
	items, _ := cacher.GetMulti(ctx, []string{"cas-key"})
	item = items["cas-key"]

	// Valid CAS
	item.Value = []byte("updated")
	if err := cacher.CompareAndSwapMulti(ctx, []*dsorm.Item{item}); err != nil {
		t.Fatalf("CAS failed: %v", err)
	}

	// Invalid CAS (old CAS info)
	item.Value = []byte("stale")
	if err := cacher.CompareAndSwapMulti(ctx, []*dsorm.Item{item}); err == nil {
		t.Fatal("Expected CAS conflict")
	} else if me, ok := err.(dsorm.MultiError); !ok || me[0] != dsorm.ErrCASConflict {
		t.Fatalf("Expected ErrCASConflict, got %v", err)
	}
}

func TestMemoryIncrement(t *testing.T) {
	cacher := NewCache()
	ctx := context.Background()

	// Increment non-existent key creates it
	val, err := cacher.Increment(ctx, "incr-key", 1, time.Minute)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}

	// Increment again
	val, err = cacher.Increment(ctx, "incr-key", 5, time.Minute)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 6 {
		t.Errorf("expected 6, got %d", val)
	}

	// Negative delta
	val, err = cacher.Increment(ctx, "incr-key", -2, time.Minute)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 4 {
		t.Errorf("expected 4, got %d", val)
	}
}

func TestMemoryLRUEviction(t *testing.T) {
	// Create a cache with very small capacity
	cacher := NewCache(WithMaxEntries(3))
	ctx := context.Background()

	// Add 3 items
	for i := 0; i < 3; i++ {
		item := &dsorm.Item{
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("val-%d", i)),
		}
		cacher.SetMulti(ctx, []*dsorm.Item{item})
	}

	// All 3 should be present
	items, _ := cacher.GetMulti(ctx, []string{"key-0", "key-1", "key-2"})
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}

	// Add a 4th item, should evict the least recently used (key-0)
	cacher.SetMulti(ctx, []*dsorm.Item{{
		Key:   "key-3",
		Value: []byte("val-3"),
	}})

	items, _ = cacher.GetMulti(ctx, []string{"key-0"})
	if _, ok := items["key-0"]; ok {
		t.Error("key-0 should have been evicted")
	}

	items, _ = cacher.GetMulti(ctx, []string{"key-3"})
	if _, ok := items["key-3"]; !ok {
		t.Error("key-3 should exist")
	}
}

func TestMemoryTTLExpiration(t *testing.T) {
	cacher := NewCache()
	ctx := context.Background()

	item := &dsorm.Item{
		Key:        "expire-key",
		Value:      []byte("expire-val"),
		Expiration: 50 * time.Millisecond,
	}
	cacher.SetMulti(ctx, []*dsorm.Item{item})

	// Should exist immediately
	items, _ := cacher.GetMulti(ctx, []string{"expire-key"})
	if _, ok := items["expire-key"]; !ok {
		t.Fatal("item should exist before expiration")
	}

	// Wait for expiration
	time.Sleep(60 * time.Millisecond)

	items, _ = cacher.GetMulti(ctx, []string{"expire-key"})
	if _, ok := items["expire-key"]; ok {
		t.Fatal("item should have expired")
	}
}
