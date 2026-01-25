package memory

import (
	"context"
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
