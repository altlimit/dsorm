package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"google.golang.org/appengine/v2"

	"github.com/altlimit/dsorm/cache/memcache"
	"github.com/altlimit/dsorm/ds"
)

func main() {
	http.HandleFunc("/test-memcache", handleTestMemcache)
	appengine.Main()
}

func handleTestMemcache(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	cacher := memcache.NewCache()

	// 1. Test Set and Get
	item := &ds.Item{
		Key:        "test-key",
		Value:      []byte("test-value"),
		Expiration: time.Minute,
	}
	if err := cacher.SetMulti(ctx, []*ds.Item{item}); err != nil {
		http.Error(w, fmt.Sprintf("SetMulti failed: %v", err), 500)
		return
	}

	items, err := cacher.GetMulti(ctx, []string{"test-key"})
	if err != nil {
		http.Error(w, fmt.Sprintf("GetMulti failed: %v", err), 500)
		return
	}
	if got, ok := items["test-key"]; !ok || string(got.Value) != "test-value" {
		http.Error(w, fmt.Sprintf("GetMulti mismatch: %v", items), 500)
		return
	}

	// 2. Test Delete
	if err := cacher.DeleteMulti(ctx, []string{"test-key"}); err != nil {
		http.Error(w, fmt.Sprintf("DeleteMulti failed: %v", err), 500)
		return
	}
	items, err = cacher.GetMulti(ctx, []string{"test-key"})
	if err != nil {
		http.Error(w, fmt.Sprintf("GetMulti after delete failed: %v", err), 500)
		return
	}
	if _, ok := items["test-key"]; ok {
		http.Error(w, "Item found after delete", 500)
		return
	}

	// 3. Test Increment with expiration
	incrKey := "test-incr-expire"
	// Clean up any leftover from previous runs
	cacher.DeleteMulti(ctx, []string{incrKey})

	val, err := cacher.Increment(ctx, incrKey, 1, 2*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("Increment (first) failed: %v", err), 500)
		return
	}
	if val != 1 {
		http.Error(w, fmt.Sprintf("Increment (first) expected 1, got %d", val), 500)
		return
	}

	// Increment again — should add to existing value
	val, err = cacher.Increment(ctx, incrKey, 5, 2*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("Increment (second) failed: %v", err), 500)
		return
	}
	if val != 6 {
		http.Error(w, fmt.Sprintf("Increment (second) expected 6, got %d", val), 500)
		return
	}

	// Wait for expiration
	time.Sleep(3 * time.Second)

	// Key should be gone
	items, err = cacher.GetMulti(ctx, []string{incrKey})
	if err != nil {
		http.Error(w, fmt.Sprintf("GetMulti after incr expire failed: %v", err), 500)
		return
	}
	if _, ok := items[incrKey]; ok {
		http.Error(w, "Increment key should have expired but still exists", 500)
		return
	}

	fmt.Fprintln(w, "Memcache tests passed!")
	log.Println("Memcache tests passed")
}
