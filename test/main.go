package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"google.golang.org/appengine/v2"

	ds "github.com/altlimit/dsorm/ds"
	"github.com/altlimit/dsorm/cache/memcache"
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

	fmt.Fprintln(w, "Memcache tests passed!")
	log.Println("Memcache tests passed")
}
