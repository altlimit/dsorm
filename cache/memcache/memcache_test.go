package memcache_test

import (
	"net/http"
	"testing"
	"time"
)

func TestMemcacheIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	http.DefaultClient.Timeout = 1 * time.Minute
	resp, err := http.Get("http://localhost:8080/test-memcache")
	if err != nil {
		t.Fatalf("Failed to call test-memcache: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", resp.StatusCode)
	}
}
