package ds

import "errors"

// onlyCacheMisses reports whether err says nothing more than "some of these
// keys were not in the cache", alone or as every non-nil entry of a MultiError.
//
// Releasing a write's cache locks deletes them, and a lock that is already gone
// is a cache miss: a concurrent write to the same entity released it first, or
// it expired or was evicted. The release only wants the lock gone, so that is
// success, not a fault worth reporting. Anything else mixed in still is.
func onlyCacheMisses(err error) bool {
	var me MultiError
	if errors.As(err, &me) {
		missed := false
		for _, e := range me {
			if e == nil {
				continue
			}
			if !onlyCacheMisses(e) {
				return false
			}
			missed = true
		}
		return missed
	}
	return errors.Is(err, ErrCacheMiss)
}
