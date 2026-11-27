package ds

import (
	"errors"
	"fmt"
	"testing"

	pkgerrors "github.com/pkg/errors"
)

func TestOnlyCacheMisses(t *testing.T) {
	boom := errors.New("connection reset")
	cases := map[string]struct {
		err  error
		want bool
	}{
		"nil":                             {nil, false},
		"a miss":                          {ErrCacheMiss, true},
		"a miss wrapped by a backend":     {fmt.Errorf("redis: expected to remove 1 keys, but only removed 0: %w", ErrCacheMiss), true},
		"a miss wrapped by pkg/errors":    {pkgerrors.Wrap(ErrCacheMiss, "release"), true},
		"another error":                   {boom, false},
		"per-key misses":                  {MultiError{nil, ErrCacheMiss, ErrCacheMiss}, true},
		"a miss beside a real failure":    {MultiError{ErrCacheMiss, boom}, false},
		"a MultiError with nothing in it": {MultiError{nil, nil}, false},
		"a wrapped MultiError of misses":  {fmt.Errorf("delete: %w", MultiError{ErrCacheMiss}), true},
	}
	for name, tc := range cases {
		if got := onlyCacheMisses(tc.err); got != tc.want {
			t.Errorf("%s: onlyCacheMisses(%v) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
}
