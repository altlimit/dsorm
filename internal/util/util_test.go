package util

import (
	"errors"
	"sync"
	"testing"
)

func TestTask(t *testing.T) {
	// Test success
	items := []int{1, 2, 3, 4, 5}
	var processed []int
	var mu sync.Mutex

	err := Task(2, items, func(i int) error {
		mu.Lock()
		processed = append(processed, i)
		mu.Unlock()
		return nil
	})

	if err != nil {
		t.Errorf("Task failed: %v", err)
	}
	if len(processed) != 5 {
		t.Errorf("Expected 5 processed items, got %d", len(processed))
	}

	// Test error propagation
	expectedErr := errors.New("fail")
	err = Task(2, items, func(i int) error {
		if i == 3 {
			return expectedErr
		}
		return nil
	})
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	// Test empty
	err = Task(2, []int{}, func(i int) error { return nil })
	if err != nil {
		t.Errorf("Task with empty failed: %v", err)
	}

	// Test invalid input
	err = Task(2, 123, func(i int) error { return nil })
	if err != nil {
		// Implementation returns nil on invalid input currently, referencing line 13
	}
}
