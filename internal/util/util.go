package util

import (
	"reflect"
	"sync"
)

// Task runs a function concurrently on a slice of items with a limit on concurrency.
func Task(concurrency int, items interface{}, fn interface{}) error {
	v := reflect.ValueOf(items)
	if v.Kind() != reflect.Slice {
		// Or panic?
		return nil
	}

	f := reflect.ValueOf(fn)
	// simple validation could be added

	in := make(chan int)
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range in {
				// Call fn(items[idx])
				item := v.Index(idx)
				res := f.Call([]reflect.Value{item})

				// Check error
				if len(res) > 0 {
					if errVal := res[0]; !errVal.IsNil() {
						if err, ok := errVal.Interface().(error); ok {
							errOnce.Do(func() {
								firstErr = err
							})
						}
					}
				}
			}
		}()
	}

	// Feed input
	for i := 0; i < v.Len(); i++ {
		// Stop early if error? Optional.
		in <- i
	}
	close(in)
	wg.Wait()

	return firstErr
}
