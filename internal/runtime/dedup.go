package runtime

import "fmt"

// SafeStringKey executes a key function and converts panics into errors.
func SafeStringKey[T any](fn func(T) string, in T) (key string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("dedup key panic: %v", r)
		}
	}()
	return fn(in), nil
}
