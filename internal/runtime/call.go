package runtime

// Call2 invokes a two-arg callback with panic recovery.
func Call2[A, B any](fn func(A, B), a A, b B) {
	if fn == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	fn(a, b)
}

// Call3 invokes a three-arg callback with panic recovery.
func Call3[A, B, C any](fn func(A, B, C), a A, b B, c C) {
	if fn == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	fn(a, b, c)
}
