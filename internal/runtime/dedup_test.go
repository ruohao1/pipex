package runtime

import "testing"

func TestSafeStringKey_Success(t *testing.T) {
	key, err := SafeStringKey(func(v int) string {
		return "k"
	}, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "k" {
		t.Fatalf("key = %q, want %q", key, "k")
	}
}

func TestSafeStringKey_Panic(t *testing.T) {
	_, err := SafeStringKey(func(v int) string {
		panic("boom")
	}, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "dedup key panic: boom" {
		t.Fatalf("error = %q, want %q", got, "dedup key panic: boom")
	}
}
