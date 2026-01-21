package fetcher

import "testing"

func TestDepthOfPath(t *testing.T) {
	if got := depthOfPath("/ipfs/bafy123"); got != 0 {
		t.Fatalf("expected 0 got %d", got)
	}
	if got := depthOfPath("/ipfs/bafy123/a"); got != 1 {
		t.Fatalf("expected 1 got %d", got)
	}
	if got := depthOfPath("/ipfs/bafy123/a/b"); got != 2 {
		t.Fatalf("expected 2 got %d", got)
	}
}

func TestMinInt64(t *testing.T) {
	if minInt64(1, 2) != 1 {
		t.Fatal("expected 1")
	}
	if minInt64(3, 2) != 2 {
		t.Fatal("expected 2")
	}
}
