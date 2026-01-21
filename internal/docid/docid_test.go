package docid

import "testing"

func TestForRootAndPath_Deterministic(t *testing.T) {
	got1 := ForRootAndPath("bafyroot", "/ipfs/bafyroot/a.txt")
	got2 := ForRootAndPath("bafyroot", "/ipfs/bafyroot/a.txt")
	if got1 != got2 {
		t.Fatalf("expected deterministic, got %q and %q", got1, got2)
	}
	if len(got1) != 64 {
		t.Fatalf("expected 64 hex chars, got %d", len(got1))
	}
}
