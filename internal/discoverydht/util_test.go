package discoverydht

import (
	"testing"

	mh "github.com/multiformats/go-multihash"
)

func TestMhToCIDString_Empty(t *testing.T) {
	if got := mhToCIDString(nil); got != "" {
		t.Fatalf("expected empty string got %q", got)
	}
}

func TestMhToCIDString_Sha256(t *testing.T) {
	m, err := mh.Sum([]byte("hello"), mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	got := mhToCIDString(m)
	if got == "" {
		t.Fatalf("expected cid string")
	}
	// CIDv0 should start with Qm
	if len(got) < 2 || got[:2] != "Qm" {
		t.Fatalf("expected CIDv0 prefix Qm, got %q", got)
	}
}

func TestMhToCIDString_NotSha256(t *testing.T) {
	m, err := mh.Sum([]byte("hello"), mh.SHA1, -1)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	got := mhToCIDString(m)
	if got == "" {
		t.Fatalf("expected cid string")
	}
	// We wrap as CIDv1 raw, which is base32 and typically starts with 'b'.
	if got[0] != 'b' {
		t.Fatalf("expected CIDv1 base32 string starting with b, got %q", got)
	}
}
