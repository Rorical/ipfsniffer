package filter

import "testing"

func TestDecide(t *testing.T) {
	p := Policy{
		SkipExt:        []string{".zip", ".png"},
		SkipMimePrefix: []string{"image/", "video/"},
		MaxFileBytes:   10,
	}

	if d := Decide("/ipfs/x/file.txt", "text/plain", 11, p); d.Allowed || d.SkipReason != "too_large" {
		t.Fatalf("expected too_large, got %+v", d)
	}

	if d := Decide("/ipfs/x/file.png", "image/png", 1, p); d.Allowed || d.SkipReason != "ext_denied" {
		t.Fatalf("expected ext_denied, got %+v", d)
	}

	if d := Decide("/ipfs/x/file.bin", "image/png", 1, p); d.Allowed || d.SkipReason != "mime_denied" {
		t.Fatalf("expected mime_denied, got %+v", d)
	}

	if d := Decide("/ipfs/x/file.txt", "text/plain", 1, p); !d.Allowed {
		t.Fatalf("expected allowed, got %+v", d)
	}
}
