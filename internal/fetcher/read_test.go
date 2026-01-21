package fetcher

import (
	"bytes"
	"io"
	"testing"
)

func TestReadUpToN(t *testing.T) {
	b := bytes.NewBufferString("hello")
	out, n, err := readUpToN(b, 10)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 got %d", n)
	}
	if string(out) != "hello" {
		t.Fatalf("unexpected %q", string(out))
	}

	b2 := bytes.NewBufferString("hello")
	out2, n2, err := readUpToN(b2, 2)
	if err != nil && err != io.ErrUnexpectedEOF {
		t.Fatalf("err: %v", err)
	}
	if n2 != 2 {
		t.Fatalf("expected 2 got %d", n2)
	}
	if string(out2) != "he" {
		t.Fatalf("unexpected %q", string(out2))
	}
}
