package server

import (
	"net/url"
	"testing"
)

func TestParseSearchParams_FromAndSizeValidation(t *testing.T) {
	_, err := parseSearchParams(url.Values{"from": []string{"-1"}})
	if err == nil {
		t.Fatalf("expected error")
	}
	_, err = parseSearchParams(url.Values{"from": []string{"nope"}})
	if err == nil {
		t.Fatalf("expected error")
	}
	_, err = parseSearchParams(url.Values{"size": []string{"0"}})
	if err == nil {
		t.Fatalf("expected error")
	}
	_, err = parseSearchParams(url.Values{"size": []string{"101"}})
	if err == nil {
		t.Fatalf("expected error")
	}

	p, err := parseSearchParams(url.Values{"from": []string{"1"}, "size": []string{"2"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if p.From != 1 || p.Size != 2 {
		t.Fatalf("unexpected params: %+v", p)
	}
}
