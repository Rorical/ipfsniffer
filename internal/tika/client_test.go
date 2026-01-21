package tika

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExtractText_RequiresBaseURL(t *testing.T) {
	c := &Client{}
	_, err := c.ExtractText(context.Background(), strings.NewReader("x"), 0, 10)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestExtractText_RequiresMaxTextBytes(t *testing.T) {
	c := &Client{BaseURL: "http://example.com"}
	_, err := c.ExtractText(context.Background(), strings.NewReader("x"), 0, 0)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestExtractText_SuccessAndTruncate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(405)
			return
		}
		if r.URL.Path != "/tika" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("content-type", "text/plain")
		_, _ = w.Write([]byte("hello world"))
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, HTTP: srv.Client()}

	res, err := c.ExtractText(context.Background(), strings.NewReader("<html/>"), 0, 5)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(res.Text) != "hello" {
		t.Fatalf("text: %q", string(res.Text))
	}
	if !res.Truncated {
		t.Fatalf("expected truncated")
	}
}

func TestExtractText_Non2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("nope"))
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, HTTP: srv.Client()}
	_, err := c.ExtractText(context.Background(), strings.NewReader("x"), 0, 10)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestExtractText_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, HTTP: srv.Client()}
	_, err := c.ExtractText(context.Background(), bytes.NewReader([]byte("x")), 10*time.Millisecond, 10)
	if err == nil {
		t.Fatalf("expected error")
	}
}
