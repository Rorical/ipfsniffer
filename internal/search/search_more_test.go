package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
)

func TestSearch_RequiresClientAndIndex(t *testing.T) {
	c := &Client{}
	_, err := c.Search(context.Background(), SearchParams{})
	if err == nil {
		t.Fatalf("expected error")
	}

	osc, err := opensearch.NewClient(opensearch.Config{Addresses: []string{"http://127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	c = &Client{OS: osc}
	_, err = c.Search(context.Background(), SearchParams{})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestGetDoc_RequiresClientIndexAndDocID(t *testing.T) {
	c := &Client{}
	_, _, err := c.GetDoc(context.Background(), "x")
	if err == nil {
		t.Fatalf("expected error")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"found":true,"_source":{"doc_id":"1"}}`))
	}))
	defer srv.Close()

	osc, err := opensearch.NewClient(opensearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	c = &Client{OS: osc, Index: "idx"}
	_, _, err = c.GetDoc(context.Background(), "")
	if err == nil {
		t.Fatalf("expected error")
	}
}
