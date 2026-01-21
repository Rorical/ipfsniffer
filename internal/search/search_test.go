package search

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
)

func TestSearch_RequestBuildsHighlightAndSort(t *testing.T) {
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/idx/_search" {
			w.WriteHeader(404)
			return
		}
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":1},"hits":[{"_id":"1","_score":1.0,"_source":{"path":"/x"},"highlight":{"text":["<em>hello</em>"]}}]}}`))
	}))
	defer srv.Close()

	osc, err := opensearch.NewClient(opensearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	c := &Client{OS: osc, Index: "idx"}
	res, err := c.Search(context.Background(), SearchParams{Q: "hello", From: 0, Size: 10, Sort: "size_bytes:asc"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if res.Total != 1 || len(res.Hits) != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res.Hits[0].Highlight == nil || len(res.Hits[0].Highlight["text"]) != 1 {
		t.Fatalf("expected highlight")
	}

	var body map[string]any
	if err := json.Unmarshal(gotBody, &body); err != nil {
		t.Fatalf("body json: %v", err)
	}
	if _, ok := body["highlight"]; !ok {
		t.Fatalf("expected highlight in request")
	}
	if _, ok := body["sort"]; !ok {
		t.Fatalf("expected sort in request")
	}
}

func TestSearch_DefaultSortWhenQEmpty(t *testing.T) {
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0},"hits":[]}}`))
	}))
	defer srv.Close()

	osc, err := opensearch.NewClient(opensearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	c := &Client{OS: osc, Index: "idx"}
	_, err = c.Search(context.Background(), SearchParams{Q: "", From: 0, Size: 10})
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if !bytes.Contains(gotBody, []byte("processed_at")) {
		t.Fatalf("expected default sort processed_at in request: %s", string(gotBody))
	}
}

func TestSearch_BadSortIsBadRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0},"hits":[]}}`))
	}))
	defer srv.Close()

	osc, err := opensearch.NewClient(opensearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	c := &Client{OS: osc, Index: "idx"}
	_, err = c.Search(context.Background(), SearchParams{Q: "x", From: 0, Size: 10, Sort: "nope:asc"})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !IsBadRequest(err) {
		t.Fatalf("expected bad request error, got %v", err)
	}
}

func TestGetDoc_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"found":false}`))
	}))
	defer srv.Close()

	osc, err := opensearch.NewClient(opensearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	c := &Client{OS: osc, Index: "idx"}
	_, found, err := c.GetDoc(context.Background(), "missing")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if found {
		t.Fatalf("expected not found")
	}
}

func TestGetDoc_Found(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"found":true,"_source":{"doc_id":"1","path":"/x"}}`))
	}))
	defer srv.Close()

	osc, err := opensearch.NewClient(opensearch.Config{Addresses: []string{srv.URL}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	c := &Client{OS: osc, Index: "idx"}
	doc, found, err := c.GetDoc(context.Background(), "1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatalf("expected found")
	}
	if len(doc) == 0 {
		t.Fatalf("expected doc")
	}
}
