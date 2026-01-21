//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Rorical/IPFSniffer/internal/opensearch"
	"github.com/Rorical/IPFSniffer/internal/search"

	osclient "github.com/opensearch-project/opensearch-go/v4"
	osapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func TestIntegration_SearchAndDoc(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	baseURL := os.Getenv("IPFSNIFFER_IT_OPENSEARCH_URL")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:9200"
	}

	osc, err := opensearch.New(opensearch.Config{URL: baseURL, Insecure: true})
	if err != nil {
		t.Fatalf("opensearch client: %v", err)
	}

	index := fmt.Sprintf("ipfsniffer-docs-it-%d", time.Now().UnixNano())
	alias := "ipfsniffer-docs-it"
	if err := opensearch.EnsureIndex(ctx, osc, opensearch.IndexSpec{IndexName: index, AliasName: alias}, opensearch.DefaultMappingJSON); err != nil {
		t.Fatalf("ensure index: %v", err)
	}

	// Index two docs.
	seedDoc := func(id, rootCID, path, text string, processedAt time.Time, size int64) {
		doc := map[string]any{
			"doc_id":          id,
			"root_cid":        rootCID,
			"cid":             "",
			"path":            path,
			"path_text":       path,
			"filename":        "file.txt",
			"filename_text":   "file.txt",
			"node_type":       "file",
			"ext":             ".txt",
			"mime":            "text/plain",
			"size_bytes":      size,
			"content_indexed": true,
			"skip_reason":     "",
			"text":            text,
			"text_truncated":  false,
			"names_text":      "file.txt",
			"discovered_at":   nil,
			"fetched_at":      nil,
			"processed_at":    processedAt.UTC().Format(time.RFC3339Nano),
			"sources":         []string{"ipns"},
			"ipns_name":       "",
			"dir": map[string]any{
				"entries_count":     0,
				"entries_truncated": false,
			},
		}

		b, _ := json.Marshal(doc)
		req := osapi.IndexReq{Index: index, DocumentID: id, Body: bytes.NewReader(b), Params: osapi.IndexParams{Refresh: "true"}}
		res, err := osc.Do(ctx, req, nil)
		if res != nil {
			defer func() { _ = res.Body.Close() }()
		}
		if err != nil {
			t.Fatalf("index doc: %v", err)
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			t.Fatalf("index doc status %d", res.StatusCode)
		}
	}

	seedDoc("doc-a", "bafyroot", "/ipfs/bafyroot/a.txt", "hello world", time.Now().Add(-5*time.Minute), 10)
	seedDoc("doc-b", "bafyroot", "/ipfs/bafyroot/b.txt", "hello ipfsniffer", time.Now().Add(-1*time.Minute), 20)

	api := &API{Search: &search.Client{OS: osc, Index: alias}}
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	// /search should return hits + highlight.
	res, err := http.Get(srv.URL + "/search?q=hello&sort=processed_at:desc")
	if err != nil {
		t.Fatalf("search request: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		// If OpenSearch isn't running, this will typically be 502 from our handler.
		t.Fatalf("search status %d", res.StatusCode)
	}
	var searchOut struct {
		Total int `json:"total"`
		Hits  []struct {
			ID        string              `json:"id"`
			Highlight map[string][]string `json:"highlight"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&searchOut); err != nil {
		t.Fatalf("decode search response: %v", err)
	}
	if searchOut.Total < 2 {
		t.Fatalf("expected total>=2 got %d", searchOut.Total)
	}
	foundHighlight := false
	for _, h := range searchOut.Hits {
		if h.ID == "doc-a" || h.ID == "doc-b" {
			if len(h.Highlight) > 0 {
				foundHighlight = true
			}
		}
	}
	if !foundHighlight {
		t.Fatalf("expected at least one hit to include highlight")
	}

	// Invalid sort should return 400.
	resBad, err := http.Get(srv.URL + "/search?q=hello&sort=badfield:asc")
	if err != nil {
		t.Fatalf("bad sort request: %v", err)
	}
	defer resBad.Body.Close()
	if resBad.StatusCode == 502 {
		t.Skip("OpenSearch not reachable")
	}
	if resBad.StatusCode != 400 {
		t.Fatalf("expected 400 got %d", resBad.StatusCode)
	}

	// /doc/{id}
	res2, err := http.Get(srv.URL + "/doc/doc-b")
	if err != nil {
		t.Fatalf("doc request: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != 200 {
		t.Fatalf("doc status %d", res2.StatusCode)
	}
	var docOut struct {
		ID string `json:"id"`
		// doc is arbitrary json
		Doc json.RawMessage `json:"doc"`
	}
	if err := json.NewDecoder(res2.Body).Decode(&docOut); err != nil {
		t.Fatalf("decode doc response: %v", err)
	}
	if docOut.ID != "doc-b" {
		t.Fatalf("expected doc-b got %q", docOut.ID)
	}
	if len(docOut.Doc) == 0 {
		t.Fatalf("expected doc body")
	}
}

// Ensure we reference osclient package in integration tag compilation.
var _ = osclient.Client{}
