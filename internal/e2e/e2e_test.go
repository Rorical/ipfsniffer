//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	"github.com/Rorical/IPFSniffer/internal/config"
	"github.com/Rorical/IPFSniffer/internal/extractor"
	"github.com/Rorical/IPFSniffer/internal/indexer"
	"github.com/Rorical/IPFSniffer/internal/indexprep"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/opensearch"
	"github.com/Rorical/IPFSniffer/internal/search"
	"github.com/Rorical/IPFSniffer/internal/tika"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
)

func TestE2E_InlineFetchResultToSearch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	opensearchURL := getenv("IPFSNIFFER_E2E_OPENSEARCH_URL", "http://127.0.0.1:9200")
	tikaURL := getenv("IPFSNIFFER_E2E_TIKA_URL", "http://127.0.0.1:9998")
	natsURL := getenv("IPFSNIFFER_E2E_NATS_URL", "nats://127.0.0.1:4222")

	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.NATS.URL = natsURL
	cfg.OpenSearch.URL = opensearchURL
	cfg.Tika.URL = tikaURL

	nc, js, err := internalnats.Connect(ctx, cfg.NATS)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	defer nc.Drain()

	if err := internalnats.EnsureStream(ctx, js); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	osc, err := opensearch.New(opensearch.Config{URL: cfg.OpenSearch.URL, Insecure: true})
	if err != nil {
		t.Fatalf("opensearch client: %v", err)
	}

	// Create a unique index per test run.
	idx := "ipfsniffer-docs-e2e-" + uuid.NewString()
	alias := "ipfsniffer-docs"
	if err := opensearch.EnsureIndex(ctx, osc, opensearch.IndexSpec{IndexName: idx, AliasName: alias}, opensearch.DefaultMappingJSON); err != nil {
		t.Fatalf("ensure index: %v", err)
	}

	// Start workers (extractor -> doc.ready -> index-prep -> index.request -> indexer -> OpenSearch).
	// These run until ctx cancels.
	tikaClient := &tika.Client{BaseURL: cfg.Tika.URL, HTTP: &http.Client{Timeout: 30 * time.Second}}

	ex := &extractor.Worker{
		NATS:         js,
		Tika:         tikaClient,
		Durable:      "e2e-extractor",
		MaxDeliver:   1,
		MaxTextBytes: 200000,
		TikaTimeout:  10 * time.Second,
	}
	ip := &indexprep.Worker{
		NATS:       js,
		Durable:    "e2e-index-prep",
		MaxDeliver: 1,
		IndexName:  idx,
	}
	ix := &indexer.Worker{
		NATS:          js,
		OS:            osc,
		Durable:       "e2e-indexer",
		MaxDeliver:    1,
		BulkMax:       10,
		FlushInterval: 200 * time.Millisecond,
	}

	go func() { _ = ex.Run(ctx) }()
	go func() { _ = ip.Run(ctx) }()
	go func() { _ = ix.Run(ctx) }()

	// Publish one inline fetch.result.
	fr := &ipfsnifferv1.FetchResult{
		V:  1,
		Id: uuid.NewString(),
		Ts: time.Now().UTC().Format(time.RFC3339Nano),
		Data: &ipfsnifferv1.FetchResultData{
			RootCid:   "bafyroot",
			Path:      "/ipfs/bafyroot/index.html",
			NodeType:  "file",
			SizeBytes: 20,
			Mime:      "text/html",
			Ext:       ".html",
			Content: &ipfsnifferv1.FetchContentResult{
				Mode:         "inline",
				InlineBase64: base64.StdEncoding.EncodeToString([]byte("<html><title>x</title><body>Hello E2E</body></html>")),
			},
			Status:    "ok",
			FetchedAt: time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	payload, err := codec.Marshal(fr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := internalnats.Publish(ctx, js, internalnats.SubjectFetchResult, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait until it is searchable.
	sc := &search.Client{OS: osc, Index: alias}

	// Search on the exact index name to avoid any alias propagation lag.
	idxClient := &search.Client{OS: osc, Index: idx}

	deadline := time.Now().Add(45 * time.Second)
	var lastErr error
	var lastTotal int
	for time.Now().Before(deadline) {
		res, err := idxClient.Search(ctx, search.SearchParams{Q: "Hello", Size: 5})
		if err == nil && res.Total > 0 {
			// Also confirm the alias is wired (best-effort).
			_, _ = sc.Search(ctx, search.SearchParams{Q: "Hello", Size: 1})
			return
		}
		lastErr = err
		lastTotal = res.Total
		// indexing/refresh is async; retry for a bit.
		time.Sleep(500 * time.Millisecond)
	}
	// If we got here, dump a tiny hint.
	if lastErr != nil {
		t.Fatalf("doc not searchable before timeout: last err: %v", lastErr)
	}
	t.Fatalf("doc not searchable before timeout: last total: %d", lastTotal)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// compile-time check
var _ = nats.ErrTimeout
