package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type IndexSpec struct {
	IndexName string
	AliasName string
}

func EnsureIndex(ctx context.Context, c *opensearch.Client, spec IndexSpec, mappingJSON []byte) error {
	if spec.IndexName == "" {
		return fmt.Errorf("index name required")
	}
	if spec.AliasName == "" {
		spec.AliasName = "ipfsniffer-docs"
	}

	// Check if index exists
	existsReq := opensearchapi.IndicesExistsReq{Indices: []string{spec.IndexName}}
	existsResp, err := c.Do(ctx, existsReq, nil)
	if err != nil {
		return fmt.Errorf("indices exists: %w", err)
	}
	_ = existsResp.Body.Close()

	if existsResp.StatusCode == 200 {
		return EnsureAlias(ctx, c, spec.AliasName, spec.IndexName)
	}
	if existsResp.StatusCode != 404 {
		return fmt.Errorf("indices exists status %d", existsResp.StatusCode)
	}

	body := bytes.NewReader(mappingJSON)
	createReq := opensearchapi.IndicesCreateReq{Index: spec.IndexName, Body: body}
	createResp, err := c.Do(ctx, createReq, nil)
	if err != nil {
		return fmt.Errorf("indices create: %w", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode < 200 || createResp.StatusCode >= 300 {
		return fmt.Errorf("indices create status %d", createResp.StatusCode)
	}

	return EnsureAlias(ctx, c, spec.AliasName, spec.IndexName)
}

func EnsureAlias(ctx context.Context, c *opensearch.Client, alias, index string) error {
	if alias == "" || index == "" {
		return nil
	}

	payload := map[string]any{
		"actions": []map[string]any{
			{"add": map[string]any{"index": index, "alias": alias}},
		},
	}
	b, _ := json.Marshal(payload)

	req := opensearchapi.AliasesReq{Body: bytes.NewReader(b)}
	res, err := c.Do(ctx, req, nil)
	if err != nil {
		return fmt.Errorf("update aliases: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("update aliases status %d", res.StatusCode)
	}
	return nil
}
