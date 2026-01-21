package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	osclient "github.com/opensearch-project/opensearch-go/v4"
	osapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type Client struct {
	OS *osclient.Client

	// Index can be either an index name or an alias name.
	Index string
}

type SearchParams struct {
	Q string

	From int
	Size int

	RootCID string
	Path    string
	Mime    string
	Ext     string
	Source  string

	// Sort format: field:dir (e.g. processed_at:desc).
	Sort string
}

func (p *SearchParams) Normalize() {
	p.Q = strings.TrimSpace(p.Q)
	p.RootCID = strings.TrimSpace(p.RootCID)
	p.Path = strings.TrimSpace(p.Path)
	p.Mime = strings.TrimSpace(p.Mime)
	p.Ext = strings.TrimSpace(p.Ext)
	p.Source = strings.TrimSpace(p.Source)
	p.Sort = strings.TrimSpace(p.Sort)

	// Note: we keep parsing lenient; stricter validation can be done at the HTTP layer.
	if p.From < 0 {
		p.From = 0
	}
	if p.Size <= 0 {
		p.Size = 20
	}
	if p.Size > 100 {
		p.Size = 100
	}
}

type SearchResult struct {
	Total int      `json:"total"`
	From  int      `json:"from"`
	Size  int      `json:"size"`
	Hits  []HitDoc `json:"hits"`
}

type HitDoc struct {
	ID        string              `json:"id"`
	Score     float32             `json:"score"`
	Doc       json.RawMessage     `json:"doc"`
	Highlight map[string][]string `json:"highlight,omitempty"`
}

func ParseSearchParams(values url.Values) SearchParams {
	p := SearchParams{}
	p.Q = values.Get("q")

	p.From = parseInt(values.Get("from"), 0)
	p.Size = parseInt(values.Get("size"), 20)

	p.RootCID = values.Get("root_cid")
	p.Path = values.Get("path")
	p.Mime = values.Get("mime")
	p.Ext = values.Get("ext")
	p.Source = values.Get("source")
	p.Sort = values.Get("sort")
	return p
}

func (c *Client) Search(ctx context.Context, p SearchParams) (SearchResult, error) {
	ctx, span := tracer().Start(ctx, "Search")
	defer span.End()
	if c.OS == nil {
		return SearchResult{}, fmt.Errorf("opensearch client required")
	}
	if c.Index == "" {
		return SearchResult{}, fmt.Errorf("index required")
	}

	p.Normalize()

	query := buildQuery(p)
	sortSpec, err := parseSort(p)
	if err != nil {
		return SearchResult{}, errors.Join(ErrBadRequest, err)
	}

	body := map[string]any{
		"from":             p.From,
		"size":             p.Size,
		"track_total_hits": true,
		"query":            query,
		"_source":          true,
		// Only include sort when present; OpenSearch rejects sort:null.
	}
	if sortSpec != nil {
		body["sort"] = sortSpec
	}
	body["highlight"] = map[string]any{
		"pre_tags":  []string{"<em>"},
		"post_tags": []string{"</em>"},
		"fields": map[string]any{
			"text":       map[string]any{"fragment_size": 150, "number_of_fragments": 3},
			"names_text": map[string]any{"fragment_size": 80, "number_of_fragments": 2},
			"path_text":  map[string]any{"fragment_size": 80, "number_of_fragments": 2},
		},
	}

	b, _ := json.Marshal(body)
	api := osapi.Client{Client: c.OS}
	resp, err := api.Search(ctx, &osapi.SearchReq{Indices: []string{c.Index}, Body: bytes.NewReader(b)})
	if err != nil {
		return SearchResult{}, err
	}
	if resp.Inspect().Response != nil {
		code := resp.Inspect().Response.StatusCode
		if code < 200 || code >= 300 {
			return SearchResult{}, fmt.Errorf("search http status %d", code)
		}
	}

	out := SearchResult{Total: resp.Hits.Total.Value, From: p.From, Size: p.Size}
	out.Hits = make([]HitDoc, 0, len(resp.Hits.Hits))
	for _, h := range resp.Hits.Hits {
		out.Hits = append(out.Hits, HitDoc{ID: h.ID, Score: h.Score, Doc: h.Source, Highlight: h.Highlight})
	}
	return out, nil
}

func (c *Client) GetDoc(ctx context.Context, docID string) (json.RawMessage, bool, error) {
	ctx, span := tracer().Start(ctx, "GetDoc")
	defer span.End()
	if c.OS == nil {
		return nil, false, fmt.Errorf("opensearch client required")
	}
	if c.Index == "" {
		return nil, false, fmt.Errorf("index required")
	}
	docID = strings.TrimSpace(docID)
	if docID == "" {
		return nil, false, fmt.Errorf("doc_id required")
	}

	// Use the base opensearch client; it can execute any opensearchapi request via Do().
	var out osapi.DocumentGetResp
	res, err := c.OS.Do(ctx, osapi.DocumentGetReq{Index: c.Index, DocumentID: docID}, &out)
	if res != nil {
		defer func() { _ = res.Body.Close() }()
	}
	if err != nil {
		return nil, false, err
	}
	if res != nil {
		switch {
		case res.StatusCode == 404:
			return nil, false, nil
		case res.StatusCode < 200 || res.StatusCode >= 300:
			return nil, false, fmt.Errorf("doc get http status %d", res.StatusCode)
		}
	}

	if !out.Found {
		return nil, false, nil
	}
	return out.Source, true, nil
}

func buildQuery(p SearchParams) map[string]any {
	must := make([]any, 0, 4)
	filter := make([]any, 0, 4)

	// Free-text query.
	if p.Q != "" {
		// Use a bool query with should clauses for fuzzy matching and prefix matching
		// This allows searches like "wiki" to match "wikipedia"
		shouldClauses := make([]any, 0, 4)

		// Exact match with simple_query_string
		shouldClauses = append(shouldClauses, map[string]any{
			"simple_query_string": map[string]any{
				"query":                p.Q,
				"fields":               []string{"text", "names_text", "path_text"},
				"default_operator":     "and",
				"minimum_should_match": "1",
			},
		})

		// Fuzzy match for typos and partial matches
		shouldClauses = append(shouldClauses, map[string]any{
			"multi_match": map[string]any{
				"query":         p.Q,
				"fields":        []string{"text^1", "names_text^2", "path_text^1.5"},
				"type":          "best_fields",
				"fuzziness":     "AUTO",
				"prefix_length": 1,
			},
		})

		// Prefix match for partial words (e.g., "wiki" → "wikipedia")
		shouldClauses = append(shouldClauses, map[string]any{
			"multi_match": map[string]any{
				"query":  p.Q,
				"fields": []string{"text", "names_text", "path_text"},
				"type":   "phrase_prefix",
			},
		})

		must = append(must, map[string]any{
			"bool": map[string]any{
				"should":               shouldClauses,
				"minimum_should_match": "1",
			},
		})
	} else {
		must = append(must, map[string]any{"match_all": map[string]any{}})
	}

	if p.RootCID != "" {
		filter = append(filter, map[string]any{"term": map[string]any{"root_cid": p.RootCID}})
	}
	if p.Path != "" {
		// Use prefix match on the keyword field.
		filter = append(filter, map[string]any{"prefix": map[string]any{"path": p.Path}})
	}
	if p.Mime != "" {
		filter = append(filter, map[string]any{"term": map[string]any{"mime": p.Mime}})
	}
	if p.Ext != "" {
		filter = append(filter, map[string]any{"term": map[string]any{"ext": p.Ext}})
	}
	if p.Source != "" {
		filter = append(filter, map[string]any{"term": map[string]any{"sources": p.Source}})
	}

	return map[string]any{
		"bool": map[string]any{
			"must":   must,
			"filter": filter,
		},
	}
}

func parseSort(p SearchParams) ([]any, error) {
	sort := strings.TrimSpace(p.Sort)
	if sort == "" {
		if strings.TrimSpace(p.Q) == "" {
			sort = "processed_at:desc"
		} else {
			// Keep score-based ordering for real queries.
			return nil, nil
		}
	}

	// Only allow a small, explicit list.
	allowed := map[string]struct{}{
		"processed_at": {},
		"size_bytes":   {},
	}

	field := sort
	dir := "asc"
	if strings.Contains(sort, ":") {
		parts := strings.SplitN(sort, ":", 2)
		field = parts[0]
		dir = parts[1]
	}
	field = strings.TrimSpace(field)
	dir = strings.ToLower(strings.TrimSpace(dir))
	if field == "" {
		return nil, fmt.Errorf("sort: field required")
	}
	if _, ok := allowed[field]; !ok {
		return nil, fmt.Errorf("sort: unsupported field %q", field)
	}
	if dir != "asc" && dir != "desc" {
		return nil, fmt.Errorf("sort: dir must be asc or desc")
	}

	return []any{map[string]any{field: map[string]any{"order": dir}}}, nil
}

func parseInt(s string, def int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return i
}
