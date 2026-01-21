package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rorical/IPFSniffer/internal/search"
)

type fakeSearch struct {
	searchFn func(ctx context.Context, p search.SearchParams) (search.SearchResult, error)
	getFn    func(ctx context.Context, docID string) (json.RawMessage, bool, error)
}

func (f *fakeSearch) Search(ctx context.Context, p search.SearchParams) (search.SearchResult, error) {
	if f.searchFn == nil {
		return search.SearchResult{}, nil
	}
	return f.searchFn(ctx, p)
}

func (f *fakeSearch) GetDoc(ctx context.Context, docID string) (json.RawMessage, bool, error) {
	if f.getFn == nil {
		return nil, false, nil
	}
	return f.getFn(ctx, docID)
}

func TestSearch_MethodNotAllowed(t *testing.T) {
	api := &API{Search: &fakeSearch{}}
	r := httptest.NewRequest(http.MethodPost, "/search", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d", w.Code)
	}
}

func TestSearch_BadParams(t *testing.T) {
	api := &API{Search: &fakeSearch{}}
	r := httptest.NewRequest(http.MethodGet, "/search?size=999", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d", w.Code)
	}
}

func TestSearch_BadSort_Returns400(t *testing.T) {
	api := &API{Search: &fakeSearch{searchFn: func(ctx context.Context, p search.SearchParams) (search.SearchResult, error) {
		return search.SearchResult{}, errors.Join(search.ErrBadRequest, errors.New("sort: unsupported field"))
	}}}
	r := httptest.NewRequest(http.MethodGet, "/search?sort=nope:asc", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d", w.Code)
	}
}

func TestSearch_BackendFailure_Returns502(t *testing.T) {
	api := &API{Search: &fakeSearch{searchFn: func(ctx context.Context, p search.SearchParams) (search.SearchResult, error) {
		return search.SearchResult{}, errors.New("boom")
	}}}
	r := httptest.NewRequest(http.MethodGet, "/search?q=x", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status %d", w.Code)
	}
}

func TestSearch_Ok(t *testing.T) {
	api := &API{Search: &fakeSearch{searchFn: func(ctx context.Context, p search.SearchParams) (search.SearchResult, error) {
		return search.SearchResult{Total: 1, From: p.From, Size: p.Size, Hits: []search.HitDoc{{ID: "1"}}}, nil
	}}}
	r := httptest.NewRequest(http.MethodGet, "/search?q=x&from=1&size=2", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	if w.Body.Len() == 0 {
		t.Fatalf("expected body")
	}
}

func TestDoc_MissingID(t *testing.T) {
	api := &API{Search: &fakeSearch{}}
	r := httptest.NewRequest(http.MethodGet, "/doc/", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d", w.Code)
	}
}

func TestDoc_NotFound(t *testing.T) {
	api := &API{Search: &fakeSearch{getFn: func(ctx context.Context, docID string) (json.RawMessage, bool, error) {
		return nil, false, nil
	}}}
	r := httptest.NewRequest(http.MethodGet, "/doc/abc", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status %d", w.Code)
	}
}

func TestDoc_BackendError(t *testing.T) {
	api := &API{Search: &fakeSearch{getFn: func(ctx context.Context, docID string) (json.RawMessage, bool, error) {
		return nil, false, errors.New("boom")
	}}}
	r := httptest.NewRequest(http.MethodGet, "/doc/abc", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status %d", w.Code)
	}
}

func TestDoc_Ok(t *testing.T) {
	api := &API{Search: &fakeSearch{getFn: func(ctx context.Context, docID string) (json.RawMessage, bool, error) {
		return json.RawMessage(`{"doc_id":"abc"}`), true, nil
	}}}
	r := httptest.NewRequest(http.MethodGet, "/doc/abc", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	if w.Body.Len() == 0 {
		t.Fatalf("expected body")
	}
}

func TestSearch_BadRequestErrorFromSearchPackage_Returns400(t *testing.T) {
	api := &API{Search: &fakeSearch{searchFn: func(ctx context.Context, p search.SearchParams) (search.SearchResult, error) {
		return search.SearchResult{}, errors.Join(search.ErrBadRequest, errors.New("sort: unsupported field"))
	}}}
	r := httptest.NewRequest(http.MethodGet, "/search?q=x", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d", w.Code)
	}
}
