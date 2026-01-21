package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/Rorical/IPFSniffer/internal/httpjson"
	"github.com/Rorical/IPFSniffer/internal/search"
)

type Searcher interface {
	Search(ctx context.Context, p search.SearchParams) (search.SearchResult, error)
	GetDoc(ctx context.Context, docID string) (json.RawMessage, bool, error)
}

type API struct {
	Search Searcher
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/search", a.handleSearch)
	mux.HandleFunc("/doc/", a.handleDoc)

	h := http.Handler(mux)
	h = OTel(h)
	h = RequestLogging(h)
	return h
}

func (a *API) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpjson.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if a.Search == nil {
		httpjson.Error(w, http.StatusInternalServerError, "search client not configured")
		return
	}

	params, err := parseSearchParams(r.URL.Query())
	if err != nil {
		httpjson.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	res, err := a.Search.Search(r.Context(), params)
	if err != nil {
		if search.IsBadRequest(err) {
			httpjson.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		httpjson.Error(w, http.StatusBadGateway, "search failed")
		return
	}
	httpjson.Write(w, http.StatusOK, res)
}

func (a *API) handleDoc(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpjson.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if a.Search == nil {
		httpjson.Error(w, http.StatusInternalServerError, "search client not configured")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/doc/")
	id = strings.TrimSpace(id)
	if id == "" {
		httpjson.Error(w, http.StatusBadRequest, "missing doc id")
		return
	}

	doc, found, err := a.Search.GetDoc(r.Context(), id)
	if err != nil {
		httpjson.Error(w, http.StatusBadGateway, "doc fetch failed")
		return
	}
	if !found {
		httpjson.Error(w, http.StatusNotFound, "not found")
		return
	}

	httpjson.Write(w, http.StatusOK, map[string]any{"id": id, "doc": doc})
}
