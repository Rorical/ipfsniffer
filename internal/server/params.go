package server

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/Rorical/IPFSniffer/internal/search"
)

func parseSearchParams(v url.Values) (search.SearchParams, error) {
	p := search.ParseSearchParams(v)
	p.Normalize()

	if raw := strings.TrimSpace(v.Get("from")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return search.SearchParams{}, fmt.Errorf("from must be an integer")
		}
		if n < 0 {
			return search.SearchParams{}, fmt.Errorf("from must be >= 0")
		}
		p.From = n
	}

	if raw := strings.TrimSpace(v.Get("size")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return search.SearchParams{}, fmt.Errorf("size must be an integer")
		}
		if n <= 0 {
			return search.SearchParams{}, fmt.Errorf("size must be > 0")
		}
		if n > 100 {
			return search.SearchParams{}, fmt.Errorf("size must be <= 100")
		}
		p.Size = n
	}

	return p, nil
}
