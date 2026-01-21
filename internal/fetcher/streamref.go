package fetcher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ipfs/go-cid"
)

// StreamUnixFSContent streams up to maxBytes from a UnixFS file to the provided writer.
//
// This is the building block for "non-inline" extraction: the extractor can request
// a fetcher to stream bytes into Tika without loading the entire file in memory.
//
// TODO: replace this with a tighter in-process DAG reader once extractor is implemented.
func StreamUnixFSContent(ctx context.Context, gatewayBaseURL string, root cid.Cid, p string, maxBytes int64, w io.Writer) (int64, error) {
	if gatewayBaseURL == "" {
		return 0, fmt.Errorf("gatewayBaseURL required")
	}
	if maxBytes <= 0 {
		return 0, fmt.Errorf("maxBytes must be > 0")
	}

	// NOTE: this is a placeholder. For now we keep it as a helper; we do not wire
	// it into the pipeline yet.
	url := fmt.Sprintf("%s/ipfs/%s%s", gatewayBaseURL, root.String(), p)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", maxBytes-1))

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	n, err := io.Copy(w, resp.Body)
	if err != nil {
		return n, err
	}
	if n > maxBytes {
		return n, fmt.Errorf("read beyond maxBytes")
	}
	return n, nil
}
