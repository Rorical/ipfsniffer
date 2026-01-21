package tika

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

type ExtractResult struct {
	Text      []byte
	Truncated bool
}

func (c *Client) ExtractText(ctx context.Context, r io.Reader, timeout time.Duration, maxTextBytes int64) (ExtractResult, error) {
	if c.BaseURL == "" {
		return ExtractResult{}, fmt.Errorf("tika base url required")
	}
	if maxTextBytes <= 0 {
		return ExtractResult{}, fmt.Errorf("maxTextBytes must be > 0")
	}

	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{}
	}

	reqCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	url := c.BaseURL + "/tika"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, url, r)
	if err != nil {
		return ExtractResult{}, err
	}
	req.Header.Set("Accept", "text/plain")

	resp, err := hc.Do(req)
	if err != nil {
		return ExtractResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return ExtractResult{}, fmt.Errorf("tika status %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}

	// Read up to maxTextBytes+1 to detect truncation.
	limited := io.LimitReader(resp.Body, maxTextBytes+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return ExtractResult{}, err
	}

	res := ExtractResult{Text: out, Truncated: false}
	if int64(len(out)) > maxTextBytes {
		res.Text = out[:maxTextBytes]
		res.Truncated = true
	}
	return res, nil
}
