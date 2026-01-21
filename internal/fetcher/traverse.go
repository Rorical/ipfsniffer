package fetcher

import (
	"context"
	"encoding/base64"
	"io"
	"path"
	"strings"

	"github.com/Rorical/IPFSniffer/internal/filter"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	boxofiles "github.com/ipfs/boxo/files"
	boxopath "github.com/ipfs/boxo/path"
)

type traverseState struct {
	// best-effort counters for max_total_bytes budgeting.
	totalBytes int64
}

type traverseLimits struct {
	maxTotalBytes  int64
	maxFileBytes   int64
	maxDagNodes    int64
	maxDepth       int64
	inlineMaxBytes int64
}

func buildPolicy(in *ipfsnifferv1.FetchRequest) filter.Policy {
	return filter.Policy{
		SkipExt:        in.GetData().GetPolicy().GetSkipExt(),
		SkipMimePrefix: in.GetData().GetPolicy().GetSkipMimePrefix(),
		MaxFileBytes:   in.GetData().GetLimits().GetMaxFileBytes(),
	}
}

func buildLimits(in *ipfsnifferv1.FetchRequest) traverseLimits {
	return traverseLimits{
		maxTotalBytes:  in.GetData().GetLimits().GetMaxTotalBytes(),
		maxFileBytes:   in.GetData().GetLimits().GetMaxFileBytes(),
		maxDagNodes:    in.GetData().GetLimits().GetMaxDagNodes(),
		maxDepth:       in.GetData().GetLimits().GetMaxDepth(),
		inlineMaxBytes: in.GetData().GetContent().GetInlineMaxBytes(),
	}
}

func traverse(ctx context.Context, rootCID string, basePath string, node boxofiles.Node, depth int64, st *traverseState, lim traverseLimits, pol filter.Policy, emit func(*ipfsnifferv1.FetchResultData) error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if lim.maxDepth > 0 && depth > lim.maxDepth {
		return emit(&ipfsnifferv1.FetchResultData{RootCid: rootCID, Path: basePath, NodeType: "unknown", Status: "skipped", SkipReason: "limit_exceeded", Error: "max_depth exceeded"})
	}

	sizeBytes := int64(0)
	if sz, err := node.Size(); err == nil {
		sizeBytes = sz
	}

	// max_total_bytes is best-effort. For directories this may be cumulative and expensive;
	// we apply it when we know sizeBytes > 0.
	if sizeBytes > 0 {
		if lim.maxTotalBytes > 0 && st.totalBytes+sizeBytes > lim.maxTotalBytes {
			return emit(&ipfsnifferv1.FetchResultData{RootCid: rootCID, Path: basePath, NodeType: "unknown", Status: "skipped", SkipReason: "limit_exceeded", Error: "max_total_bytes exceeded"})
		}
		st.totalBytes += sizeBytes
	}

	nodeType := "file"
	if _, ok := node.(boxofiles.Directory); ok {
		nodeType = "dir"
	}

	// Filter: only meaningful for files (dirs are metadata only).
	ext := strings.ToLower(path.Ext(basePath))
	mime := ""

	decision := filter.Decide(basePath, mime, sizeBytes, pol)

	d := &ipfsnifferv1.FetchResultData{
		RootCid:    rootCID,
		Path:       basePath,
		NodeType:   nodeType,
		SizeBytes:  sizeBytes,
		Mime:       mime,
		Ext:        ext,
		Content:    &ipfsnifferv1.FetchContentResult{Mode: "none"},
		Directory:  &ipfsnifferv1.FetchDirectory{Entries: nil, Truncated: false},
		Status:     "ok",
		SkipReason: "",
		Error:      "",
		FetchedAt:  "",
	}

	if nodeType == "file" {
		if !decision.Allowed {
			d.Status = "skipped"
			d.SkipReason = decision.SkipReason
			return emit(d)
		}

		// Enforce max_file_bytes for files.
		if lim.maxFileBytes > 0 && sizeBytes > lim.maxFileBytes {
			d.Status = "skipped"
			d.SkipReason = "too_large"
			return emit(d)
		}

		// Inline extraction (best-effort) and count bytes read towards max_total_bytes.
		if lim.inlineMaxBytes > 0 {
			f, ok := node.(boxofiles.File)
			if ok {
				// Guard: don't read beyond max_total_bytes budget.
				budget := lim.inlineMaxBytes
				if lim.maxTotalBytes > 0 {
					remaining := lim.maxTotalBytes - st.totalBytes
					if remaining <= 0 {
						d.Status = "skipped"
						d.SkipReason = "limit_exceeded"
						d.Error = "max_total_bytes exceeded"
						return emit(d)
					}
					if remaining < budget {
						budget = remaining
					}
				}

				inlineBytes, readN, err := readUpToN(f, budget)
				if err != nil {
					d.Status = "failed"
					d.Error = err.Error()
					return emit(d)
				}
				st.totalBytes += readN
				if len(inlineBytes) > 0 {
					d.Content.Mode = "inline"
					d.Content.InlineBase64 = base64.StdEncoding.EncodeToString(inlineBytes)
				}
			}
		}
	}

	if nodeType == "dir" {
		dir := node.(boxofiles.Directory)
		it := dir.Entries()

		entries := make([]string, 0, 64)
		truncated := false

		for it.Next() {
			name := it.Name()
			entries = append(entries, name)

			// recursively traverse children
			child := it.Node()
			childPath := basePath
			if strings.HasSuffix(childPath, "/") {
				childPath = strings.TrimSuffix(childPath, "/")
			}
			childPath = childPath + "/" + name

			if err := traverse(ctx, rootCID, childPath, child, depth+1, st, lim, pol, emit); err != nil {
				// if limit exceeded, mark truncation and stop this directory
				if strings.Contains(err.Error(), "max_") {
					truncated = true
					break
				}
				return err
			}
		}
		if err := it.Err(); err != nil {
			d.Status = "failed"
			d.Error = err.Error()
			_ = emit(d)
			return err
		}

		d.Directory.Entries = entries
		d.Directory.Truncated = truncated
		return emit(d)
	}

	// file path: actual inline content is handled by fetcher.go (per request settings)
	return emit(d)
}

func readUpToN(r io.Reader, n int64) ([]byte, int64, error) {
	if n <= 0 {
		return nil, 0, nil
	}
	buf := make([]byte, n)
	readN, err := io.ReadFull(r, buf)
	if err != nil {
		if err != io.ErrUnexpectedEOF && err != io.EOF {
			return nil, 0, err
		}
	}
	return buf[:readN], int64(readN), nil
}

func parsePath(p string) (boxopath.Path, error) {
	return boxopath.NewPath(p)
}
