package fetcher

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	boxopath "github.com/ipfs/boxo/path"
	"github.com/ipfs/go-ipld-format"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	ipfs "github.com/Rorical/IPFSniffer/internal/kubo"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
)

type Worker struct {
	IPFS *ipfs.Node
	NATS nats.JetStreamContext

	Durable    string
	MaxDeliver int

	// Retry settings for DHT lookups
	MaxRetries     int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// Retryable error types that should trigger a retry
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// Retry context cancellations and timeouts
	if strings.Contains(errStr, "context canceled") ||
		strings.Contains(errStr, "context deadline exceeded") ||
		strings.Contains(errStr, "i/o deadline") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "no peers") ||
		strings.Contains(errStr, "provider not found") {
		return true
	}
	return false
}

// Calculate delay with exponential backoff and jitter
func calculateRetryDelay(attempt int, baseDelay, maxDelay time.Duration) time.Duration {
	// Exponential backoff: base * 2^attempt
	delay := float64(baseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}
	// Add jitter: +/- 25%
	jitter := delay * 0.25
	jitterNanos := int64(jitter * float64(time.Nanosecond))
	// Random value between -jitter and +jitter
	randomOffset := int64(time.Now().UnixNano()%100-50) * jitterNanos / 50
	return time.Duration(delay) + time.Duration(randomOffset)
}

func (w *Worker) Run(ctx context.Context) error {
	if w.IPFS == nil || w.IPFS.API == nil {
		return fmt.Errorf("ipfs node required")
	}
	if w.NATS == nil {
		return fmt.Errorf("nats jetstream required")
	}

	// Apply defaults
	if w.MaxRetries <= 0 {
		w.MaxRetries = 3
	}
	if w.RetryBaseDelay <= 0 {
		w.RetryBaseDelay = 500 * time.Millisecond
	}
	if w.RetryMaxDelay <= 0 {
		w.RetryMaxDelay = 5 * time.Second
	}

	durable := w.Durable
	if durable == "" {
		durable = "fetcher"
	}

	if err := internalnats.EnsureConsumer(ctx, w.NATS, internalnats.SubjectFetchRequest, durable, w.MaxDeliver); err != nil {
		return err
	}

	sub, err := w.NATS.PullSubscribe(internalnats.SubjectFetchRequest, durable)
	if err != nil {
		return fmt.Errorf("pull subscribe: %w", err)
	}

	logger := logging.FromContext(ctx)
	logger.Info("fetcher started", "subject", internalnats.SubjectFetchRequest, "durable", durable, "max_retries", w.MaxRetries, "retry_base_delay", w.RetryBaseDelay)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			return fmt.Errorf("fetch: %w", err)
		}

		for _, msg := range msgs {
			if err := w.handleMsg(ctx, msg); err != nil {
				logger.Error("handle msg", "err", err)
				continue
			}
			_ = msg.Ack()
		}
	}
}

// resolveNodeWithRetry wraps ResolveNode with retry logic
func (w *Worker) resolveNodeWithRetry(ctx context.Context, ipfsPath boxopath.Path, attempt int) (interface{}, error) {
	// Calculate delay with exponential backoff
	delay := calculateRetryDelay(attempt, w.RetryBaseDelay, w.RetryMaxDelay)
	if attempt > 0 && delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	return w.IPFS.API.ResolveNode(ctx, ipfsPath)
}

func (w *Worker) handleMsg(ctx context.Context, msg *nats.Msg) error {
	var in ipfsnifferv1.FetchRequest
	if err := codec.Unmarshal(msg.Data, &in); err != nil {
		return err
	}

	root := in.GetData().GetRootCid()
	p := in.GetData().GetPath()
	if p == "" {
		p = "/ipfs/" + root
	}

	logger := logging.FromContext(ctx).With("root_cid", root, "path", p)

	// Phase 2: recursively traverse the tree and emit one fetch.result per node.
	// Timeout is honored by deriving a context with deadline (best-effort).
	if ms := in.GetData().GetLimits().GetTimeoutMs(); ms > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer cancel()
		logger.Debug("fetcher: timeout set", "ms", ms)
	}

	ipfsPath, err := parsePath(p)
	if err != nil {
		logger.Error("fetcher: invalid path", "err", err)
		return w.emitFailed(ctx, &in, root, p, "failed", "invalid_path", err)
	}

	pol := buildPolicy(&in)
	lim := buildLimits(&in)
	st := &traverseState{}

	// Enforce max_dag_nodes as raw IPLD blocks during traversal (single-pass).
	// We wrap the DAGService used by unixfs nodes so each underlying Get() counts.
	dagSvc := w.IPFS.Raw.DAG
	if lim.maxDagNodes		dagSvc > 0 {
 = newCountingDAG(dagSvc, lim.maxDagNodes)
	}

	// Resolve the initial IPLD node with retry logic
	var ipldNode format.Node
	var resolveErr error
	for attempt := 0; attempt <= w.MaxRetries; attempt++ {
		var node interface{}
		node, resolveErr = w.resolveNodeWithRetry(ctx, ipfsPath, attempt)
		if resolveErr == nil {
			ipldNode = node.(format.Node)
			break
		}
		if !isRetryableError(resolveErr) {
			// Non-retryable error, fail immediately
			logger.Error("fetcher: resolve failed (non-retryable)", "err", resolveErr, "attempt", attempt)
			return w.emitFailed(ctx, &in, root, p, "failed", "fetch_failed", resolveErr)
		}
		if attempt < w.MaxRetries {
			logger.Warn("fetcher: resolve failed, retrying", "err", resolveErr, "attempt", attempt+1, "max_retries", w.MaxRetries)
		}
	}
	if resolveErr != nil {
		logger.Error("fetcher: resolve failed after all retries", "err", resolveErr)
		return w.emitFailed(ctx, &in, root, p, "failed", "fetch_failed", resolveErr)
	}

	node, err := unixfile.NewUnixfsFile(ctx, dagSvc, ipldNode)
	if err != nil {
		return w.emitFailed(ctx, &in, root, p, "failed", "fetch_failed", err)
	}

	emit := func(d *ipfsnifferv1.FetchResultData) error {
		// Fill common envelope-ish fields and publish.
		if d.FetchedAt == "" {
			d.FetchedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		out := &ipfsnifferv1.FetchResult{
			V:     1,
			Id:    uuid.NewString(),
			Ts:    time.Now().UTC().Format(time.RFC3339Nano),
			Trace: in.Trace,
			Data:  d,
		}
		b, err := codec.Marshal(out)
		if err != nil {
			return err
		}
		if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectFetchResult, b); err != nil {
			_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectFetchResult, b)
			return err
		}
		return nil
	}

	// Depth is computed relative to the starting path.
	if lim.maxDepth > 0 && depthOfPath(p) > int(lim.maxDepth) {
		return emit(&ipfsnifferv1.FetchResultData{RootCid: root, Path: p, NodeType: "unknown", Status: "skipped", SkipReason: "limit_exceeded", Error: "path depth exceeded", Content: &ipfsnifferv1.FetchContentResult{Mode: "none"}, Directory: &ipfsnifferv1.FetchDirectory{Entries: nil, Truncated: false}})
	}

	if err := traverse(ctx, root, p, node, 0, st, lim, pol, emit); err != nil {
		// if traversal exceeded limits, emit a final "failed" record at root.
		_ = emit(&ipfsnifferv1.FetchResultData{RootCid: root, Path: p, NodeType: "unknown", Status: "failed", SkipReason: "limit_exceeded", Error: err.Error(), Content: &ipfsnifferv1.FetchContentResult{Mode: "none"}, Directory: &ipfsnifferv1.FetchDirectory{Entries: nil, Truncated: true}})
		return err
	}

	return nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func depthOfPath(p string) int {
	// /ipfs/<cid>/a/b => depth 2, /ipfs/<cid> => 0
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "/ipfs/") {
		p = strings.TrimPrefix(p, "/ipfs/")
		parts := strings.Split(strings.Trim(p, "/"), "/")
		if len(parts) <= 1 {
			return 0
		}
		// parts[0] is the CID, remainder is path depth.
		return len(parts) - 1
	}
	return strings.Count(strings.Trim(p, "/"), "/")
}

func (w *Worker) emitFailed(ctx context.Context, in *ipfsnifferv1.FetchRequest, root, p, status, reason string, cause error) error {
	res := &ipfsnifferv1.FetchResult{
		V:     1,
		Id:    uuid.NewString(),
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace: in.Trace,
		Data: &ipfsnifferv1.FetchResultData{
			RootCid:    root,
			Path:       p,
			NodeType:   "unknown",
			SizeBytes:  0,
			Mime:       "",
			Ext:        strings.ToLower(filepath.Ext(p)),
			Content:    &ipfsnifferv1.FetchContentResult{Mode: "none"},
			Directory:  &ipfsnifferv1.FetchDirectory{Entries: nil, Truncated: false},
			Status:     status,
			SkipReason: reason,
			Error:      cause.Error(),
			FetchedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	b, err := codec.Marshal(res)
	if err != nil {
		return err
	}

	if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectFetchResult, b); err != nil {
		_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectFetchResult, b)
		return err
	}

	return nil
}
