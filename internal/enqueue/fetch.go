package enqueue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/redis"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
)

// FetchEnqueuer consumes cid.discovered and publishes fetch.request.
//
// This is the missing link that turns sniffed CIDs into actual fetching+indexing work.
type FetchEnqueuer struct {
	NATS  nats.JetStreamContext
	Redis *goredis.Client

	Dedupe redis.Dedupe

	Durable    string
	MaxDeliver int

	// Default limits/policy for fetch jobs.
	Limits FetchDefaults
	Policy FetchPolicyDefaults
	Inline InlineDefaults
}

type FetchDefaults struct {
	MaxTotalBytes int64
	MaxFileBytes  int64
	MaxDAGNodes   int64
	MaxDepth      int64
	Timeout       time.Duration
}

type FetchPolicyDefaults struct {
	SkipExt        []string
	SkipMimePrefix []string
}

type InlineDefaults struct {
	InlineMaxBytes int64
}

func (w *FetchEnqueuer) Run(ctx context.Context) error {
	if w.NATS == nil {
		return fmt.Errorf("nats jetstream required")
	}
	if w.Redis == nil {
		return fmt.Errorf("redis required")
	}
	if w.Dedupe.Prefix == "" {
		w.Dedupe.Prefix = "ipfsniffer:seen:fetch"
	}
	if w.Dedupe.TTL == 0 {
		w.Dedupe.TTL = 24 * time.Hour
	}
	w.applyDefaults()

	logger := logging.FromContext(ctx)

	durable := w.Durable
	if durable == "" {
		durable = "enqueue-fetch"
	}

	if err := internalnats.EnsureConsumer(ctx, w.NATS, internalnats.SubjectCidDiscovered, durable, w.MaxDeliver); err != nil {
		return err
	}

	subD, err := w.NATS.PullSubscribe(internalnats.SubjectCidDiscovered, durable)
	if err != nil {
		return fmt.Errorf("pull subscribe discovered: %w", err)
	}

	logger.Info("fetch enqueuer started", "subject", internalnats.SubjectCidDiscovered)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := w.pollDiscovered(ctx, subD); err != nil {
			return err
		}
	}
}

func (w *FetchEnqueuer) pollDiscovered(ctx context.Context, sub *nats.Subscription) error {
	msgs, err := sub.Fetch(1, nats.MaxWait(1*time.Second))
	if err != nil {
		if err == nats.ErrTimeout {
			return nil
		}
		return fmt.Errorf("fetch discovered: %w", err)
	}
	for _, msg := range msgs {
		if err := w.handleDiscovered(ctx, msg); err != nil {
			logging.FromContext(ctx).Error("handle discovered", "err", err)
			continue
		}
		_ = msg.Ack()
	}
	return nil
}

func (w *FetchEnqueuer) handleDiscovered(ctx context.Context, msg *nats.Msg) error {
	var in ipfsnifferv1.CidDiscovered
	if err := codec.Unmarshal(msg.Data, &in); err != nil {
		return err
	}
	d := in.GetData()
	if d == nil {
		return nil
	}
	logger := logging.FromContext(ctx).With("cid", d.GetCid(), "source", d.GetSource(), "source_detail", d.GetSourceDetail())
	logger.Debug("enqueue-fetch: received cid.discovered")

	// Datastore-level DHT sniffing produces both fetchable provider records
	// and internal DHT bookkeeping (IPNS routing keys, peer keys). We block
	// the internal bookkeeping but allow provider records since they point to
	// actual content that can be fetched.
	if d.GetSource() == "dht" && strings.HasPrefix(d.GetSourceDetail(), "datastore_") {
		// Allow provider records (these are the content we actually want to fetch)
		if strings.Contains(d.GetSourceDetail(), ":providers") || strings.Contains(d.GetSourceDetail(), ":provider") {
			logger.Debug("enqueue-fetch: allowing provider record")
			// proceed to enqueue
		} else {
			// Block internal DHT bookkeeping (IPNS routing keys, peer keys, etc.)
			logger.Debug("enqueue-fetch: blocking internal DHT bookkeeping")
			return nil
		}
	}

	cand := strings.TrimSpace(d.GetCid())
	if cand == "" {
		logger.Debug("enqueue-fetch: empty cid, skipping")
		return nil
	}
	// Enqueue only direct /ipfs/<cid> paths or bare CIDs. Ignore /ipns.
	rootCID, path := normalizeToFetchTarget(cand)
	if rootCID == "" {
		logger.Debug("enqueue-fetch: not a fetch target, skipping")
		return nil
	}

	logger.Info("enqueue-fetch: enqueuing fetch request", "root_cid", rootCID, "path", path)
	return w.enqueueFetch(ctx, in.Trace, rootCID, path, d.GetObservedAt())
}

func (w *FetchEnqueuer) enqueueFetch(ctx context.Context, trace *ipfsnifferv1.TraceContext, rootCID, path string, observedAt string) error {
	// Per-target dedupe so we don't enqueue infinite work for hot CIDs.
	key := rootCID + ":" + path
	seen, err := w.Dedupe.Seen(ctx, w.Redis, key)
	if err != nil {
		return err
	}
	if seen {
		return nil
	}

	env := &ipfsnifferv1.FetchRequest{
		V:     1,
		Id:    uuid.NewString(),
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace: trace,
		Data: &ipfsnifferv1.FetchRequestData{
			RootCid:    rootCID,
			Path:       path,
			ObservedAt: observedAt,
			Limits: &ipfsnifferv1.FetchLimits{
				MaxTotalBytes: w.Limits.MaxTotalBytes,
				MaxFileBytes:  w.Limits.MaxFileBytes,
				MaxDagNodes:   w.Limits.MaxDAGNodes,
				MaxDepth:      w.Limits.MaxDepth,
				TimeoutMs:     w.Limits.Timeout.Milliseconds(),
			},
			Policy: &ipfsnifferv1.FetchPolicy{
				SkipExt:        w.Policy.SkipExt,
				SkipMimePrefix: w.Policy.SkipMimePrefix,
			},
			Content: &ipfsnifferv1.FetchContent{
				InlineMaxBytes: w.Inline.InlineMaxBytes,
			},
		},
	}

	b, err := codec.Marshal(env)
	if err != nil {
		return err
	}
	if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectFetchRequest, b); err != nil {
		_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectFetchRequest, b)
		return err
	}
	return nil
}

func (w *FetchEnqueuer) applyDefaults() {
	if w.Limits.MaxTotalBytes == 0 {
		w.Limits.MaxTotalBytes = 100 * 1024 * 1024
	}
	if w.Limits.MaxFileBytes == 0 {
		w.Limits.MaxFileBytes = 10 * 1024 * 1024
	}
	if w.Limits.MaxDAGNodes == 0 {
		w.Limits.MaxDAGNodes = 200000
	}
	if w.Limits.MaxDepth == 0 {
		w.Limits.MaxDepth = 64
	}
	if w.Limits.Timeout == 0 {
		// Network-sniffed CIDs are often not fetchable; keep per-job timeouts short
		// so we don't stall the entire pipeline.
		w.Limits.Timeout = 30 * time.Second
	}
	if w.Inline.InlineMaxBytes == 0 {
		w.Inline.InlineMaxBytes = 256 * 1024
	}
	if w.Policy.SkipExt == nil {
		w.Policy.SkipExt = []string{".zip", ".tar", ".gz", ".tgz", ".mp4", ".mp3", ".png", ".jpg", ".jpeg", ".gif", ".webp"}
	}
	if w.Policy.SkipMimePrefix == nil {
		w.Policy.SkipMimePrefix = []string{"video/", "audio/", "image/"}
	}
}

func normalizeToFetchTarget(s string) (rootCID, path string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	if strings.HasPrefix(s, "/ipfs/") {
		rest := strings.TrimPrefix(s, "/ipfs/")
		parts := strings.SplitN(rest, "/", 2)
		root := parts[0]
		if root == "" {
			return "", ""
		}
		return root, s
	}
	// bare CID
	if strings.HasPrefix(s, "bafy") || strings.HasPrefix(s, "Qm") {
		return s, "/ipfs/" + s
	}
	return "", ""
}
