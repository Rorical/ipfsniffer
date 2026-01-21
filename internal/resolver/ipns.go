package resolver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	ipfs "github.com/Rorical/IPFSniffer/internal/kubo"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
)

type IPNSResolverWorker struct {
	IPFS *ipfs.Node
	NATS nats.JetStreamContext

	Durable    string
	MaxDeliver int
}

func (w *IPNSResolverWorker) Run(ctx context.Context) error {
	if w.IPFS == nil || w.IPFS.API == nil {
		return fmt.Errorf("ipfs node required")
	}
	if w.NATS == nil {
		return fmt.Errorf("nats jetstream required")
	}

	durable := w.Durable
	if durable == "" {
		durable = "resolver-ipns"
	}

	if err := internalnats.EnsureConsumer(ctx, w.NATS, internalnats.SubjectCidDiscovered, durable, w.MaxDeliver); err != nil {
		return err
	}

	sub, err := w.NATS.PullSubscribe(internalnats.SubjectCidDiscovered, durable)
	if err != nil {
		return fmt.Errorf("pull subscribe: %w", err)
	}

	logger := logging.FromContext(ctx)
	logger.Info("resolver started", "subject", internalnats.SubjectCidDiscovered, "durable", durable)

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
				// don't ack; let JetStream retry
				continue
			}
			_ = msg.Ack()
		}
	}
}

func (w *IPNSResolverWorker) handleMsg(ctx context.Context, msg *nats.Msg) error {
	var in ipfsnifferv1.CidDiscovered
	if err := codec.Unmarshal(msg.Data, &in); err != nil {
		return err
	}

	// Phase 1: only resolve if the "cid" looks like an IPNS name or /ipns/... path.
	cand := strings.TrimSpace(in.GetData().GetCid())
	if cand == "" {
		return nil
	}

	if !strings.HasPrefix(cand, "/ipns/") {
		// Nothing to do yet. Later phases will treat cid.discovered as an IPFS CID.
		return nil
	}

	// Kubo coreiface Name().Resolve expects name (string), not path.
	name := strings.TrimPrefix(cand, "/ipns/")

	resolved, err := w.IPFS.API.Name().Resolve(ctx, name)
	if err != nil {
		return fmt.Errorf("ipns resolve %s: %w", name, err)
	}

	// Publish the resolved /ipfs/... path back into cid.discovered.
	// This is the only conversion step; the rest of the pipeline ignores /ipns.
	out := &ipfsnifferv1.CidDiscovered{
		V:     1,
		Id:    uuid.NewString(),
		Ts:    time.Now().UTC().Format(time.RFC3339Nano),
		Trace: in.Trace,
		Data: &ipfsnifferv1.CidDiscoveredData{
			Cid:          resolved.String(),
			Source:       "ipns",
			SourceDetail: "resolved",
			PeerId:       "",
			ObservedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	b, err := codec.Marshal(out)
	if err != nil {
		return err
	}

	if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectCidDiscovered, b); err != nil {
		_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectCidDiscovered, b)
		return err
	}

	return nil
}
