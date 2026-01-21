package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/cidutil"
	ipfs "github.com/Rorical/IPFSniffer/internal/kubo"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/redis"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type PubSubWorker struct {
	IPFS  *ipfs.Node
	NATS  nats.JetStreamContext
	Redis *goredis.Client

	Topics []string
	Dedupe redis.Dedupe
}

func (w *PubSubWorker) Run(ctx context.Context) error {
	if w.IPFS == nil || w.IPFS.API == nil {
		return fmt.Errorf("ipfs node required")
	}
	if w.NATS == nil {
		return fmt.Errorf("nats jetstream required")
	}
	if w.Redis == nil {
		return fmt.Errorf("redis client required")
	}

	logger := logging.FromContext(ctx)

	if len(w.Topics) == 0 {
		return fmt.Errorf("no pubsub topics configured")
	}

	// Subscribe per-topic.
	for _, topic := range w.Topics {
		topic := topic
		sub, err := w.IPFS.API.PubSub().Subscribe(ctx, topic)
		if err != nil {
			return fmt.Errorf("pubsub subscribe %s: %w", topic, err)
		}

		go func() {
			defer sub.Close()
			logger.Info("pubsub subscribed", "topic", topic)

			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					logger.Error("pubsub next", "topic", topic, "err", err)
					time.Sleep(1 * time.Second)
					continue
				}

				w.handleMessage(ctx, topic, msg.Data(), msg.From().String())
			}
		}()
	}

	<-ctx.Done()
	return ctx.Err()
}

func (w *PubSubWorker) handleMessage(ctx context.Context, topic string, payload []byte, peerID string) {
	logger := logging.FromContext(ctx)

	cids := cidutil.ExtractCIDStrings(string(payload))
	if len(cids) == 0 {
		return
	}

	for _, c := range cids {
		seen, err := w.Dedupe.Seen(ctx, w.Redis, c)
		if err != nil {
			logger.Error("dedupe", "cid", c, "err", err)
			continue
		}
		if seen {
			continue
		}

		env := &ipfsnifferv1.CidDiscovered{
			V:  1,
			Id: newID(),
			Ts: time.Now().UTC().Format(time.RFC3339Nano),
			Data: &ipfsnifferv1.CidDiscoveredData{
				Cid:          c,
				Source:       "pubsub",
				SourceDetail: topic,
				PeerId:       peerID,
				ObservedAt:   time.Now().UTC().Format(time.RFC3339Nano),
			},
		}

		b, err := proto.Marshal(env)
		if err != nil {
			logger.Error("marshal", "cid", c, "err", err)
			continue
		}

		if _, err := internalnats.Publish(ctx, w.NATS, internalnats.SubjectCidDiscovered, b); err != nil {
			logger.Error("publish", "subject", internalnats.SubjectCidDiscovered, "cid", c, "err", err)
			// best-effort DLQ for publish failures
			_, _ = internalnats.PublishDLQ(ctx, w.NATS, internalnats.SubjectCidDiscovered, b)
			continue
		}

		logger.Debug("cid discovered", slog.String("cid", c), slog.String("topic", topic))
	}
}

func newID() string {
	return uuid.NewString()
}
