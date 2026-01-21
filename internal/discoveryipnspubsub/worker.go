package discoveryipnspubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Rorical/IPFSniffer/internal/ipnssniff"
	"github.com/Rorical/IPFSniffer/internal/logging"
	"github.com/Rorical/IPFSniffer/internal/redis"

	psrouter "github.com/libp2p/go-libp2p-pubsub-router"

	nats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
)

// Worker subscribes to IPNS pubsub record topics by actively querying the IPNS DHT namespace.
//
// IMPORTANT: IPNS pubsub is per-name. There is no global IPNS pubsub feed.
// The only way to get IPNS pubsub messages is to subscribe to specific name topics.
// In Kubo, subscriptions are created on first IPNS resolution.
//
// This worker implements a pragmatic strategy:
//   - periodically performs random /ipns lookups (configurable list) via the routing system
//     to force subscriptions in the PSRouter
//   - listens for updates via SearchValue and publishes /ipfs values as cid.discovered
//
// This is still limited, but can provide a steadier stream than arbitrary app pubsub topics
// if you seed it with known-active IPNS names.
type Worker struct {
	PSRouter *psrouter.PubsubValueStore

	NATS  nats.JetStreamContext
	Redis *goredis.Client

	Dedupe redis.Dedupe

	Names []string
	Poll  time.Duration

	Durable    string
	MaxDeliver int
}

func (w *Worker) Run(ctx context.Context) error {
	if w.PSRouter == nil {
		return fmt.Errorf("psrouter required")
	}
	if w.NATS == nil {
		return fmt.Errorf("nats required")
	}
	if w.Redis == nil {
		return fmt.Errorf("redis required")
	}
	if w.Dedupe.Prefix == "" {
		w.Dedupe.Prefix = "ipfsniffer:seen:ipns:pubsub"
	}
	if w.Dedupe.TTL == 0 {
		w.Dedupe.TTL = 24 * time.Hour
	}
	if w.Poll == 0 {
		w.Poll = 10 * time.Minute
	}

	logger := logging.FromContext(ctx)
	logger.Info("discovery-ipns-pubsub started", "names", len(w.Names), "poll", w.Poll)

	sn := &ipnssniff.Sniffer{NATS: w.NATS, Redis: w.Redis, Dedupe: w.Dedupe}

	runOne := func(ctx context.Context, name string) {
		name = strings.TrimSpace(name)
		name = strings.TrimPrefix(name, "/ipns/")
		if name == "" {
			return
		}

		key := "/ipns/" + name
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		ch, err := w.PSRouter.SearchValue(cctx, key)
		if err != nil {
			return
		}
		select {
		case val := <-ch:
			if ipfsPath, ok := ipnssniff.ExtractIPFSPathFromIPNSRecord(val); ok {
				_ = sn.PublishCID(ctx, ipfsPath, "ipns-pubsub", "search_value", "")
			}
		case <-cctx.Done():
			return
		}
	}

	// Kick once on startup.
	for _, name := range w.Names {
		runOne(ctx, name)
	}

	// Poll to keep some subscriptions alive and harvest latest values.
	t := time.NewTicker(w.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			for _, name := range w.Names {
				runOne(ctx, name)
			}
		}
	}
}
