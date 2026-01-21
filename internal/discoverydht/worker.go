package discoverydht

import (
	"context"
	"fmt"
	"time"

	"github.com/Rorical/IPFSniffer/internal/dhtsniff"
	"github.com/Rorical/IPFSniffer/internal/ipnssniff"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/redis"

	ipfs "github.com/Rorical/IPFSniffer/internal/kubo"

	"github.com/ipfs/kubo/core/node/libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	dual "github.com/libp2p/go-libp2p-kad-dht/dual"
	records "github.com/libp2p/go-libp2p-kad-dht/records"
	routing "github.com/libp2p/go-libp2p/core/routing"

	nats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
)

type Worker struct {
	RepoPath string

	NATS   nats.JetStreamContext
	Redis  *goredis.Client
	Dedupe redis.Dedupe
}

func (w *Worker) Run(ctx context.Context) error {
	if w.NATS == nil {
		return fmt.Errorf("nats jetstream required")
	}
	if w.Redis == nil {
		return fmt.Errorf("redis required")
	}
	if w.RepoPath == "" {
		return fmt.Errorf("repoPath required")
	}

	if w.Dedupe.Prefix == "" {
		w.Dedupe.Prefix = "ipfsniffer:seen:cid"
	}
	if w.Dedupe.TTL == 0 {
		w.Dedupe.TTL = 24 * time.Hour
	}

	logger := logging.FromContext(ctx)
	logger.Info("discovery-dht starting")

	routingOpt := func(args libp2p.RoutingOptionArgs) (routing.Routing, error) {
		ds := &dhtsniff.PublishingDatastore{
			Inner: args.Datastore,
			Sniff: &ipnssniff.Sniffer{NATS: w.NATS, Redis: w.Redis, Dedupe: redis.Dedupe{Prefix: w.Dedupe.Prefix + ":dhtds", TTL: w.Dedupe.TTL}},
		}

		pm, err := records.NewProviderManager(args.Ctx, args.Host.ID(), args.Host.Peerstore(), ds)
		if err != nil {
			return nil, fmt.Errorf("provider manager: %w", err)
		}

		wrapped := &PublishingProviderStore{
			Inner:  pm,
			NATS:   w.NATS,
			Redis:  w.Redis,
			Dedupe: w.Dedupe,
		}

		dhtOpts := []dht.Option{
			dht.Concurrency(10),
			dht.Mode(dht.ModeServer),
			dht.Datastore(ds),
			dht.Validator(args.Validator),
			dht.ProviderStore(wrapped),
		}

		wanOpts := []dht.Option{
			dht.BootstrapPeers(args.BootstrapPeers...),
		}

		return dual.New(
			args.Ctx, args.Host,
			dual.DHTOption(dhtOpts...),
			dual.WanDHTOption(wanOpts...),
		)
	}

	// Build node in DHT server mode so we can observe inbound provider records.
	node, err := ipfs.OpenOrInitWithRouting(ctx, w.RepoPath, routingOpt)
	if err != nil {
		return err
	}
	defer node.Close()

	logger.Info("discovery-dht running", "subject", internalnats.SubjectCidDiscovered)
	<-ctx.Done()
	return ctx.Err()
}
