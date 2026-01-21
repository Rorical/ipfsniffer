package discoveryipnsdht

import (
	"context"
	"fmt"
	"time"

	"github.com/Rorical/IPFSniffer/internal/dhtsniff"
	"github.com/Rorical/IPFSniffer/internal/ipnssniff"
	ipfs "github.com/Rorical/IPFSniffer/internal/kubo"
	"github.com/Rorical/IPFSniffer/internal/logging"
	"github.com/Rorical/IPFSniffer/internal/redis"

	"github.com/ipfs/kubo/core/node/libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	dual "github.com/libp2p/go-libp2p-kad-dht/dual"
	routing "github.com/libp2p/go-libp2p/core/routing"

	nats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
)

// Worker runs an embedded Kubo node with a DHT routing stack whose record
// validator is wrapped to observe IPNS records flowing through the DHT.
//
// This is intended to sniff live IPNS record traffic (PutValue/GetValue) and publish:
// - /ipns/<name> (stable name)
// - /ipfs/<cid>/... (direct fetch target when present in record value)
//
// Note: this must run in DHT server mode to have a chance of seeing inbound
// traffic.
//
// Output: publishes to `cid.discovered`.
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
		w.Dedupe.Prefix = "ipfsniffer:seen:ipns:dht"
	}
	if w.Dedupe.TTL == 0 {
		w.Dedupe.TTL = 24 * time.Hour
	}

	logger := logging.FromContext(ctx)
	logger.Info("discovery-ipns-dht starting")

	routingOpt := func(args libp2p.RoutingOptionArgs) (routing.Routing, error) {
		// We cannot wrap the ipns validator: go-libp2p-kad-dht requires the
		// /ipns validator to be of type ipns.Validator when using the /ipfs DHT.
		// Instead, sniff IPNS record keys/values via the datastore wrapper.

		ds := &dhtsniff.PublishingDatastore{
			Inner: args.Datastore,
			Sniff: &ipnssniff.Sniffer{NATS: w.NATS, Redis: w.Redis, Dedupe: redis.Dedupe{Prefix: w.Dedupe.Prefix + ":ipnsdhtds", TTL: w.Dedupe.TTL}},
		}

		dhtOpts := []dht.Option{
			dht.Concurrency(10),
			dht.Mode(dht.ModeServer),
			dht.Datastore(ds),
			dht.Validator(args.Validator),
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

	node, err := ipfs.OpenOrInitWithRouting(ctx, w.RepoPath, routingOpt)
	if err != nil {
		return err
	}
	defer node.Close()

	logger.Info("discovery-ipns-dht running")
	<-ctx.Done()
	return ctx.Err()
}
