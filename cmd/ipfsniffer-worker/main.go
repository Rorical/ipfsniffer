package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rorical/IPFSniffer/internal/config"
	"github.com/Rorical/IPFSniffer/internal/discovery"
	"github.com/Rorical/IPFSniffer/internal/discoverydht"
	"github.com/Rorical/IPFSniffer/internal/discoveryipnsdht"
	"github.com/Rorical/IPFSniffer/internal/discoveryipnspubsub"
	"github.com/Rorical/IPFSniffer/internal/enqueue"
	"github.com/Rorical/IPFSniffer/internal/extractor"
	"github.com/Rorical/IPFSniffer/internal/fetcher"
	"github.com/Rorical/IPFSniffer/internal/indexer"
	"github.com/Rorical/IPFSniffer/internal/indexprep"
	"github.com/Rorical/IPFSniffer/internal/kubo"
	"github.com/Rorical/IPFSniffer/internal/logging"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/opensearch"
	"github.com/Rorical/IPFSniffer/internal/redis"
	"github.com/Rorical/IPFSniffer/internal/resolver"
	"github.com/Rorical/IPFSniffer/internal/tika"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	logger := logging.New(logging.Config{Level: slog.LevelInfo})
	slog.SetDefault(logger)
	ctx = logging.WithLogger(ctx, logger)

	shutdownOTel, err := logging.InitOTel(ctx, logging.OTelConfig{Insecure: true, ServiceName: "ipfsniffer-worker"})
	if err != nil {
		slog.Error("otel init", "err", err)
		os.Exit(1)
	}
	defer func() {
		_ = shutdownOTel(context.Background())
	}()

	nc, js, err := internalnats.Connect(ctx, cfg.NATS)
	if err != nil {
		slog.Error("nats connect", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()

	if err := internalnats.EnsureStream(ctx, js); err != nil {
		slog.Error("ensure stream", "err", err)
		os.Exit(1)
	}

	role := os.Getenv("IPFSNIFFER_WORKER_ROLE")
	if role == "" {
		role = "discovery-pubsub"
	}

	slog.Info("worker started", "env", cfg.Service.Env, "role", role)

	switch role {
	case "discovery-dht":
		// DHT provider-record capture (server mode).
		// Note: this can increase resource usage and may see limited traffic when not publicly reachable.
		rdb, err := redis.Connect(ctx, redis.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
		if err != nil {
			slog.Error("redis connect", "err", err)
			os.Exit(1)
		}
		defer rdb.Close()

		w := &discoverydht.Worker{
			RepoPath: cfg.Kubo.RepoPath,
			NATS:     js,
			Redis:    rdb,
			Dedupe:   redis.Dedupe{Prefix: "ipfsniffer:seen:cid", TTL: cfg.Discovery.DedupeTTL},
		}
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("discovery-dht run", "err", err)
			os.Exit(1)
		}
	case "discovery-pubsub":
		ipfsNode, err := kubo.OpenOrInitPubSub(ctx, cfg.Kubo.RepoPath)
		if err != nil {
			slog.Error("kubo open", "err", err)
			os.Exit(1)
		}
		defer ipfsNode.Close()

		rdb, err := redis.Connect(ctx, redis.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
		if err != nil {
			slog.Error("redis connect", "err", err)
			os.Exit(1)
		}
		defer rdb.Close()

		w := &discovery.PubSubWorker{
			IPFS:   ipfsNode,
			NATS:   js,
			Redis:  rdb,
			Topics: cfg.Discovery.PubSubTopics,
			Dedupe: redis.Dedupe{Prefix: "ipfsniffer:seen:cid", TTL: cfg.Discovery.DedupeTTL},
		}

		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("worker run", "err", err)
			os.Exit(1)
		}
	case "resolver-ipns":
		ipfsNode, err := kubo.OpenOrInit(ctx, cfg.Kubo.RepoPath)
		if err != nil {
			slog.Error("kubo open", "err", err)
			os.Exit(1)
		}
		defer ipfsNode.Close()

		w := &resolver.IPNSResolverWorker{
			IPFS:       ipfsNode,
			NATS:       js,
			Durable:    "resolver-ipns",
			MaxDeliver: internalnats.DefaultMaxDeliver,
		}

		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("resolver run", "err", err)
			os.Exit(1)
		}
	case "discovery-ipns-dht":
		// DHT validator wrapper to sniff IPNS records.
		rdb, err := redis.Connect(ctx, redis.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
		if err != nil {
			slog.Error("redis connect", "err", err)
			os.Exit(1)
		}
		defer rdb.Close()

		w := &discoveryipnsdht.Worker{
			RepoPath: cfg.Kubo.RepoPath,
			NATS:     js,
			Redis:    rdb,
			Dedupe:   redis.Dedupe{Prefix: "ipfsniffer:seen:ipns:dht", TTL: cfg.Discovery.DedupeTTL},
		}
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("discovery-ipns-dht run", "err", err)
			os.Exit(1)
		}
	case "discovery-ipns-pubsub":
		// Seed per-name IPNS pubsub subscriptions and harvest updates.
		ipfsNode, err := kubo.OpenOrInitIPNSPubSub(ctx, cfg.Kubo.RepoPath)
		if err != nil {
			slog.Error("kubo open", "err", err)
			os.Exit(1)
		}
		defer ipfsNode.Close()
		if ipfsNode.Raw == nil || ipfsNode.Raw.PSRouter == nil {
			slog.Error("ipns pubsub disabled in node")
			os.Exit(1)
		}

		rdb, err := redis.Connect(ctx, redis.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
		if err != nil {
			slog.Error("redis connect", "err", err)
			os.Exit(1)
		}
		defer rdb.Close()

		w := &discoveryipnspubsub.Worker{
			PSRouter: ipfsNode.Raw.PSRouter,
			NATS:     js,
			Redis:    rdb,
			Dedupe:   redis.Dedupe{Prefix: "ipfsniffer:seen:ipns:pubsub", TTL: cfg.Discovery.DedupeTTL},
			Names:    cfg.Discovery.IPNSPubSubNames,
			Poll:     cfg.Discovery.IPNSPubSubPoll,
		}
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("discovery-ipns-pubsub run", "err", err)
			os.Exit(1)
		}
	case "enqueue-fetch":
		rdb, err := redis.Connect(ctx, redis.Config{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
		if err != nil {
			slog.Error("redis connect", "err", err)
			os.Exit(1)
		}
		defer rdb.Close()

		w := &enqueue.FetchEnqueuer{
			NATS:       js,
			Redis:      rdb,
			Dedupe:     redis.Dedupe{Prefix: "ipfsniffer:seen:fetch", TTL: 24 * time.Hour},
			MaxDeliver: internalnats.DefaultMaxDeliver,
			Limits: enqueue.FetchDefaults{
				MaxTotalBytes: cfg.Fetch.MaxTotalBytes,
				MaxFileBytes:  cfg.Fetch.MaxFileBytes,
				MaxDAGNodes:   cfg.Fetch.MaxDAGNodes,
				MaxDepth:      cfg.Fetch.MaxDepth,
				Timeout:       cfg.Fetch.Timeout,
			},
			Inline: enqueue.InlineDefaults{InlineMaxBytes: cfg.Fetch.InlineMaxBytes},
			Policy: enqueue.FetchPolicyDefaults{SkipExt: cfg.Fetch.SkipExt, SkipMimePrefix: cfg.Fetch.SkipMimePrefix},
		}
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("enqueue-fetch run", "err", err)
			os.Exit(1)
		}
	case "fetcher":
		ipfsNode, err := kubo.OpenOrInit(ctx, cfg.Kubo.RepoPath)
		if err != nil {
			slog.Error("kubo open", "err", err)
			os.Exit(1)
		}
		defer ipfsNode.Close()

		w := &fetcher.Worker{
			IPFS:       ipfsNode,
			NATS:       js,
			Durable:    "fetcher",
			MaxDeliver: internalnats.DefaultMaxDeliver,
		}

		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("fetcher run", "err", err)
			os.Exit(1)
		}
	case "stream-server":
		ipfsNode, err := kubo.OpenOrInit(ctx, cfg.Kubo.RepoPath)
		if err != nil {
			slog.Error("kubo open", "err", err)
			os.Exit(1)
		}
		defer ipfsNode.Close()

		srv := &fetcher.StreamServer{IPFS: ipfsNode, NATS: js}
		if err := srv.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("stream server run", "err", err)
			os.Exit(1)
		}
	case "extractor":
		tc := &tika.Client{BaseURL: cfg.Tika.URL}
		w := &extractor.Worker{
			NATS:         js,
			Tika:         tc,
			Durable:      "extractor",
			MaxDeliver:   internalnats.DefaultMaxDeliver,
			TikaTimeout:  cfg.Tika.Timeout,
			MaxTextBytes: cfg.Tika.MaxTextBytes,
		}

		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("extractor run", "err", err)
			os.Exit(1)
		}
	case "index-prep":
		w := &indexprep.Worker{
			NATS:       js,
			Durable:    "index-prep",
			MaxDeliver: internalnats.DefaultMaxDeliver,
			IndexName:  cfg.OpenSearch.Index,
		}
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("index-prep run", "err", err)
			os.Exit(1)
		}
	case "indexer":
		osc, err := opensearch.New(opensearch.Config{URL: cfg.OpenSearch.URL, Insecure: true})
		if err != nil {
			slog.Error("opensearch client", "err", err)
			os.Exit(1)
		}
		_ = indexer.EnsureDefaultIndex(ctx, osc, cfg.OpenSearch.Index)

		w := &indexer.Worker{
			NATS:       js,
			OS:         osc,
			Durable:    "indexer",
			MaxDeliver: internalnats.DefaultMaxDeliver,
			BulkMax:    100,
		}
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("indexer run", "err", err)
			os.Exit(1)
		}
	default:
		slog.Error("unknown role", "role", role)
		os.Exit(2)
	}

	slog.Info("worker shutting down")
}
