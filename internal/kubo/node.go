package kubo

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	icore "github.com/ipfs/kubo/core/coreiface"

	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	"github.com/ipfs/kubo/core/node/libp2p"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo/fsrepo"
)

type Node struct {
	RepoPath string
	API      icore.CoreAPI
	Raw      *core.IpfsNode

	repo io.Closer
}

var loadPluginsOnce sync.Once

func ensurePlugins(externalPluginsPath string) error {
	// externalPluginsPath is expected to contain a "plugins" subdir.
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("plugin loader: %w", err)
	}
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("plugin init: %w", err)
	}
	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("plugin inject: %w", err)
	}
	return nil
}

type Options struct {
	EnablePubSub     bool
	EnableIPNSPubSub bool
}

// OpenOrInit opens an existing repo at repoPath or initializes it with defaults.
func OpenOrInit(ctx context.Context, repoPath string) (*Node, error) {
	return OpenOrInitWithRouting(ctx, repoPath, libp2p.DHTClientOption)
}

// OpenOrInitPubSub enables Kubo pubsub support (required by discovery-pubsub).
func OpenOrInitPubSub(ctx context.Context, repoPath string) (*Node, error) {
	return OpenOrInitWithRoutingAndOptions(ctx, repoPath, libp2p.DHTClientOption, Options{EnablePubSub: true})
}

// OpenOrInitIPNSPubSub enables Kubo's IPNS-over-PubSub subsystem (PSRouter).
// This implicitly requires pubsub.
func OpenOrInitIPNSPubSub(ctx context.Context, repoPath string) (*Node, error) {
	return OpenOrInitWithRoutingAndOptions(ctx, repoPath, libp2p.DHTClientOption, Options{EnableIPNSPubSub: true})
}

// OpenOrInitWithRouting opens an existing repo at repoPath or initializes it with defaults.
// The routing option controls DHT client vs server participation.
func OpenOrInitWithRouting(ctx context.Context, repoPath string, routingOpt libp2p.RoutingOption) (*Node, error) {
	return OpenOrInitWithRoutingAndOptions(ctx, repoPath, routingOpt, Options{})
}

func OpenOrInitWithRoutingAndOptions(ctx context.Context, repoPath string, routingOpt libp2p.RoutingOption, opts Options) (*Node, error) {
	if repoPath == "" {
		return nil, fmt.Errorf("repoPath required")
	}

	loadPluginsOnce.Do(func() {
		_ = ensurePlugins("")
	})

	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		if err := os.MkdirAll(repoPath, 0o700); err != nil {
			return nil, fmt.Errorf("mkdir repo: %w", err)
		}
	}

	if !fsrepo.IsInitialized(repoPath) {
		cfg, err := config.Init(io.Discard, 2048)
		if err != nil {
			return nil, fmt.Errorf("kubo config init: %w", err)
		}

		// PubSub is an explicit opt-in experiment; only enable for roles that need it.
		if opts.EnablePubSub || opts.EnableIPNSPubSub {
			// Router default is gossipsub.
			cfg.Pubsub.Enabled = config.True
		}
		if opts.EnableIPNSPubSub {
			cfg.Ipns.UsePubsub = config.True
		}

		// Ensure the embedded Kubo datastore cannot grow without bound.
		// Defaults in Kubo are 10GB and 90%, but we explicitly set a smaller cap for this project.
		// Override via env if needed.
		cfg.Datastore.StorageMax = getenvDefault("IPFSNIFFER_KUBO_STORAGE_MAX", "5GB")
		cfg.Datastore.StorageGCWatermark = getenvIntDefault("IPFSNIFFER_KUBO_GC_WATERMARK", 80)

		// Safety: do not expose HTTP API/gateway by default (worker doesn't serve HTTP).
		cfg.Addresses.API = []string{"/ip4/127.0.0.1/tcp/0"}
		cfg.Addresses.Gateway = []string{"/ip4/127.0.0.1/tcp/0"}

		// Initialize repo.
		if err := fsrepo.Init(repoPath, cfg); err != nil {
			return nil, fmt.Errorf("fsrepo init: %w", err)
		}
	}

	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, fmt.Errorf("fsrepo open: %w", err)
	}

	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: routingOpt,
		Repo:    repo,
	}
	if opts.EnablePubSub || opts.EnableIPNSPubSub {
		if nodeOptions.ExtraOpts == nil {
			nodeOptions.ExtraOpts = map[string]bool{}
		}
		nodeOptions.ExtraOpts["pubsub"] = true
	}
	if opts.EnableIPNSPubSub {
		if nodeOptions.ExtraOpts == nil {
			nodeOptions.ExtraOpts = map[string]bool{}
		}
		nodeOptions.ExtraOpts["ipnsps"] = true
	}

	raw, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		_ = repo.Close()
		return nil, fmt.Errorf("core new node: %w", err)
	}

	api, err := coreapi.NewCoreAPI(raw)
	if err != nil {
		raw.Close()
		_ = repo.Close()
		return nil, fmt.Errorf("coreapi: %w", err)
	}

	return &Node{
		RepoPath: repoPath,
		API:      api,
		Raw:      raw,
		repo:     repo,
	}, nil
}

func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	if n.Raw != nil {
		n.Raw.Close()
	}
	if n.repo != nil {
		return n.repo.Close()
	}
	return nil
}

func getenvDefault(key, def string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return def
	}
	return v
}

func getenvIntDefault(key string, def int64) int64 {
	v := getenvDefault(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}
