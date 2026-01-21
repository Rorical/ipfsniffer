package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
)

type Config struct {
	NATS internalnats.ConnConfig

	Redis RedisConfig

	Discovery DiscoveryConfig
	Fetch     FetchConfig

	OpenSearch OpenSearchConfig
	Tika       TikaConfig

	Kubo KuboConfig

	Service ServiceConfig
}

type ServiceConfig struct {
	Env string
}

type RedisConfig struct {
	Addr     string
	DB       int
	Password string
}

type DiscoveryConfig struct {
	PubSubTopics []string
	DedupeTTL    time.Duration

	// IPNSPubSubNames is a seed list of IPNS names to resolve via PSRouter.
	// There is no global IPNS pubsub feed; we must subscribe per-name.
	IPNSPubSubNames []string
	IPNSPubSubPoll  time.Duration
}

type FetchConfig struct {
	MaxTotalBytes  int64
	MaxFileBytes   int64
	MaxDAGNodes    int64
	MaxDepth       int64
	Timeout        time.Duration
	InlineMaxBytes int64

	SkipExt        []string
	SkipMimePrefix []string
}

type OpenSearchConfig struct {
	URL   string
	Index string
}

type TikaConfig struct {
	URL          string
	Timeout      time.Duration
	MaxTextBytes int64
}

type KuboConfig struct {
	RepoPath string
}

func LoadFromEnv() (Config, error) {
	cfg := Config{}

	cfg.Service.Env = getenv("IPFSNIFFER_ENV", "dev")

	cfg.NATS = internalnats.DefaultConnConfig()
	cfg.NATS.URL = getenv("IPFSNIFFER_NATS_URL", cfg.NATS.URL)
	cfg.NATS.Name = getenv("IPFSNIFFER_NATS_NAME", cfg.NATS.Name)
	if d := getenv("IPFSNIFFER_NATS_TIMEOUT", ""); d != "" {
		dur, err := time.ParseDuration(d)
		if err != nil {
			return Config{}, fmt.Errorf("IPFSNIFFER_NATS_TIMEOUT: %w", err)
		}
		cfg.NATS.Timeout = dur
	}

	cfg.Redis.Addr = getenv("IPFSNIFFER_REDIS_ADDR", "127.0.0.1:6379")
	cfg.Redis.Password = getenv("IPFSNIFFER_REDIS_PASSWORD", "")
	cfg.Redis.DB = getenvInt("IPFSNIFFER_REDIS_DB", 0)

	cfg.Discovery.PubSubTopics = splitCSV(getenv("IPFSNIFFER_DISCOVERY_PUBSUB_TOPICS", ""))
	if len(cfg.Discovery.PubSubTopics) == 0 {
		cfg.Discovery.PubSubTopics = []string{"ipfs.pubsub.chat", "fil"}
	}
	cfg.Discovery.DedupeTTL = getenvDuration("IPFSNIFFER_DISCOVERY_DEDUPE_TTL", 24*time.Hour)

	cfg.Discovery.IPNSPubSubNames = splitCSV(getenv("IPFSNIFFER_DISCOVERY_IPNS_PUBSUB_NAMES", ""))
	cfg.Discovery.IPNSPubSubPoll = getenvDuration("IPFSNIFFER_DISCOVERY_IPNS_PUBSUB_POLL", 10*time.Minute)

	cfg.Fetch.MaxTotalBytes = getenvInt64("IPFSNIFFER_FETCH_MAX_TOTAL_BYTES", 100*1024*1024)
	cfg.Fetch.MaxFileBytes = getenvInt64("IPFSNIFFER_FETCH_MAX_FILE_BYTES", 10*1024*1024)
	cfg.Fetch.MaxDAGNodes = getenvInt64("IPFSNIFFER_FETCH_MAX_DAG_NODES", 200000)
	cfg.Fetch.MaxDepth = getenvInt64("IPFSNIFFER_FETCH_MAX_DEPTH", 64)
	cfg.Fetch.Timeout = getenvDuration("IPFSNIFFER_FETCH_TIMEOUT", 10*time.Minute)
	cfg.Fetch.InlineMaxBytes = getenvInt64("IPFSNIFFER_FETCH_INLINE_MAX_BYTES", 256*1024)
	cfg.Fetch.SkipExt = splitCSV(getenv("IPFSNIFFER_FETCH_SKIP_EXT", ".zip,.tar,.gz,.tgz,.mp4,.mp3,.png,.jpg,.jpeg,.gif,.webp"))
	cfg.Fetch.SkipMimePrefix = splitCSV(getenv("IPFSNIFFER_FETCH_SKIP_MIME_PREFIX", "video/,audio/,image/"))

	cfg.OpenSearch.URL = getenv("IPFSNIFFER_OPENSEARCH_URL", "http://127.0.0.1:9200")
	cfg.OpenSearch.Index = getenv("IPFSNIFFER_OPENSEARCH_INDEX", "ipfsniffer-docs-v1")

	cfg.Tika.URL = getenv("IPFSNIFFER_TIKA_URL", "http://127.0.0.1:9998")
	cfg.Tika.Timeout = getenvDuration("IPFSNIFFER_TIKA_TIMEOUT", 60*time.Second)
	cfg.Tika.MaxTextBytes = getenvInt64("IPFSNIFFER_TIKA_MAX_TEXT_BYTES", 2_000_000)

	cfg.Kubo.RepoPath = getenv("IPFSNIFFER_KUBO_REPO", defaultKuboRepo())

	return cfg, nil
}

func getenv(key, def string) string {
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

func getenvInt(key string, def int) int {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func getenvInt64(key string, def int64) int64 {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return i
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func defaultKuboRepo() string {

	home, err := os.UserHomeDir()
	if err != nil {
		return ".ipfs"
	}
	return home + string(os.PathSeparator) + ".ipfs"
}
