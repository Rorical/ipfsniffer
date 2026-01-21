package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadFromEnv_Defaults(t *testing.T) {
	// Avoid leaking env from developer machine.
	clear := []string{
		"IPFSNIFFER_ENV",
		"IPFSNIFFER_NATS_URL",
		"IPFSNIFFER_NATS_NAME",
		"IPFSNIFFER_NATS_TIMEOUT",
		"IPFSNIFFER_REDIS_ADDR",
		"IPFSNIFFER_REDIS_PASSWORD",
		"IPFSNIFFER_REDIS_DB",
		"IPFSNIFFER_DISCOVERY_PUBSUB_TOPICS",
		"IPFSNIFFER_DISCOVERY_DEDUPE_TTL",
		"IPFSNIFFER_OPENSEARCH_URL",
		"IPFSNIFFER_OPENSEARCH_INDEX",
		"IPFSNIFFER_TIKA_URL",
		"IPFSNIFFER_TIKA_TIMEOUT",
		"IPFSNIFFER_TIKA_MAX_TEXT_BYTES",
		"IPFSNIFFER_KUBO_REPO",
	}
	for _, k := range clear {
		_ = os.Unsetenv(k)
	}

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}

	if cfg.Service.Env == "" {
		t.Fatalf("expected env")
	}
	if cfg.NATS.URL == "" {
		t.Fatalf("expected nats url")
	}
	if cfg.Redis.Addr == "" {
		t.Fatalf("expected redis addr")
	}
	if cfg.OpenSearch.URL == "" {
		t.Fatalf("expected opensearch url")
	}
	if cfg.OpenSearch.Index == "" {
		t.Fatalf("expected opensearch index")
	}
	if cfg.Tika.URL == "" {
		t.Fatalf("expected tika url")
	}
	if cfg.Tika.Timeout <= 0 {
		t.Fatalf("expected tika timeout")
	}
	if cfg.Tika.MaxTextBytes <= 0 {
		t.Fatalf("expected max text bytes")
	}
	if cfg.Kubo.RepoPath == "" {
		t.Fatalf("expected kubo repo")
	}

	if len(cfg.Discovery.PubSubTopics) == 0 {
		t.Fatalf("expected default pubsub topics")
	}
}

func TestLoadFromEnv_OverridesAndValidation(t *testing.T) {
	_ = os.Setenv("IPFSNIFFER_ENV", "test")
	_ = os.Setenv("IPFSNIFFER_REDIS_ADDR", "127.0.0.1:9999")
	_ = os.Setenv("IPFSNIFFER_REDIS_DB", "2")
	_ = os.Setenv("IPFSNIFFER_DISCOVERY_PUBSUB_TOPICS", "a,b, c")
	_ = os.Setenv("IPFSNIFFER_DISCOVERY_DEDUPE_TTL", "2h")
	_ = os.Setenv("IPFSNIFFER_TIKA_TIMEOUT", "3s")
	_ = os.Setenv("IPFSNIFFER_TIKA_MAX_TEXT_BYTES", "123")
	_ = os.Setenv("IPFSNIFFER_OPENSEARCH_INDEX", "idx")
	_ = os.Setenv("IPFSNIFFER_KUBO_REPO", "/tmp/ipfsrepo")
	defer func() {
		_ = os.Unsetenv("IPFSNIFFER_ENV")
		_ = os.Unsetenv("IPFSNIFFER_REDIS_ADDR")
		_ = os.Unsetenv("IPFSNIFFER_REDIS_DB")
		_ = os.Unsetenv("IPFSNIFFER_DISCOVERY_PUBSUB_TOPICS")
		_ = os.Unsetenv("IPFSNIFFER_DISCOVERY_DEDUPE_TTL")
		_ = os.Unsetenv("IPFSNIFFER_TIKA_TIMEOUT")
		_ = os.Unsetenv("IPFSNIFFER_TIKA_MAX_TEXT_BYTES")
		_ = os.Unsetenv("IPFSNIFFER_OPENSEARCH_INDEX")
		_ = os.Unsetenv("IPFSNIFFER_KUBO_REPO")
	}()

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.Service.Env != "test" {
		t.Fatalf("env: got %q", cfg.Service.Env)
	}
	if cfg.Redis.Addr != "127.0.0.1:9999" {
		t.Fatalf("redis addr: got %q", cfg.Redis.Addr)
	}
	if cfg.Redis.DB != 2 {
		t.Fatalf("redis db: got %d", cfg.Redis.DB)
	}
	if got := cfg.Discovery.PubSubTopics; len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("topics: %+v", got)
	}
	if cfg.Discovery.DedupeTTL != 2*time.Hour {
		t.Fatalf("dedupe ttl: %v", cfg.Discovery.DedupeTTL)
	}
	if cfg.Tika.Timeout != 3*time.Second {
		t.Fatalf("tika timeout: %v", cfg.Tika.Timeout)
	}
	if cfg.Tika.MaxTextBytes != 123 {
		t.Fatalf("max text bytes: %d", cfg.Tika.MaxTextBytes)
	}
	if cfg.OpenSearch.Index != "idx" {
		t.Fatalf("index: %q", cfg.OpenSearch.Index)
	}
	if cfg.Kubo.RepoPath != "/tmp/ipfsrepo" {
		t.Fatalf("repo: %q", cfg.Kubo.RepoPath)
	}
}

func TestLoadFromEnv_BadTimeout(t *testing.T) {
	_ = os.Setenv("IPFSNIFFER_NATS_TIMEOUT", "notaduration")
	defer os.Unsetenv("IPFSNIFFER_NATS_TIMEOUT")

	_, err := LoadFromEnv()
	if err == nil {
		t.Fatalf("expected error")
	}
}
