package nats

import (
	"context"
	"fmt"
	"time"

	nats "github.com/nats-io/nats.go"
)

type ConnConfig struct {
	URL     string
	Name    string
	Timeout time.Duration
}

func DefaultConnConfig() ConnConfig {
	return ConnConfig{
		URL:     "nats://127.0.0.1:4222",
		Name:    "ipfsniffer",
		Timeout: 5 * time.Second,
	}
}

func Connect(ctx context.Context, cfg ConnConfig) (*nats.Conn, nats.JetStreamContext, error) {
	if cfg.URL == "" {
		cfg.URL = DefaultConnConfig().URL
	}
	if cfg.Name == "" {
		cfg.Name = DefaultConnConfig().Name
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultConnConfig().Timeout
	}

	opts := []nats.Option{
		nats.Name(cfg.Name),
		nats.Timeout(cfg.Timeout),
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream: %w", err)
	}

	return nc, js, nil
}
