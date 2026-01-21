package nats

import (
	"context"
	"fmt"

	nats "github.com/nats-io/nats.go"
)

type Publisher interface {
	Publish(subject string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
}

func Publish(ctx context.Context, js Publisher, subject string, payload []byte) (*nats.PubAck, error) {
	if subject == "" {
		return nil, fmt.Errorf("subject required")
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("payload required")
	}

	ack, err := js.Publish(subject, payload)
	if err != nil {
		return nil, fmt.Errorf("publish %s: %w", subject, err)
	}
	return ack, nil
}

func PublishDLQ(ctx context.Context, js Publisher, subject string, payload []byte) (*nats.PubAck, error) {
	return Publish(ctx, js, DLQSubject(subject), payload)
}
