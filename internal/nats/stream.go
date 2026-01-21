package nats

import (
	"context"
	"fmt"
	"strings"

	nats "github.com/nats-io/nats.go"
)

const DefaultMaxDeliver = 5

func EnsureStream(ctx context.Context, js nats.JetStreamContext) error {
	stream, err := js.StreamInfo(StreamName)
	if err == nil && stream != nil {
		return nil
	}
	if err != nil && err != nats.ErrStreamNotFound {
		return fmt.Errorf("lookup stream: %w", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      StreamName,
		Subjects:  PipelineSubjects,
		Retention: nats.LimitsPolicy,
		Storage:   nats.FileStorage,
		MaxMsgs:   -1,
		MaxBytes:  -1,
		MaxAge:    0,
	})
	if err != nil {
		return fmt.Errorf("add stream: %w", err)
	}

	for _, subject := range PipelineSubjects {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:      dlqStreamName(subject),
			Subjects:  []string{DLQSubject(subject)},
			Retention: nats.LimitsPolicy,
			Storage:   nats.FileStorage,
			MaxMsgs:   -1,
			MaxBytes:  -1,
			MaxAge:    0,
		})
		if err != nil && err != nats.ErrStreamNameAlreadyInUse {
			return fmt.Errorf("add dlq stream for %s: %w", subject, err)
		}
	}

	return nil
}

func EnsureConsumer(ctx context.Context, js nats.JetStreamContext, subject, durable string, maxDeliver int) error {
	if maxDeliver <= 0 {
		maxDeliver = DefaultMaxDeliver
	}

	cfg := &nats.ConsumerConfig{
		Durable:       durable,
		Description:   fmt.Sprintf("%s consumer", subject),
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subject,
		MaxDeliver:    maxDeliver,
	}

	_, err := js.AddConsumer(StreamName, cfg)
	if err != nil && err != nats.ErrConsumerNameAlreadyInUse {
		return fmt.Errorf("add consumer %s: %w", durable, err)
	}

	return nil
}

func dlqStreamName(subject string) string {
	// JetStream stream names must be alphanumeric with limited punctuation. Subjects contain '.' and '*' so we normalize.
	n := subject
	n = strings.ReplaceAll(n, ".", "_")
	n = strings.ReplaceAll(n, "*", "STAR")
	return fmt.Sprintf("%s_%s_DLQ", StreamName, n)
}
