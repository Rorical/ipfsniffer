package ipnssniff

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/redis"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
)

type Sniffer struct {
	NATS   nats.JetStreamContext
	Redis  *goredis.Client
	Dedupe redis.Dedupe
}

func (s *Sniffer) PublishCID(ctx context.Context, cidOrPath, source, sourceDetail, peerID string) error {
	if s.NATS == nil || s.Redis == nil {
		return nil
	}
	if s.Dedupe.Prefix == "" {
		s.Dedupe.Prefix = "ipfsniffer:seen:cid"
	}
	if s.Dedupe.TTL == 0 {
		s.Dedupe.TTL = 24 * time.Hour
	}

	seen, err := s.Dedupe.Seen(ctx, s.Redis, source+":"+cidOrPath)
	if err != nil {
		return err
	}
	if seen {
		return nil
	}

	env := &ipfsnifferv1.CidDiscovered{
		V:  1,
		Id: uuid.NewString(),
		Ts: time.Now().UTC().Format(time.RFC3339Nano),
		Data: &ipfsnifferv1.CidDiscoveredData{
			Cid:          cidOrPath,
			Source:       source,
			SourceDetail: sourceDetail,
			PeerId:       peerID,
			ObservedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	b, err := codec.Marshal(env)
	if err != nil {
		return err
	}
	if _, err := internalnats.Publish(ctx, s.NATS, internalnats.SubjectCidDiscovered, b); err != nil {
		_, _ = internalnats.PublishDLQ(ctx, s.NATS, internalnats.SubjectCidDiscovered, b)
		return err
	}
	return nil
}
