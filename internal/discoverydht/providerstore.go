package discoverydht

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Rorical/IPFSniffer/internal/codec"
	internalnats "github.com/Rorical/IPFSniffer/internal/nats"
	"github.com/Rorical/IPFSniffer/internal/redis"
	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"

	nats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"

	records "github.com/libp2p/go-libp2p-kad-dht/records"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type PublishingProviderStore struct {
	Inner records.ProviderStore

	NATS   nats.JetStreamContext
	Redis  *goredis.Client
	Dedupe redis.Dedupe
}

func (s *PublishingProviderStore) AddProvider(ctx context.Context, key []byte, prov peer.AddrInfo) error {
	if s.Inner == nil {
		return fmt.Errorf("inner provider store required")
	}
	if s.NATS == nil {
		return fmt.Errorf("nats required")
	}
	if s.Redis == nil {
		return fmt.Errorf("redis required")
	}

	cidStr := mhToCIDString(key)
	if cidStr != "" {
		seen, err := s.Dedupe.Seen(ctx, s.Redis, cidStr)
		if err == nil && !seen {
			env := &ipfsnifferv1.CidDiscovered{
				V:  1,
				Id: uuid.NewString(),
				Ts: time.Now().UTC().Format(time.RFC3339Nano),
				Data: &ipfsnifferv1.CidDiscoveredData{
					Cid:          cidStr,
					Source:       "dht",
					SourceDetail: "provider_add",
					PeerId:       prov.ID.String(),
					RemoteAddrs:  peerAddrsToStrings(prov.Addrs),
					ObservedAt:   time.Now().UTC().Format(time.RFC3339Nano),
				},
			}
			b, merr := codec.Marshal(env)
			if merr == nil {
				_, _ = internalnats.Publish(ctx, s.NATS, internalnats.SubjectCidDiscovered, b)
			}
		}
	}

	return s.Inner.AddProvider(ctx, key, prov)
}

func (s *PublishingProviderStore) GetProviders(ctx context.Context, key []byte) ([]peer.AddrInfo, error) {
	if s.Inner == nil {
		return nil, fmt.Errorf("inner provider store required")
	}
	return s.Inner.GetProviders(ctx, key)
}

func (s *PublishingProviderStore) Close() error {
	if s.Inner == nil {
		return nil
	}
	return s.Inner.Close()
}
