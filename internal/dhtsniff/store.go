package dhtsniff

import (
	"context"
	"strings"

	"github.com/Rorical/IPFSniffer/internal/ipnssniff"
	"github.com/Rorical/IPFSniffer/internal/logging"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	mbase32 "github.com/multiformats/go-base32"
)

// PublishingDatastore wraps a datastore used by the DHT and attempts to
// extract CID-like values from datastore keys.
//
// This mirrors the core trick in https://github.com/Rorical/Radar:
// the DHT encodes content multihashes/CIDs into datastore key segments
// (often base32-raw), so observing datastore operations gives you a broad
// stream of content identifiers moving through the DHT.
//
// Important: this is intentionally conservative to avoid flooding the pipeline.
// We only attempt to parse keys whose first path segment looks like a DHT
// namespace of interest (providers, ipns, pk).
type PublishingDatastore struct {
	Inner datastore.Batching
	Sniff *ipnssniff.Sniffer
}

func (s *PublishingDatastore) sniffKey(ctx context.Context, op string, key datastore.Key, value []byte) {
	if s == nil || s.Sniff == nil {
		return
	}
	parts := key.List()
	if len(parts) < 2 {
		return
	}

	logger := logging.FromContext(ctx).With("op", op, "key", key.String(), "parts", parts)
	logger.Debug("dhtsniff: datastore operation")

	// Heuristic filter to keep noise down.
	switch parts[0] {
	case "providers", "provider", "ipns", "pk":
		logger.Debug("dhtsniff: matched namespace", "namespace", parts[0])
	default:
		return
	}

	// Radar only looked at parts[1]. That's generally where the DHT puts the
	// multihash/CID (base32 raw). We'll mirror that.
	cand := parts[1]
	if cand == "" {
		return
	}

	logger.Debug("dhtsniff: checking candidate", "candidate", cand)

	// Try base32-raw decode first. This often yields CID bytes for the providers
	// namespace, and routing key bytes for ipns.
	if byts, err := mbase32.RawStdEncoding.DecodeString(cand); err == nil {
		logger.Debug("dhtsniff: base32 decode succeeded", "bytes_len", len(byts))
		// providers/provider keys: usually CID bytes.
		if _, id, err := cid.CidFromBytes(byts); err == nil {
			logger.Info("dhtsniff: publishing CID", "cid", id.String(), "namespace", parts[0])
			_ = s.Sniff.PublishCID(ctx, id.String(), "dht", "datastore_"+op+":"+parts[0], "")
			return
		}

		// ipns keys: usually routing key bytes (peer id bytes). We can derive a
		// stable /ipns/<name> path from it.
		if parts[0] == "ipns" {
			logger.Debug("dhtsniff: checking ipns routing key", "bytes_len", len(byts))
			if namePath, ok := ipnssniff.IPNSRoutingKeyBytesToNamePath(byts); ok {
				logger.Info("dhtsniff: publishing IPNS name", "name", namePath)
				_ = s.Sniff.PublishCID(ctx, namePath, "ipns-dht", "datastore_"+op+":routing_key", "")
			}
			if len(value) > 0 {
				logger.Debug("dhtsniff: checking IPNS record value", "bytes_len", len(value))
				if ipfsPath, ok := ipnssniff.ExtractIPFSPathFromIPNSRecord(value); ok {
					logger.Info("dhtsniff: publishing IPFS path from IPNS record", "path", ipfsPath)
					_ = s.Sniff.PublishCID(ctx, ipfsPath, "ipns-dht", "datastore_"+op+":record_value", "")
				}
			}
		}
	}

	// Fallback: segment might already be a CID string (e.g. bafy...)
	found := extractCIDStrings(strings.ToLower(cand))
	logger.Debug("dhtsniff: fallback extracted CIDs", "count", len(found))
	for _, c := range found {
		logger.Info("dhtsniff: publishing CID (fallback)", "cid", c)
		_ = s.Sniff.PublishCID(ctx, c, "dht", "datastore_"+op+":"+parts[0], "")
	}
}

func extractCIDStrings(s string) []string {
	if s == "" {
		return nil
	}

	// Keep this local; cidutil.ExtractCIDStrings expects lowercase, but we want
	// to avoid importing the package just for a single regex.
	//
	// CID decode is the real validation step.
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == '/' || r == ':' || r == '_' || r == '-' || r == '.'
	})
	seen := make(map[string]struct{}, len(fields))
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if _, ok := seen[f]; ok {
			continue
		}
		if _, err := cid.Decode(f); err != nil {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
	}
	return out
}

func (s *PublishingDatastore) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	s.sniffKey(ctx, "get", key, nil)
	return s.Inner.Get(ctx, key)
}

func (s *PublishingDatastore) Has(ctx context.Context, key datastore.Key) (bool, error) {
	s.sniffKey(ctx, "has", key, nil)
	return s.Inner.Has(ctx, key)
}

func (s *PublishingDatastore) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	s.sniffKey(ctx, "getsize", key, nil)
	return s.Inner.GetSize(ctx, key)
}

func (s *PublishingDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	if q.Prefix != "" {
		s.sniffKey(ctx, "query", datastore.NewKey(q.Prefix), nil)
	}
	return s.Inner.Query(ctx, q)
}

func (s *PublishingDatastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	s.sniffKey(ctx, "put", key, value)
	return s.Inner.Put(ctx, key, value)
}

func (s *PublishingDatastore) Delete(ctx context.Context, key datastore.Key) error {
	s.sniffKey(ctx, "delete", key, nil)
	return s.Inner.Delete(ctx, key)
}

func (s *PublishingDatastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return s.Inner.Sync(ctx, prefix)
}

func (s *PublishingDatastore) Close() error {
	return s.Inner.Close()
}

func (s *PublishingDatastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return s.Inner.Batch(ctx)
}
