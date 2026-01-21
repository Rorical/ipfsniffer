package ipnssniff

import (
	"context"
	"strings"

	record "github.com/libp2p/go-libp2p-record"
)

// SniffingValidator wraps a libp2p record.Validator and emits cid.discovered
// when it encounters valid IPNS records (from DHT PutValue/GetValue flows).
//
// It publishes:
// - the IPNS name as "/ipns/<name>" (stable)
// - and, when the record value points to /ipfs, publishes that /ipfs path (direct fetch target)
type SniffingValidator struct {
	Inner record.Validator
	Sniff *Sniffer
}

// WrapIPNSValidator preserves namespacing when the underlying validator is a
// record.NamespacedValidator.
//
// Kubo's /ipfs DHT requires a namespaced validator; wrapping the whole
// validator breaks that invariant and prevents the node from starting.
//
// This function wraps only the "ipns" namespace validator while leaving other
// namespaces untouched.
func WrapIPNSValidator(inner record.Validator, sniff *Sniffer) record.Validator {
	if inner == nil {
		return inner
	}
	if nv, ok := inner.(record.NamespacedValidator); ok {
		out := make(record.NamespacedValidator, len(nv))
		for k, v := range nv {
			out[k] = v
		}
		if ipnsv, ok := out["ipns"]; ok && ipnsv != nil {
			out["ipns"] = &SniffingValidator{Inner: ipnsv, Sniff: sniff}
		}
		return out
	}
	return &SniffingValidator{Inner: inner, Sniff: sniff}
}

func (v *SniffingValidator) Validate(key string, value []byte) error {
	err := v.Inner.Validate(key, value)
	if err != nil {
		return err
	}

	if v.Sniff == nil {
		return nil
	}

	ns, _, nsErr := record.SplitKey(key)
	if nsErr != nil || ns != "ipns" {
		return nil
	}

	ctx := context.Background()
	// Stable source: IPNS name itself.
	if namePath, ok := IPNSRoutingKeyToNamePath(key); ok {
		_ = v.Sniff.PublishCID(ctx, namePath, "ipns-dht", "routing_key", "")
	}
	// Direct source: resolved /ipfs value (if present in the record).
	if ipfsPath, ok := ExtractIPFSPathFromIPNSRecord(value); ok {
		_ = v.Sniff.PublishCID(ctx, ipfsPath, "ipns-dht", "record_value", "")
	}

	return nil
}

func (v *SniffingValidator) Select(key string, values [][]byte) (int, error) {
	idx, err := v.Inner.Select(key, values)
	if err != nil {
		return idx, err
	}
	if v.Sniff == nil {
		return idx, nil
	}
	if !strings.HasPrefix(key, "/ipns/") {
		return idx, nil
	}
	if idx >= 0 && idx < len(values) {
		if ipfsPath, ok := ExtractIPFSPathFromIPNSRecord(values[idx]); ok {
			_ = v.Sniff.PublishCID(context.Background(), ipfsPath, "ipns-dht", "select", "")
		}
	}
	return idx, nil
}

var _ record.Validator = (*SniffingValidator)(nil)
