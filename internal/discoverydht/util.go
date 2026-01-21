package discoverydht

import (
	"strings"

	cid "github.com/ipfs/go-cid"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

func peerAddrsToStrings(addrs []ma.Multiaddr) []string {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a == nil {
			continue
		}
		out = append(out, a.String())
	}
	return out
}

func mhToCIDString(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	// Provider keys are multihashes.
	m, err := mh.Cast(key)
	if err != nil {
		return ""
	}

	dec, err := mh.Decode(m)
	if err != nil {
		return ""
	}

	// Prefer CIDv0 when possible (sha2-256 only).
	if dec.Code == mh.SHA2_256 {
		c0 := cid.NewCidV0(m)
		return c0.String()
	}

	// Otherwise wrap as CIDv1 raw for a stable CID-like representation.
	c1 := cid.NewCidV1(cid.Raw, m)
	// Keep it in base32 by default.
	return strings.ToLower(c1.String())
}
