package cidutil

import (
	"regexp"

	cid "github.com/ipfs/go-cid"
)

// Roughly matches CID-like strings; we validate candidates with go-cid.
// Base32/base36 CID strings are lowercase.
var cidLikeRe = regexp.MustCompile(`\b([a-z0-9]{10,})\b`)

func ExtractCIDStrings(s string) []string {
	if s == "" {
		return nil
	}

	matches := cidLikeRe.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return nil
	}

	out := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		cand := m[1]
		if _, ok := seen[cand]; ok {
			continue
		}

		if _, err := cid.Decode(cand); err != nil {
			continue
		}

		seen[cand] = struct{}{}
		out = append(out, cand)
	}
	return out
}
