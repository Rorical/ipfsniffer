package filter

import (
	"path/filepath"
	"strings"
)

type Policy struct {
	SkipExt        []string
	SkipMimePrefix []string
	MaxFileBytes   int64
}

type Decision struct {
	Allowed    bool
	SkipReason string
}

func Decide(path string, mime string, sizeBytes int64, p Policy) Decision {
	if p.MaxFileBytes > 0 && sizeBytes > p.MaxFileBytes {
		return Decision{Allowed: false, SkipReason: "too_large"}
	}

	ext := strings.ToLower(filepath.Ext(path))
	for _, s := range p.SkipExt {
		if strings.ToLower(s) == ext && ext != "" {
			return Decision{Allowed: false, SkipReason: "ext_denied"}
		}
	}

	mime = strings.ToLower(strings.TrimSpace(mime))
	for _, pref := range p.SkipMimePrefix {
		pref = strings.ToLower(strings.TrimSpace(pref))
		if pref != "" && strings.HasPrefix(mime, pref) {
			return Decision{Allowed: false, SkipReason: "mime_denied"}
		}
	}

	return Decision{Allowed: true}
}
