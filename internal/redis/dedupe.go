package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Dedupe struct {
	Prefix string
	TTL    time.Duration
}

func (d Dedupe) key(cid string) string {
	return fmt.Sprintf("%s:%s", d.Prefix, cid)
}

// Seen returns true if we've already seen the CID. If not seen, it marks it as seen.
func (d Dedupe) Seen(ctx context.Context, rdb *goredis.Client, cid string) (bool, error) {
	if cid == "" {
		return false, fmt.Errorf("cid required")
	}
	if d.Prefix == "" {
		d.Prefix = "ipfsniffer:seen"
	}
	if d.TTL == 0 {
		d.TTL = 24 * time.Hour
	}

	ok, err := rdb.SetNX(ctx, d.key(cid), "1", d.TTL).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx: %w", err)
	}
	// ok=true means key was set (not seen before)
	return !ok, nil
}
