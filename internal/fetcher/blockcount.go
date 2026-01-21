package fetcher

import (
	"context"
	"fmt"

	cid "github.com/ipfs/go-cid"
	ipldformat "github.com/ipfs/go-ipld-format"
)

type BlockCounter struct {
	DAG ipldformat.DAGService

	Limit int64

	seen  map[string]struct{}
	count int64
}

func (bc *BlockCounter) CountFromRoot(ctx context.Context, root cid.Cid) (int64, error) {
	if bc.DAG == nil {
		return 0, fmt.Errorf("dag service required")
	}
	if bc.seen == nil {
		bc.seen = make(map[string]struct{})
	}

	return bc.walk(ctx, root)
}

func (bc *BlockCounter) walk(ctx context.Context, c cid.Cid) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	k := c.String()
	if _, ok := bc.seen[k]; ok {
		return bc.count, nil
	}
	bc.seen[k] = struct{}{}
	bc.count++
	if bc.Limit > 0 && bc.count > bc.Limit {
		return bc.count, fmt.Errorf("max_dag_nodes exceeded")
	}

	n, err := bc.DAG.Get(ctx, c)
	if err != nil {
		return bc.count, err
	}

	for _, l := range n.Links() {
		if _, err := bc.walk(ctx, l.Cid); err != nil {
			return bc.count, err
		}
	}

	return bc.count, nil
}
