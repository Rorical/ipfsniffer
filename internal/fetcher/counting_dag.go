package fetcher

import (
	"context"
	"fmt"
	"sync"

	cid "github.com/ipfs/go-cid"
	ipldformat "github.com/ipfs/go-ipld-format"
)

// countingDAGService wraps a DAGService and counts unique CIDs fetched via Get.
// This approximates "raw IPLD blocks" fetched during traversal.
//
// Note: blocks loaded during the initial path resolution performed by Kubo may
// bypass this wrapper; the goal here is to avoid double-fetch by enforcing
// max_dag_nodes during the actual traversal.
type countingDAGService struct {
	inner ipldformat.DAGService
	limit int64

	mu    sync.Mutex
	seen  map[string]struct{}
	count int64
}

func newCountingDAG(inner ipldformat.DAGService, limit int64) *countingDAGService {
	return &countingDAGService{
		inner: inner,
		limit: limit,
		seen:  make(map[string]struct{}),
	}
}

func (d *countingDAGService) Get(ctx context.Context, c cid.Cid) (ipldformat.Node, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	k := c.String()
	d.mu.Lock()
	if _, ok := d.seen[k]; !ok {
		d.seen[k] = struct{}{}
		d.count++
		if d.limit > 0 && d.count > d.limit {
			d.mu.Unlock()
			return nil, fmt.Errorf("max_dag_nodes exceeded")
		}
	}
	d.mu.Unlock()

	return d.inner.Get(ctx, c)
}

func (d *countingDAGService) Add(ctx context.Context, n ipldformat.Node) error {
	return d.inner.Add(ctx, n)
}

func (d *countingDAGService) AddMany(ctx context.Context, nds []ipldformat.Node) error {
	return d.inner.AddMany(ctx, nds)
}

func (d *countingDAGService) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipldformat.NodeOption {
	return d.inner.GetMany(ctx, cids)
}

func (d *countingDAGService) Remove(ctx context.Context, c cid.Cid) error {
	return d.inner.Remove(ctx, c)
}

func (d *countingDAGService) RemoveMany(ctx context.Context, cids []cid.Cid) error {
	return d.inner.RemoveMany(ctx, cids)
}

var _ ipldformat.DAGService = (*countingDAGService)(nil)
