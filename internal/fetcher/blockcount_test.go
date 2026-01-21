package fetcher

import (
	"context"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipldformat "github.com/ipfs/go-ipld-format"
)

type memNode struct {
	c     cid.Cid
	links []*ipldformat.Link
	data  []byte
}

func (n *memNode) RawData() []byte                                      { return n.data }
func (n *memNode) Cid() cid.Cid                                         { return n.c }
func (n *memNode) String() string                                       { return n.c.String() }
func (n *memNode) Loggable() map[string]any                             { return map[string]any{} }
func (n *memNode) Resolve(path []string) (interface{}, []string, error) { return nil, nil, nil }
func (n *memNode) Tree(path string, depth int) []string                 { return nil }
func (n *memNode) ResolveLink(path []string) (*ipldformat.Link, []string, error) {
	return nil, nil, nil
}
func (n *memNode) Copy() ipldformat.Node               { return n }
func (n *memNode) Links() []*ipldformat.Link           { return n.links }
func (n *memNode) Stat() (*ipldformat.NodeStat, error) { return &ipldformat.NodeStat{}, nil }
func (n *memNode) Size() (uint64, error)               { return uint64(len(n.data)), nil }

type memDAG struct {
	nodes map[string]ipldformat.Node
}

func (d *memDAG) Add(context.Context, ipldformat.Node) error       { return nil }
func (d *memDAG) AddMany(context.Context, []ipldformat.Node) error { return nil }
func (d *memDAG) Get(_ context.Context, c cid.Cid) (ipldformat.Node, error) {
	n, ok := d.nodes[c.String()]
	if !ok {
		return nil, ipldformat.ErrNotFound{Cid: c}

	}
	return n, nil
}
func (d *memDAG) GetMany(context.Context, []cid.Cid) <-chan *ipldformat.NodeOption { return nil }
func (d *memDAG) Remove(context.Context, cid.Cid) error                            { return nil }
func (d *memDAG) RemoveMany(context.Context, []cid.Cid) error                      { return nil }

var _ ipldformat.DAGService = (*memDAG)(nil)

func TestBlockCounterLimit(t *testing.T) {
	// build three distinct cids
	b1 := blocks.NewBlock([]byte("a"))
	b2 := blocks.NewBlock([]byte("b"))
	b3 := blocks.NewBlock([]byte("c"))

	n1 := &memNode{c: b1.Cid(), links: []*ipldformat.Link{{Cid: b2.Cid()}, {Cid: b3.Cid()}}, data: b1.RawData()}
	n2 := &memNode{c: b2.Cid(), links: nil, data: b2.RawData()}
	n3 := &memNode{c: b3.Cid(), links: nil, data: b3.RawData()}

	dag := &memDAG{nodes: map[string]ipldformat.Node{b1.Cid().String(): n1, b2.Cid().String(): n2, b3.Cid().String(): n3}}

	bc := &BlockCounter{DAG: dag, Limit: 2}
	_, err := bc.CountFromRoot(context.Background(), b1.Cid())
	if err == nil {
		t.Fatalf("expected limit error")
	}
}
