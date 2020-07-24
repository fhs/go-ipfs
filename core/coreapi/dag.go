package coreapi

import (
	"context"
	"fmt"

	cid "github.com/ipfs/go-cid"
	pin "github.com/ipfs/go-ipfs-pinner"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
)

type dagAPI struct {
	ipld.DAGService

	core *CoreAPI
}

type pinningAdder CoreAPI

func (adder *pinningAdder) Add(ctx context.Context, nd ipld.Node) error {
	defer adder.blockstore.PinLock().Unlock()

	if err := adder.dag.Add(ctx, nd); err != nil {
		return fmt.Errorf("pinningAdder.dag.Add: %v", err)
	}

	adder.pinning.PinWithMode(nd.Cid(), pin.Recursive)

	err := adder.pinning.Flush(ctx)
	if err != nil {
		err = fmt.Errorf("pinningAdder.pinning.Flush: %v", err)
	}
	return err
}

func (adder *pinningAdder) AddMany(ctx context.Context, nds []ipld.Node) error {
	defer adder.blockstore.PinLock().Unlock()

	if err := adder.dag.AddMany(ctx, nds); err != nil {
		return err
	}

	cids := cid.NewSet()

	for _, nd := range nds {
		c := nd.Cid()
		if cids.Visit(c) {
			adder.pinning.PinWithMode(c, pin.Recursive)
		}
	}

	return adder.pinning.Flush(ctx)
}

func (api *dagAPI) Pinning() ipld.NodeAdder {
	return (*pinningAdder)(api.core)
}

func (api *dagAPI) Session(ctx context.Context) ipld.NodeGetter {
	return dag.NewSession(ctx, api.DAGService)
}

var (
	_ ipld.DAGService  = (*dagAPI)(nil)
	_ dag.SessionMaker = (*dagAPI)(nil)
)
