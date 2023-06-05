package main

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	pbnear "github.com/streamingfast/firehose-near/pb/sf/near/type/v1"
)

func blockUpgrader(block *pbbstream.Block) (*pbbstream.Block, error) {
	nb := &pbnear.Block{}
	err := block.Payload.UnmarshalTo(nb)
	if err != nil {
		return nil, fmt.Errorf("unmarshal block: %w", err)
	}

	block.ParentNum = nb.GetFirehoseBlockParentNumber()
	block.ParentId = nb.GetFirehoseBlockParentID()
	block.Timestamp = timestamppb.New(nb.GetFirehoseBlockTime())
	block.LibNum = nb.GetFirehoseBlockLIBNum()

	return block, nil
}
