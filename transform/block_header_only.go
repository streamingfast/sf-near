package transform

import (
	"fmt"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	pbtransform "github.com/streamingfast/firehose-near/pb/sf/near/transform/v1"
	pbnear "github.com/streamingfast/firehose-near/pb/sf/near/type/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var HeaderOnlyMessageName = proto.MessageName(&pbtransform.HeaderOnly{})

func NewHeaderOnlyTransformFactory(_ dstore.Store, _ []uint64) (*transform.Factory, error) {
	return &transform.Factory{
		Obj: &pbtransform.HeaderOnly{},
		NewFunc: func(message *anypb.Any) (transform.Transform, error) {
			mname := message.MessageName()
			if mname != HeaderOnlyMessageName {
				return nil, fmt.Errorf("expected type url %q, received %q ", HeaderOnlyMessageName, message.TypeUrl)
			}

			filter := &pbtransform.HeaderOnly{}
			err := proto.Unmarshal(message.Value, filter)
			if err != nil {
				return nil, fmt.Errorf("unexpected unmarshall error: %w", err)
			}
			return &HeaderOnlyFilter{}, nil
		},
	}, nil
}

type HeaderOnlyFilter struct{}

func (p *HeaderOnlyFilter) String() string {
	return "header only filter"
}

func (p *HeaderOnlyFilter) Transform(readOnlyBlk *pbbstream.Block, in transform.Input) (transform.Output, error) {
	fullBlock := &pbnear.Block{}
	err := readOnlyBlk.Payload.UnmarshalTo(fullBlock)
	if err != nil {
		return nil, fmt.Errorf("mashalling block: %w", err)
	}

	zlog.Debug("running header only transformer",
		zap.String("hash", readOnlyBlk.GetFirehoseBlockID()),
		zap.Uint64("num", readOnlyBlk.GetFirehoseBlockNumber()),
	)

	// FIXME: The block is actually duplicated elsewhere which means that at this point,
	//        we work on our own copy of the block. So we can re-write this code to avoid
	//        all the extra allocation and simply nillify the values that we want to hide
	//        instead
	return &pbnear.Block{
		Header: fullBlock.Header,
	}, nil
}
