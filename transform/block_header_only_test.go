package transform

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/bstream/transform"
	pbtransform "github.com/streamingfast/firehose-near/pb/sf/near/transform/v1"
	pbnear "github.com/streamingfast/firehose-near/pb/sf/near/type/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func headerOnlyTransform(t *testing.T) *anypb.Any {
	transform := &pbtransform.HeaderOnly{}
	a, err := anypb.New(transform)
	require.NoError(t, err)
	return a
}

func TestHeaderOnly_Transform(t *testing.T) {
	headerOnly, err := NewHeaderOnlyTransformFactory(nil, nil)
	require.NoError(t, err)

	transformReg := transform.NewRegistry()
	transformReg.Register(headerOnly)

	transforms := []*anypb.Any{headerOnlyTransform(t)}

	preprocFunc, x, _, err := transformReg.BuildFromTransforms(transforms)
	require.NoError(t, err)
	require.Nil(t, x)

	block := &pbnear.Block{
		Header: &pbnear.BlockHeader{
			Height:     160,
			PrevHeight: 158,
			Hash:       &pbnear.CryptoHash{Bytes: []byte{0x00, 0xa0}},
			PrevHash:   &pbnear.CryptoHash{Bytes: []byte{0x00, 0x9e}},
		},
		Shards: []*pbnear.IndexerShard{
			{
				ShardId: 1,
			},
		},
		Author: "someone",
		ChunkHeaders: []*pbnear.ChunkHeader{
			{ChunkHash: []byte{0x01}},
		},
		StateChanges: []*pbnear.StateChangeWithCause{
			{
				Value: &pbnear.StateChangeValue{
					Value: &pbnear.StateChangeValue_AccessKeyUpdate_{},
				},
			},
		},
	}
	payload, err := proto.Marshal(block)
	require.NoError(t, err)

	blk := &pbbstream.Block{
		Number:    block.Num(),
		Id:        block.ID(),
		LibNum:    block.LIBNum(),
		ParentNum: block.GetFirehoseBlockParentNumber(),
		ParentId:  block.PreviousID(),
		Timestamp: timestamppb.New(block.GetFirehoseBlockTime()),
		Payload:   &anypb.Any{TypeUrl: "sf.near.type.v1.Block", Value: payload},
	}

	output, err := preprocFunc(blk)
	require.NoError(t, err)

	assertProtoEqual(t, &pbnear.Block{
		Header: &pbnear.BlockHeader{
			Height:     160,
			PrevHeight: 158,
			Hash:       &pbnear.CryptoHash{Bytes: []byte{0x00, 0xa0}},
			PrevHash:   &pbnear.CryptoHash{Bytes: []byte{0x00, 0x9e}},
		},
	}, output.(*pbnear.Block))
}
