package types

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	pbnear "github.com/streamingfast/sf-near/types/pb/sf/near/type/v1"
)

func BlockFromProto(b *pbnear.Block) (*bstream.Block, error) {
	content, err := proto.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal to binary form: %s", err)
	}

	block := &bstream.Block{
		Id:             b.ID(),
		Number:         b.Number(),
		PreviousId:     b.PreviousID(),
		Timestamp:      b.Time(),
		LibNum:         b.Number() - 1,
		PayloadKind:    pbbstream.Protocol_NEAR,
		PayloadVersion: 1,
	}
	return bstream.GetBlockPayloadSetter(block, content)
}