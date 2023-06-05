package transform

import (
	"fmt"
	"strings"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	firecore "github.com/streamingfast/firehose-core"
	pbtransform "github.com/streamingfast/firehose-near/pb/sf/near/transform/v1"
	pbnear "github.com/streamingfast/firehose-near/pb/sf/near/type/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var ReceiptFilterMessageName = proto.MessageName(&pbtransform.BasicReceiptFilter{})

func BasicReceiptFilterFactory(indexStore dstore.Store, possibleIndexSizes []uint64) (*transform.Factory, error) {
	return &transform.Factory{
		Obj: &pbtransform.BasicReceiptFilter{},
		NewFunc: func(message *anypb.Any) (transform.Transform, error) {
			mname := message.MessageName()
			if mname != ReceiptFilterMessageName {
				return nil, fmt.Errorf("expected type url %q, received %q", ReceiptFilterMessageName, message.TypeUrl)
			}

			filter := &pbtransform.BasicReceiptFilter{}
			err := proto.Unmarshal(message.Value, filter)
			if err != nil {
				return nil, fmt.Errorf("unexpected unmarshall error: %w", err)
			}

			if len(filter.Accounts) == 0 && len(filter.PrefixAndSuffixPairs) == 0 {
				return nil, fmt.Errorf("a basic account filter requires at least one account or one prefix/suffix pair")
			}

			accountMap := make(map[string]bool)
			for _, acc := range filter.Accounts {
				accountMap[acc] = true
			}
			for _, pair := range filter.PrefixAndSuffixPairs {
				if pair.Prefix == "" && pair.Suffix == "" {
					return nil, fmt.Errorf("invalid prefix_and_suffix_pairs: either prefix or suffix must be non-empty")
				}
			}
			f := &BasicReceiptFilter{
				Accounts:           accountMap,
				PrefixSuffixPairs:  filter.PrefixAndSuffixPairs,
				possibleIndexSizes: possibleIndexSizes,
				indexStore:         indexStore,
			}
			return f, nil
		},
	}, nil
}

type BasicReceiptFilter struct {
	Accounts          map[string]bool
	PrefixSuffixPairs []*pbtransform.PrefixSuffixPair

	indexStore         dstore.Store
	possibleIndexSizes []uint64
}

func (p *BasicReceiptFilter) String() string {
	return fmt.Sprintf("accounts: %v, prefix/suffix: %v", p.Accounts, p.PrefixSuffixPairs)
}

func matchesPrefixSuffix(receiverID string, prefixSuffixPairs []*pbtransform.PrefixSuffixPair) bool {
	for _, pair := range prefixSuffixPairs {
		if pair.Prefix == "" && strings.HasSuffix(receiverID, pair.Suffix) {
			return true
		}
		if pair.Suffix == "" && strings.HasPrefix(receiverID, pair.Prefix) {
			return true
		}
		if strings.HasPrefix(receiverID, pair.Prefix) && strings.HasSuffix(receiverID, pair.Suffix) {
			return true
		}
	}
	return false
}

func (p *BasicReceiptFilter) Transform(readOnlyBlk firecore.Block, in transform.Input) (transform.Output, error) {
	nearBlock := readOnlyBlk.(*pbnear.Block)
	var outShards []*pbnear.IndexerShard
	for _, shard := range nearBlock.Shards {
		var outcomes []*pbnear.IndexerExecutionOutcomeWithReceipt
		for _, outcome := range shard.ReceiptExecutionOutcomes {
			if outcome.Receipt.GetAction() != nil {
				if p.Accounts[outcome.Receipt.ReceiverId] || matchesPrefixSuffix(outcome.Receipt.ReceiverId, p.PrefixSuffixPairs) {
					outcomes = append(outcomes, outcome)
				}
			}
		}
		if len(outcomes) != 0 {
			shard.ReceiptExecutionOutcomes = outcomes
			outShards = append(outShards, shard)
		}
	}
	nearBlock.Shards = outShards
	return nearBlock, nil
}

func (p *BasicReceiptFilter) GetIndexProvider() bstream.BlockIndexProvider {
	if p.indexStore == nil {
		return nil
	}

	if len(p.Accounts) == 0 && len(p.PrefixSuffixPairs) == 0 {
		return nil
	}

	return NewNearBlockIndexProvider(
		p.indexStore,
		p.possibleIndexSizes,
		p.Accounts,
		p.PrefixSuffixPairs,
	)
}
