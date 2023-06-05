package main

import (
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/spf13/cobra"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	firecore "github.com/streamingfast/firehose-core"
	pbtransform "github.com/streamingfast/firehose-near/pb/sf/near/transform/v1"
	pbnear "github.com/streamingfast/firehose-near/pb/sf/near/type/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

func encodeBlock(blk firecore.Block) (*pbbstream.Block, error) {
	block := blk.(*pbnear.Block)

	enc := &pbbstream.Block{
		Number:    block.Num(),
		Id:        block.ID(),
		LibNum:    block.LIBNum(),
		ParentNum: block.GetFirehoseBlockParentNumber(),
		ParentId:  block.PreviousID(),
		Timestamp: timestamppb.New(block.GetFirehoseBlockTime()),
	}

	return enc, nil
}

func printBlock(blk firecore.Block, alsoPrintTransactions bool, out io.Writer) error {
	block := blk.(*pbnear.Block)

	transactionCount := 0
	for _, shard := range block.Shards {
		if shard.Chunk != nil {
			transactionCount += len(shard.Chunk.Transactions)
		}
	}

	if _, err := fmt.Fprintf(out, "Block #%d (%s) (prev: %s): %d transactions\n",
		block.Num(),
		block.ID(),
		block.PreviousID()[0:7],
		transactionCount,
	); err != nil {
		return err
	}

	if alsoPrintTransactions {
		for _, shard := range block.Shards {
			if shard.Chunk != nil {
				if _, err := fmt.Fprintf(out, "- Shard %d\n", shard.ShardId); err != nil {
					return err
				}

				for _, trx := range shard.Chunk.Transactions {
					if _, err := fmt.Fprintf(out, "  - Transaction %s\n", trx.Transaction.Hash.AsBase58String()); err != nil {
						return err
					}
				}

				for _, receipt := range shard.Chunk.Receipts {
					if _, err := fmt.Fprintf(out, "  - Receipt %s\n", receipt.ReceiptId.AsBase58String()); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func receiptAccountFiltersParser(cmd *cobra.Command, logger *zap.Logger) ([]*anypb.Any, error) {
	filterStrs, err := cmd.Flags().GetStringSlice("receipt-account-filters")
	if err != nil {
		return nil, fmt.Errorf("unable to get receipt-account-filters flag: %w", err)
	}

	var filters []*anypb.Any
	for _, filterStr := range filterStrs {
		filter, err := parseReceiptAccountFilters(filterStr)
		if err != nil {
			return nil, fmt.Errorf("unable to parse receipt account filters: %w", err)
		}
		if filter != nil {
			filters = append(filters, filter)
		}
	}

	return filters, nil
}

func parseReceiptAccountFilters(in string) (*anypb.Any, error) {
	if in == "" {
		return nil, nil
	}

	var pairs []*pbtransform.PrefixSuffixPair
	var accounts []string

	for _, unit := range strings.Split(in, ",") {
		if parts := strings.Split(unit, ":"); len(parts) == 2 {
			pairs = append(pairs, &pbtransform.PrefixSuffixPair{
				Prefix: parts[0],
				Suffix: parts[1],
			})
			continue
		}
		accounts = append(accounts, unit)
	}

	filters := &pbtransform.BasicReceiptFilter{
		Accounts:             accounts,
		PrefixAndSuffixPairs: pairs,
	}

	return anypb.New(filters)
}
