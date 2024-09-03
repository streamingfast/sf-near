package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	firecore "github.com/streamingfast/firehose-core"
	fhCmd "github.com/streamingfast/firehose-core/cmd"
	"github.com/streamingfast/firehose-core/firehose/info"
	"github.com/streamingfast/firehose-core/node-manager/mindreader"
	"github.com/streamingfast/firehose-near/codec"
	pbnear "github.com/streamingfast/firehose-near/pb/sf/near/type/v1"
	"github.com/streamingfast/firehose-near/transform"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func main() {
	chain := &firecore.Chain[*pbnear.Block]{
		ShortName:            "near",
		LongName:             "NEAR",
		ExecutableName:       "near-firehose-indexer",
		FullyQualifiedModule: "github.com/streamingfast/firehose-near",
		Version:              version,
		DefaultBlockType:     "sf.near.type.v1.Block",

		BlockFactory: func() firecore.Block { return new(pbnear.Block) },

		BlockIndexerFactories: map[string]firecore.BlockIndexerFactory[*pbnear.Block]{
			transform.ReceiptAddressIndexShortName: transform.NewNearBlockIndexer,
		},

		BlockTransformerFactories: map[protoreflect.FullName]firecore.BlockTransformerFactory{
			transform.HeaderOnlyMessageName:    transform.NewHeaderOnlyTransformFactory,
			transform.ReceiptFilterMessageName: transform.BasicReceiptFilterFactory,
		},

		ConsoleReaderFactory: func(lines chan string, blockEncoder firecore.BlockEncoder, logger *zap.Logger, tracer logging.Tracer) (mindreader.ConsolerReader, error) {
			// FIXME: This was hardcoded also in the previous firehose-near version, Firehose will break if this is not available
			return codec.NewConsoleReader(lines, firecore.NewBlockEncoder(), "http://localhost:3030")
		},

		RegisterExtraStartFlags: func(flags *pflag.FlagSet) {
			flags.String("reader-node-config-file", "", "Node configuration file, the file is copied inside the {data-dir}/reader/data folder Use {hostname} label to use short hostname in path")
			flags.String("reader-node-genesis-file", "./genesis.json", "Node genesis file, the file is copied inside the {data-dir}/reader/data folder. Use {hostname} label to use short hostname in path")
			flags.String("reader-node-key-file", "./node_key.json", "Node key configuration file, the file is copied inside the {data-dir}/reader/data folder. Use {hostname} label to use with short hostname in path")
			flags.Bool("reader-node-overwrite-node-files", false, "Force download of node-key and config files even if they already exist on the machine.")
		},

		ReaderNodeBootstrapperFactory: newReaderNodeBootstrapper,

		Tools: &firecore.ToolsConfig[*pbnear.Block]{
			MergedBlockUpgrader: blockUpgrader,

			RegisterExtraCmd: func(chain *firecore.Chain[*pbnear.Block], toolsCmd *cobra.Command, zlog *zap.Logger, tracer logging.Tracer) error {
				toolsCmd.AddCommand(newToolsGenerateNodeKeyCmd(chain))
				return nil
			},

			TransformFlags: &firecore.TransformFlags{
				Register: func(flags *pflag.FlagSet) {
					flags.String("receipt-account-filters", "", "Comma-separated accounts to use as filter/index. If it contains a colon (:), it will be interpreted as <prefix>:<suffix> (each of which can be empty, ex: 'hello:' or ':world')")
				},
				Parse: receiptAccountFiltersParser,
			},
		},

		InfoResponseFiller: info.DefaultInfoResponseFiller,
	}

	fhCmd.Main(chain)
}

// Version value, injected via go build `ldflags` at build time, **must** not be removed or inlined
var version = "dev"
