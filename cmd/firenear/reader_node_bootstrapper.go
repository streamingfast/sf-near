package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/dstore"
	firecore "github.com/streamingfast/firehose-core"
	"github.com/streamingfast/firehose-core/node-manager/operator"
	"go.uber.org/zap"
)

func newReaderNodeBootstrapper(
	ctx context.Context,
	logger *zap.Logger,
	cmd *cobra.Command,
	resolvedNodeArguments []string,
	resolver firecore.ReaderNodeArgumentResolver,
) (operator.Bootstrapper, error) {
	hostname, _ := os.Hostname()

	logger.Info("bootstrapping reader node",
		zap.String("hostname", hostname),
		zap.String("config_file", viper.GetString("reader-node-config-file")),
	)

	configFile := replaceNodeRole(viper.GetString("reader-node-config-file"), hostname)
	genesisFile := replaceNodeRole(viper.GetString("reader-node-genesis-file"), hostname)
	nodeKeyFile := replaceHostname(viper.GetString("reader-node-key-file"), hostname)
	nodeDataDir := replaceHostname(viper.GetString("reader-node-data-dir"), hostname)
	overwriteNodeFiles := viper.GetBool("reader-node-overwrite-node-files")

	logger.Info("final node configuration",
		zap.String("config_file", configFile),
		zap.String("genesis_file", genesisFile),
		zap.String("node_key_file", nodeKeyFile),
		zap.String("node_data_dir", nodeDataDir),
	)

	return &bootstrapper{
		configFile:  configFile,
		genesisFile: genesisFile,
		nodeKeyFile: nodeKeyFile,
		nodeDataDir: nodeDataDir,

		forceOverwrite: overwriteNodeFiles,
	}, nil
}

type bootstrapper struct {
	configFile  string
	genesisFile string
	nodeKeyFile string
	nodeDataDir string

	forceOverwrite bool
}

func (b *bootstrapper) Bootstrap() error {
	configFileInDataDir := filepath.Join(b.nodeDataDir, "config.json")
	genesisFileInDataDir := filepath.Join(b.nodeDataDir, "genesis.json")
	nodeKeyFileInDataDir := filepath.Join(b.nodeDataDir, "node_key.json")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	if err := os.MkdirAll(b.nodeDataDir, os.ModePerm); err != nil {
		return fmt.Errorf("create all dirs of %q: %w", b.nodeDataDir, err)
	}

	exists, err := fileExists(configFileInDataDir)
	if err != nil {
		return err
	}
	if !exists || b.forceOverwrite {
		if b.configFile != "" {
			if err := copyFile(ctx, b.configFile, configFileInDataDir); err != nil {
				return fmt.Errorf("unable to copy config file %q to %q: %w", b.configFile, configFileInDataDir, err)
			}
		}
	}

	exists, err = fileExists(nodeKeyFileInDataDir)
	if err != nil {
		return err
	}
	if !exists || b.forceOverwrite {
		if err := copyFile(ctx, b.nodeKeyFile, nodeKeyFileInDataDir); err != nil {
			return fmt.Errorf("unable to copy node key file %q to %q: %w", b.nodeKeyFile, nodeKeyFileInDataDir, err)
		}
	}

	exists, err = fileExists(genesisFileInDataDir)
	if err != nil {
		return err
	}
	if !exists {
		if err := copyFile(ctx, b.genesisFile, genesisFileInDataDir); err != nil {
			return fmt.Errorf("unable to copy genesis file %q to %q: %w", b.genesisFile, genesisFileInDataDir, err)
		}
	}

	return nil
}

func fileExists(file string) (bool, error) {
	stat, err := os.Stat(file)
	if os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return !stat.IsDir(), nil
}

func copyFile(ctx context.Context, in, out string) error {
	reader, _, _, err := dstore.OpenObject(ctx, in)
	if err != nil {
		return fmt.Errorf("unable : %w", err)
	}

	writer, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	if _, err := io.Copy(writer, reader); err != nil {
		// Do our best to delete the file if an error occurred while copying
		_ = os.Remove(out)

		return fmt.Errorf("copy content: %w", err)
	}

	return nil
}

func replaceHostname(in, hostname string) string {
	return strings.Replace(in, "{hostname}", hostname, -1)
}

func replaceNodeRole(in, hostname string) string {
	if strings.HasPrefix(hostname, "extractor-") {
		return strings.Replace(in, "{node-role}", "reader", -1) // todo(colin): this is not right. fix this in gcp
	}

	return in
}
