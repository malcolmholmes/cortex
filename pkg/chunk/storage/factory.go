package storage

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/aws"
	"github.com/weaveworks/cortex/pkg/chunk/cache"
	"github.com/weaveworks/cortex/pkg/chunk/cassandra"
	"github.com/weaveworks/cortex/pkg/chunk/gcp"
	"github.com/weaveworks/cortex/pkg/util"
)

// Config chooses which storage client to use.
type Config struct {
	StorageClient          string
	AWSStorageConfig       aws.StorageConfig
	GCPStorageConfig       gcp.Config
	CassandraStorageConfig cassandra.Config

	indexCache cache.Config
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.StorageClient, "chunk.storage-client", "aws", "Which storage client to use (aws, gcp, cassandra, inmemory).")
	cfg.AWSStorageConfig.RegisterFlags(f)
	cfg.GCPStorageConfig.RegisterFlags(f)
	cfg.CassandraStorageConfig.RegisterFlags(f)

	cfg.indexCache.RegisterFlagsWithPrefix("store.index-cache", f)
}

// Opts makes the storage clients based on the configuration.
func Opts(cfg Config, schemaCfg chunk.SchemaConfig) ([]chunk.StorageOpt, error) {
	tieredCache, err := cache.New(cfg.indexCache)
	if err != nil {
		return nil, errors.Wrap(err, "create index cache")
	}

	opts, err := newStorageOpts(cfg, schemaCfg)
	if err != nil {
		return nil, errors.Wrap(err, "error creating storage client")
	}

	for i := range opts {
		opts[i].Client = newCachingStorageClient(opts[i].Client, tieredCache, cfg.indexCache.DefaultValidity)
	}

	return opts, nil
}

func newStorageOpts(cfg Config, schemaCfg chunk.SchemaConfig) ([]chunk.StorageOpt, error) {
	switch cfg.StorageClient {
	case "inmemory":
		return chunk.Opts()
	case "aws":
		if cfg.AWSStorageConfig.DynamoDB.URL == nil {
			return nil, fmt.Errorf("Must set -dynamodb.url in aws mode")
		}
		path := strings.TrimPrefix(cfg.AWSStorageConfig.DynamoDB.URL.Path, "/")
		if len(path) > 0 {
			level.Warn(util.Logger).Log("msg", "ignoring DynamoDB URL path", "path", path)
		}
		return aws.Opts(cfg.AWSStorageConfig, schemaCfg)
	case "gcp":
		return gcp.Opts(context.Background(), cfg.GCPStorageConfig, schemaCfg)
	case "cassandra":
		return cassandra.Opts(cfg.CassandraStorageConfig, schemaCfg)
	default:
		return nil, fmt.Errorf("Unrecognized storage client %v, choose one of: aws, gcp, cassandra, inmemory", cfg.StorageClient)
	}
}

// NewTableClient makes a new table client based on the configuration.
func NewTableClient(cfg Config) (chunk.TableClient, error) {
	switch cfg.StorageClient {
	case "inmemory":
		return chunk.NewMockStorage(), nil
	case "aws":
		path := strings.TrimPrefix(cfg.AWSStorageConfig.DynamoDB.URL.Path, "/")
		if len(path) > 0 {
			level.Warn(util.Logger).Log("msg", "ignoring DynamoDB URL path", "path", path)
		}
		return aws.NewDynamoDBTableClient(cfg.AWSStorageConfig.DynamoDBConfig)
	case "gcp":
		return gcp.NewTableClient(context.Background(), cfg.GCPStorageConfig)
	case "cassandra":
		return cassandra.NewTableClient(context.Background(), cfg.CassandraStorageConfig)
	default:
		return nil, fmt.Errorf("Unrecognized storage client %v, choose one of: aws, gcp, inmemory", cfg.StorageClient)
	}
}
