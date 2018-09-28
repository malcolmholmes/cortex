package chunk

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	proto "github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk/cache"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/cortex/pkg/util/extract"
)

var (
	indexEntriesPerChunk = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "chunk_store_index_entries_per_chunk",
		Help:      "Number of entries written to storage per chunk.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 5),
	})
	rowWrites = util.NewHashBucketHistogram(util.HashBucketHistogramOpts{
		HistogramOpts: prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "chunk_store_row_writes_distribution",
			Help:      "Distribution of writes to individual storage rows",
			Buckets:   prometheus.DefBuckets,
		},
		HashBuckets: 1024,
	})
	cacheCorrupt = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "cache_corrupt_chunks_total",
		Help:      "Total count of corrupt chunks found in cache.",
	})
	indexWritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "chunk_store_index_writes_total",
		Help:      "Total count of index writes to store.",
	})
	indexWritesCached = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "chunk_store_index_writes_cached_total",
		Help:      "Total count of index writes reduced due to caching.",
	})
)

func init() {
	prometheus.MustRegister(rowWrites)
}

// StoreConfig specifies config for a ChunkStore
type StoreConfig struct {
	CacheConfig cache.Config

	MinChunkAge              time.Duration
	QueryChunkLimit          int
	CardinalityCacheSize     int
	CardinalityCacheValidity time.Duration
	CardinalityLimit         int

	EntryCache cache.Config
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *StoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.CacheConfig.RegisterFlags(f)
	cfg.EntryCache.RegisterFlagsWithPrefix("index-write-entry", f)

	f.DurationVar(&cfg.MinChunkAge, "store.min-chunk-age", 0, "Minimum time between chunk update and being saved to the store.")
	f.IntVar(&cfg.QueryChunkLimit, "store.query-chunk-limit", 2e6, "Maximum number of chunks that can be fetched in a single query.")
	f.IntVar(&cfg.CardinalityCacheSize, "store.cardinality-cache-size", 0, "Size of in-memory cardinality cache, 0 to disable.")
	f.DurationVar(&cfg.CardinalityCacheValidity, "store.cardinality-cache-validity", 1*time.Hour, "Period for which entries in the cardinality cache are valid.")
	f.IntVar(&cfg.CardinalityLimit, "store.cardinality-limit", 1e5, "Cardinality limit for index queries.")
}

// store implements Store
type store struct {
	cfg StoreConfig

	storage StorageClient
	schema  Schema
	*Fetcher

	entryCache *indexWriteCache
}

func newStore(cfg StoreConfig, schema Schema, storage StorageClient) (Store, error) {
	fetcher, err := NewChunkFetcher(cfg.CacheConfig, storage)
	if err != nil {
		return nil, errors.Wrap(err, "create chunk fetcher")
	}

	entryCache, err := cache.New(cfg.EntryCache)
	if err != nil {
		return nil, errors.Wrap(err, "make entry cache")
	}

	return &store{
		cfg:        cfg,
		storage:    storage,
		schema:     schema,
		Fetcher:    fetcher,
		entryCache: &indexWriteCache{entryCache},
	}, nil
}

// Stop any background goroutines (ie in the cache.)
func (c *store) Stop() {
	c.Fetcher.Stop()
}

// Put implements ChunkStore
func (c *store) Put(ctx context.Context, chunks []Chunk) error {
	for _, chunk := range chunks {
		if err := c.PutOne(ctx, chunk.From, chunk.Through, chunk); err != nil {
			return err
		}
	}
	return nil
}

// PutOne implements ChunkStore
func (c *store) PutOne(ctx context.Context, from, through model.Time, chunk Chunk) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	// Horribly, PutChunks mutates the chunk by setting its checksum.  By putting
	// the chunk in a slice we are in fact passing by reference, so below we
	// need to make sure we pick the chunk back out the slice.
	chunks := []Chunk{chunk}

	err = c.storage.PutChunks(ctx, chunks)
	if err != nil {
		return err
	}

	c.writeBackCache(ctx, chunks)

	writeReqs, dedupeEntries, err := c.calculateIndexEntries(userID, from, through, chunks[0])
	if err != nil {
		return err
	}

	if err := c.storage.BatchWrite(ctx, writeReqs); err != nil {
		return err
	}

	c.writeBackDedupeEntrires(dedupeEntries)
	return nil
}

// calculateIndexEntries creates a set of batched WriteRequests for all the chunks it is given.
func (c *store) calculateIndexEntries(userID string, from, through model.Time, chunk Chunk) (WriteBatch, []IndexEntry, error) {
	seenIndexEntries := map[string]struct{}{}

	metricName, err := extract.MetricNameFromMetric(chunk.Metric)
	if err != nil {
		return nil, nil, err
	}

	entries, err := c.schema.GetWriteEntries(from, through, userID, metricName, chunk.Metric, chunk.ExternalKey())
	if err != nil {
		return nil, nil, err
	}
	indexEntriesPerChunk.Observe(float64(len(entries)))

	_, entries = c.dedupeWriteEntries(entries)
	indexWritesTotal.Add(float64(len(entries)))

	// Remove duplicate entries based on tableName:hashValue:rangeValue
	result := c.storage.NewWriteBatch()
	for _, entry := range entries {
		key := fmt.Sprintf("%s:%s:%x", entry.TableName, entry.HashValue, entry.RangeValue)
		if _, ok := seenIndexEntries[key]; !ok {
			seenIndexEntries[key] = struct{}{}
			rowWrites.Observe(entry.HashValue, 1)
			result.Add(entry.TableName, entry.HashValue, entry.RangeValue, entry.Value)
		}
	}

	return result, entries, nil
}

func (c *store) dedupeWriteEntries(entries []IndexEntry) (found []IndexEntry, missing []IndexEntry) {
	if c.entryCache == nil {
		return entries, nil
	}

	found, missing = c.entryCache.Fetch(context.Background(), entries)
	return
}

func (c *store) writeBackDedupeEntrires(entries []IndexEntry) {
	if c.entryCache == nil {
		return
	}

	c.entryCache.Store(context.Background(), entries)
}

func dedupeKey(entry IndexEntry) string {
	key := strings.Join([]string{
		entry.TableName,
		entry.HashValue,
		string(entry.RangeValue),
		string(entry.Value),
	}, string('\xff'))

	hasher := fnv.New64a()
	hasher.Write([]byte(key)) // This'll never error.

	// Hex because memcache errors for the bytes produced by the hash.
	return hex.EncodeToString(hasher.Sum(nil))
}

// Get implements Store
func (c *store) Get(ctx context.Context, from, through model.Time, allMatchers ...*labels.Matcher) ([]Chunk, error) {
	log, ctx := newSpanLogger(ctx, "ChunkStore.Get")
	defer log.Span.Finish()
	level.Debug(log).Log("from", from, "through", through, "matchers", len(allMatchers))

	// Validate the query is within reasonable bounds.
	shortcut, err := c.validateQuery(ctx, from, &through)
	if err != nil {
		return nil, err
	} else if shortcut {
		return nil, nil
	}

	// Fetch metric name chunks if the matcher is of type equal,
	metricNameMatcher, matchers, ok := extract.MetricNameMatcherFromMatchers(allMatchers)
	if !ok && metricNameMatcher.Type != labels.MatchEqual {
		return nil, fmt.Errorf("query must contain metric name")
	}

	log.Span.SetTag("metric", metricNameMatcher.Value)
	return c.getMetricNameChunks(ctx, from, through, matchers, metricNameMatcher.Value)
}

func (c *store) validateQuery(ctx context.Context, from model.Time, through *model.Time) (shortcut bool, err error) {
	log, ctx := newSpanLogger(ctx, "store.validateQuery")
	defer log.Span.Finish()

	now := model.Now()

	if *through < from {
		err = fmt.Errorf("invalid query, through < from (%d < %d)", through, from)
		return
	}

	if from.After(now) {
		// time-span start is in future ... regard as legal
		level.Error(log).Log("msg", "whole timerange in future, yield empty resultset", "through", through, "from", from, "now", now)
		shortcut = true
		return
	}

	if from.After(now.Add(-c.cfg.MinChunkAge)) {
		// no data relevant to this query will have arrived at the store yet
		shortcut = true
		return
	}

	if through.After(now.Add(5 * time.Minute)) {
		// time-span end is in future ... regard as legal
		level.Error(log).Log("msg", "adjusting end timerange from future to now", "old_through", through, "new_through", now)
		*through = now // Avoid processing future part - otherwise some schemas could fail with eg non-existent table gripes
	}

	return
}

func (c *store) getMetricNameChunks(ctx context.Context, from, through model.Time, allMatchers []*labels.Matcher, metricName string) ([]Chunk, error) {
	log, ctx := newSpanLogger(ctx, "ChunkStore.getMetricNameChunks")
	defer log.Finish()
	level.Debug(log).Log("from", from, "through", through, "metricName", metricName, "matchers", len(allMatchers))

	filters, matchers := util.SplitFiltersAndMatchers(allMatchers)
	chunks, err := c.lookupChunksByMetricName(ctx, from, through, matchers, metricName)
	if err != nil {
		return nil, err
	}
	level.Debug(log).Log("Chunks in index", len(chunks))

	// Filter out chunks that are not in the selected time range.
	filtered, keys := filterChunksByTime(from, through, chunks)
	level.Debug(log).Log("Chunks post filtering", len(chunks))

	if len(filtered) > c.cfg.QueryChunkLimit {
		err := fmt.Errorf("Query %v fetched too many chunks (%d > %d)", allMatchers, len(filtered), c.cfg.QueryChunkLimit)
		level.Error(log).Log("err", err)
		return nil, err
	}

	// Now fetch the actual chunk data from Memcache / S3
	allChunks, err := c.FetchChunks(ctx, filtered, keys)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	// Filter out chunks based on the empty matchers in the query.
	filteredChunks := filterChunksByMatchers(allChunks, filters)
	return filteredChunks, nil
}

func (c *store) lookupChunksByMetricName(ctx context.Context, from, through model.Time, matchers []*labels.Matcher, metricName string) ([]Chunk, error) {
	log, ctx := newSpanLogger(ctx, "ChunkStore.lookupChunksByMetricName")
	defer log.Finish()

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	// Just get chunks for metric if there are no matchers
	if len(matchers) == 0 {
		queries, err := c.schema.GetReadQueriesForMetric(from, through, userID, model.LabelValue(metricName))
		if err != nil {
			return nil, err
		}
		level.Debug(log).Log("queries", len(queries))

		entries, err := c.lookupEntriesByQueries(ctx, queries)
		if err != nil {
			return nil, err
		}
		level.Debug(log).Log("entries", len(entries))

		chunkIDs, err := c.parseIndexEntries(ctx, entries, nil)
		if err != nil {
			return nil, err
		}
		level.Debug(log).Log("chunkIDs", len(chunkIDs))

		return c.convertChunkIDsToChunks(ctx, chunkIDs)
	}

	// Otherwise get chunks which include other matchers
	incomingChunkIDs := make(chan []string)
	incomingErrors := make(chan error)
	for _, matcher := range matchers {
		go func(matcher *labels.Matcher) {
			// Lookup IndexQuery's
			var queries []IndexQuery
			var err error
			if matcher.Type != labels.MatchEqual {
				queries, err = c.schema.GetReadQueriesForMetricLabel(from, through, userID, model.LabelValue(metricName), model.LabelName(matcher.Name))
			} else {
				queries, err = c.schema.GetReadQueriesForMetricLabelValue(from, through, userID, model.LabelValue(metricName), model.LabelName(matcher.Name), model.LabelValue(matcher.Value))
			}
			if err != nil {
				incomingErrors <- err
				return
			}
			level.Debug(log).Log("matcher", matcher, "queries", len(queries))

			// Lookup IndexEntry's
			entries, err := c.lookupEntriesByQueries(ctx, queries)
			if err != nil {
				incomingErrors <- err
				return
			}
			level.Debug(log).Log("matcher", matcher, "entries", len(entries))

			// Convert IndexEntry's to chunk IDs, filter out non-matchers at the same time.
			chunkIDs, err := c.parseIndexEntries(ctx, entries, matcher)
			if err != nil {
				incomingErrors <- err
				return
			}
			level.Debug(log).Log("matcher", matcher, "chunkIDs", len(chunkIDs))
			incomingChunkIDs <- chunkIDs
		}(matcher)
	}

	// Receive chunkSets from all matchers
	var chunkIDs []string
	var lastErr error
	for i := 0; i < len(matchers); i++ {
		select {
		case incoming := <-incomingChunkIDs:
			if chunkIDs == nil {
				chunkIDs = incoming
			} else {
				chunkIDs = intersectStrings(chunkIDs, incoming)
			}
		case err := <-incomingErrors:
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	level.Debug(log).Log("msg", "post intersection", "chunkIDs", len(chunkIDs))

	// Convert IndexEntry's into chunks
	return c.convertChunkIDsToChunks(ctx, chunkIDs)
}

func (c *store) lookupEntriesByQueries(ctx context.Context, queries []IndexQuery) ([]IndexEntry, error) {
	var entries []IndexEntry
	err := c.storage.QueryPages(ctx, queries, func(query IndexQuery, resp ReadBatch) bool {
		iter := resp.Iterator()
		for iter.Next() {
			entries = append(entries, IndexEntry{
				TableName:  query.TableName,
				HashValue:  query.HashValue,
				RangeValue: iter.RangeValue(),
				Value:      iter.Value(),
			})
		}
		return true
	})
	if err != nil {
		level.Error(util.WithContext(ctx, util.Logger)).Log("msg", "error querying storage", "err", err)
	}
	return entries, err
}

func (c *store) parseIndexEntries(ctx context.Context, entries []IndexEntry, matcher *labels.Matcher) ([]string, error) {
	result := make([]string, 0, len(entries))

	for _, entry := range entries {
		chunkKey, labelValue, _, _, err := parseChunkTimeRangeValue(entry.RangeValue, entry.Value)
		if err != nil {
			return nil, err
		}

		if matcher != nil && !matcher.Matches(string(labelValue)) {
			continue
		}
		result = append(result, chunkKey)
	}

	// Return ids sorted and deduped because they will be merged with other sets.
	sort.Strings(result)
	result = uniqueStrings(result)
	return result, nil
}

func (c *store) convertChunkIDsToChunks(ctx context.Context, chunkIDs []string) ([]Chunk, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	chunkSet := make([]Chunk, 0, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		chunk, err := ParseExternalKey(userID, chunkID)
		if err != nil {
			return nil, err
		}
		chunkSet = append(chunkSet, chunk)
	}

	return chunkSet, nil
}

type indexWriteCache struct {
	cache.Cache
}

func (c *indexWriteCache) Store(ctx context.Context, entries []IndexEntry) {
	keys := make([]string, 0, len(entries))
	bufs := make([][]byte, 0, len(entries))

	for _, entry := range entries {
		key := dedupeKey(entry)
		out, err := proto.Marshal(&entry)
		if err != nil {
			level.Warn(util.Logger).Log("msg", "error marshalling index entry", "err", err)
			return
		}

		keys = append(keys, key)
		bufs = append(bufs, out)
	}

	c.Cache.Store(ctx, keys, bufs)
	return
}

func (c *indexWriteCache) Fetch(ctx context.Context, entries []IndexEntry) (found []IndexEntry, missing []IndexEntry) {
	dedupedKeys := make(map[string]IndexEntry, len(entries))
	for _, entry := range entries {
		dedupedKeys[dedupeKey(entry)] = entry
	}

	keys := make([]string, 0, len(dedupedKeys))
	for key := range dedupedKeys {
		keys = append(keys, key)
	}

	foundKeys, bufs, _ := c.Cache.Fetch(ctx, keys)
	found = make([]IndexEntry, 0, len(foundKeys))

	for i, key := range foundKeys {
		entry := dedupedKeys[key]
		buf := bufs[i]
		out, err := proto.Marshal(&entry)
		if err != nil {
			level.Warn(util.Logger).Log("msg", "error marshalling index entry", "err", err)
			return nil, entries
		}

		if bytes.Equal(buf, out) {
			found = append(found, entry)
		}
	}

	// Calculate what is missing.
	missing = make([]IndexEntry, 0, len(entries)-len(found))
	i := 0
	for _, entry := range entries {
		if i >= len(found) {
			missing = append(missing, entry)
			continue
		}

		if !entryEquals(entry, found[i]) {
			missing = append(missing, entry)
			continue
		}

		i++
	}

	return
}

func (c *indexWriteCache) Stop() error {
	return c.Cache.Stop()
}

func entryEquals(a, b IndexEntry) bool {
	return a.TableName == b.TableName &&
		a.HashValue == b.HashValue &&
		bytes.Equal(a.RangeValue, b.RangeValue) &&
		bytes.Equal(a.Value, b.Value)
}
