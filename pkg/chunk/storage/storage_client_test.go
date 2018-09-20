package storage

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/testutils"
)

func TestChunksBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.StorageClient, schema chunk.SchemaConfig) {
		const batchSize = 50
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Write a few batches of chunks.
		written := []string{}
		for i := 0; i < 50; i++ {
			keys, chunks, err := testutils.CreateChunks(i, batchSize)
			require.NoError(t, err)
			written = append(written, keys...)
			err = client.PutChunks(ctx, chunks)
			require.NoError(t, err)
		}

		// Get a few batches of chunks.
		for i := 0; i < 50; i++ {
			keysToGet := map[string]struct{}{}
			chunksToGet := []chunk.Chunk{}
			for len(chunksToGet) < batchSize {
				key := written[rand.Intn(len(written))]
				if _, ok := keysToGet[key]; ok {
					continue
				}
				keysToGet[key] = struct{}{}
				chunk, err := chunk.ParseExternalKey(userID, key)
				require.NoError(t, err)
				chunksToGet = append(chunksToGet, chunk)
			}

			chunksWeGot, err := client.GetChunks(ctx, chunksToGet)
			require.NoError(t, err)
			require.Equal(t, len(chunksToGet), len(chunksWeGot))

			sort.Sort(ByKey(chunksToGet))
			sort.Sort(ByKey(chunksWeGot))
			for j := 0; j < len(chunksWeGot); j++ {
				require.Equal(t, chunksToGet[i].ExternalKey(), chunksWeGot[i].ExternalKey(), strconv.Itoa(i))
			}
		}
	})
}

func TestStreamChunks(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client chunk.StorageClient, schema chunk.SchemaConfig) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, chunks, err := testutils.CreateChunks(0, 2000)
		require.NoError(t, err)

		err = client.PutChunks(ctx, chunks)
		require.NoError(t, err)

		batch := client.NewStreamBatch()
		if batch == nil {
			return
		}
		tablename := schema.ChunkTables.TableFor(model.Now().Add(-time.Hour))
		batch.Add(tablename, "userID", 0, 240)

		var retrievedChunks []chunk.Chunk
		var wg sync.WaitGroup
		out := make(chan []chunk.Chunk)
		go func() {
			wg.Add(1)
			for c := range out {
				retrievedChunks = append(retrievedChunks, c...)
			}
			wg.Done()
		}()

		err = client.StreamChunks(context.Background(), batch, out)
		require.NoError(t, err)

		close(out)
		wg.Wait()
		require.Equal(t, 2000, len(retrievedChunks))

		sort.Sort(ByKey(retrievedChunks))
		sort.Sort(ByKey(chunks))
		for j := 0; j < len(retrievedChunks); j++ {
			require.Equal(t, chunks[j].ExternalKey(), retrievedChunks[j].ExternalKey(), strconv.Itoa(j))
		}
	})
}
