package internal_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testChunkRepository(t *testing.T, newRepo func() internal.ChunkRepository) {
	ts := time.Now()

	const defaultShema = "index"
	const defaultPartition = "dt=2020-01-01"

	t.Run("Put a new chunk", func(tt *testing.T) {
		repo := newRepo()
		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}

		// No chunks are returned before putting chunk
		chunks, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 60)
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks))

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))

		// One chunk should be returned after put
		chunks, err = repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 1, len(chunks))
		require.Equal(tt, 1, len(chunks[0].S3Objects))
		obj1d, err := internal.DecodeS3Object(chunks[0].S3Objects[0])
		assert.Equal(tt, "test-bucket", obj1d.Bucket)
		assert.Equal(tt, "blue/k1", obj1d.Key)
		assert.Equal(tt, "us-east-1", obj1d.Region)
	})

	t.Run("Update existing chunk", func(tt *testing.T) {
		repo := newRepo()

		// Still only one chunk should be returned after UpdateChunk
		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-2", "test-bucket", "blue/k2"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))

		writeSize := int64(33)
		writeTS := ts.Add(time.Minute)
		chunks, err := repo.GetWritableChunks(defaultShema, defaultPartition, writeTS, writeSize)
		require.NoError(tt, err)
		require.NoError(tt, repo.UpdateChunk(chunks[0], obj2, 39, ts.Add(time.Minute)))

		chunks, err = repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 1, len(chunks))
		require.Equal(tt, 2, len(chunks[0].S3Objects))
		assert.Equal(tt, int64(99), chunks[0].TotalSize)
	})

	t.Run("Put another chunk", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-3", "test-bucket", "blue/k3"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))

		chunks1, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)

		require.NoError(tt, repo.PutChunk(obj2, 50, defaultShema, defaultPartition, ts))
		chunks2, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 2, len(chunks2))

		c1, c2 := chunks2[0], chunks2[1]
		if c2.SK == chunks1[0].SK {
			c1, c2 = c2, c1 // swap
		}
		assert.Equal(tt, int64(60), c1.TotalSize)
		assert.Equal(tt, int64(50), c2.TotalSize)
	})

	t.Run("Remove a chunk", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))
		chunks1, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)

		old, err := repo.DeleteChunk(chunks1[0])
		require.NoError(tt, err)
		assert.Equal(tt, chunks1[0].PK, old.PK)
		assert.Equal(tt, chunks1[0].SK, old.SK)

		chunks2, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks2))
	})

	t.Run("Fail to update chunk exceeding ChunkSizeMax", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-3", "test-bucket", "blue/k3"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))
		chunks, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)

		assert.Equal(tt, internal.ErrUpdateChunk,
			repo.UpdateChunk(chunks[0], obj2, 40, ts.Add(time.Minute)))
		chunks, err = repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 1, len(chunks))
		assert.Equal(tt, 1, len(chunks[0].S3Objects))
		obj1d, err := internal.DecodeS3Object(chunks[0].S3Objects[0])
		assert.Equal(tt, "blue/k1", obj1d.Key)
	})

	t.Run("Fail to update chunk after freezed_at", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-3", "test-bucket", "blue/k3"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))
		chunks, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)

		assert.Equal(tt, internal.ErrUpdateChunk,
			repo.UpdateChunk(chunks[0], obj2, 5, ts.Add(time.Minute*6)))
		chunks, err = repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 1, len(chunks))
		assert.Equal(tt, 1, len(chunks[0].S3Objects))
		obj1d, err := internal.DecodeS3Object(chunks[0].S3Objects[0])
		assert.Equal(tt, "blue/k1", obj1d.Key)
	})

	t.Run("Fail to update removed chunk", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-3", "test-bucket", "blue/k3"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))
		chunks1, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)

		_, err = repo.DeleteChunk(chunks1[0])
		require.NoError(tt, err)

		assert.Equal(tt, internal.ErrUpdateChunk,
			repo.UpdateChunk(chunks1[0], obj2, 30, ts.Add(time.Minute)))
		chunks2, err := repo.GetWritableChunks(defaultShema, defaultPartition, ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks2))
	})

	t.Run("Get readable chunks (no available chunks)", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-2", "test-bucket", "blue/k2"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))
		require.NoError(tt, repo.PutChunk(obj2, 40, defaultShema, defaultPartition, ts))

		chunks1, err := repo.GetReadableChunks(defaultShema, ts.Add(time.Minute))
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks1))
	})

	t.Run("Get readable chunks (chunkSizeMin exceeded)", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-2", "test-bucket", "blue/k2"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))
		require.NoError(tt, repo.PutChunk(obj2, 80, defaultShema, defaultPartition, ts))

		chunks1, err := repo.GetReadableChunks(defaultShema, ts.Add(time.Minute))
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks1))
		assert.Equal(tt, int64(80), chunks1[0].TotalSize) // obj3
		obj, err := internal.DecodeS3Object(chunks1[0].S3Objects[0])
		require.NoError(tt, err)
		assert.Equal(tt, "blue/k2", obj.Key)
	})

	t.Run("Get readable chunks (after FreezedAt)", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-2", "test-bucket", "blue/k2"}

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, defaultPartition, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultShema, defaultPartition, ts.Add(time.Minute)))

		chunks1, err := repo.GetReadableChunks(defaultShema, ts.Add(time.Minute*5))
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks1))
		assert.Equal(tt, int64(60), chunks1[0].TotalSize) // obj3
		obj, err := internal.DecodeS3Object(chunks1[0].S3Objects[0])
		require.NoError(tt, err)
		assert.Equal(tt, "blue/k1", obj.Key)
	})

	t.Run("Put different partition chunks", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-2", "test-bucket", "blue/k2"}

		const p1, p2 = "dt=2020-01-01", "dt=2020-01-02"
		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, p1, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultShema, p2, ts))

		chunks1, err := repo.GetWritableChunks(defaultShema, p1, ts, 0)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks1))
		assert.Equal(tt, p1, chunks1[0].Partition)
		assert.Equal(tt, int64(60), chunks1[0].TotalSize)
	})

	t.Run("Update specific parition chunk", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-2", "test-bucket", "blue/k2"}
		obj3 := internal.S3Object{"us-east-3", "test-bucket", "blue/k3"}

		const p1, p2 = "dt=2020-01-01", "dt=2020-01-02"
		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, p1, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultShema, p2, ts))

		chunks1, err := repo.GetWritableChunks(defaultShema, p2, ts, 0)
		require.NoError(tt, err)
		require.NoError(tt, repo.UpdateChunk(chunks1[0], obj3, 29, ts))

		chunks2, err := repo.GetWritableChunks(defaultShema, p2, ts, 0)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks2))
		require.Equal(tt, 2, len(chunks2[0].S3Objects))
		assert.Equal(tt, int64(99), chunks2[0].TotalSize)

		objSet := batchDecodeS3Object(chunks2[0].S3Objects)
		assert.Contains(tt, objSet, obj2)
		assert.Contains(tt, objSet, obj3)
		assert.NotContains(tt, objSet, obj1)
	})

	t.Run("Get readable chunks of different partitions", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{"us-east-1", "test-bucket", "blue/k1"}
		obj2 := internal.S3Object{"us-east-2", "test-bucket", "blue/k2"}

		const p1, p2 = "dt=2020-01-01", "dt=2020-01-02"
		require.NoError(tt, repo.PutChunk(obj1, 60, defaultShema, p1, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultShema, p2, ts))

		chunks1, err := repo.GetReadableChunks(defaultShema, ts.Add(time.Hour))
		require.NoError(tt, err)
		require.Equal(tt, 2, len(chunks1))
		sizeList := []int64{chunks1[0].TotalSize, chunks1[1].TotalSize}
		assert.Contains(tt, sizeList, int64(60))
		assert.Contains(tt, sizeList, int64(70))
		assert.NotContains(tt, sizeList, int64(80))
	})
}

func TestChunkDynamoDB(t *testing.T) {
	region := os.Getenv("MINERVA_TEST_REGION")
	table := os.Getenv("MINERVA_TEST_TABLE")

	if region == "" || table == "" {
		t.Skip("Both of MINERVA_TEST_REGION and MINERVA_TEST_TABLE are required")
	}

	newRepo := func() internal.ChunkRepository {
		repo := internal.NewChunkDynamoDB(region, table)

		// For independent testing
		repo.ChunkKeyPrefix = fmt.Sprintf("chunk/%s/", uuid.New().String())

		// To simplify test
		repo.ChunkSizeMax = 100
		repo.ChunkSizeMin = 80

		return repo
	}

	testChunkRepository(t, newRepo)
}

// Test helper
func batchDecodeS3Object(encodedObjects []string) []internal.S3Object {
	var output []internal.S3Object
	for _, encobj := range encodedObjects {
		obj, err := internal.DecodeS3Object(encobj)
		if err != nil {
			log.Fatalf("Failed decode S3 object: %s", encobj)
		}

		output = append(output, *obj)
	}

	return output
}
