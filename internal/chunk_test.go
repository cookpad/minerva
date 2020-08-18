package internal_test

import (
	"fmt"
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

	t.Run("Put a new chunk", func(tt *testing.T) {
		repo := newRepo()
		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}

		// No chunks are returned before putting chunk
		chunks, err := repo.GetWritableChunks("index", ts, 60)
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks))

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))

		// One chunk should be returned after put
		chunks, err = repo.GetWritableChunks("index", ts, 0)
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
		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k2",
			Region: "us-east-2",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))

		writeSize := int64(33)
		writeTS := ts.Add(time.Minute)
		chunks, err := repo.GetWritableChunks("index", writeTS, writeSize)
		require.NoError(tt, err)
		require.NoError(tt, repo.UpdateChunk(chunks[0], obj2, 39, ts.Add(time.Minute)))

		chunks, err = repo.GetWritableChunks("index", ts, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 1, len(chunks))
		require.Equal(tt, 2, len(chunks[0].S3Objects))
		assert.Equal(tt, int64(99), chunks[0].TotalSize)
	})

	t.Run("Put another chunk", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k3",
			Region: "us-east-3",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))

		chunks1, err := repo.GetWritableChunks("index", ts, 0)
		require.NoError(tt, err)

		require.NoError(tt, repo.PutChunk(obj2, 50, "index", ts))
		chunks2, err := repo.GetWritableChunks("index", ts, 0)
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

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))
		chunks1, err := repo.GetWritableChunks("index", ts, 0)
		require.NoError(tt, err)

		old, err := repo.DeleteChunk(chunks1[0])
		require.NoError(t, err)
		assert.Equal(t, chunks1[0].PK, old.PK)
		assert.Equal(t, chunks1[0].SK, old.SK)

		chunks2, err := repo.GetWritableChunks("index", ts, 0)
		require.NoError(t, err)
		assert.Equal(t, 0, len(chunks2))
	})

	t.Run("Fail to update chunk exceeding ChunkSizeMax", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k3",
			Region: "us-east-3",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))
		chunks, err := repo.GetWritableChunks("index", ts, 0)
		require.NoError(tt, err)

		assert.Equal(tt, internal.ErrUpdateChunk,
			repo.UpdateChunk(chunks[0], obj2, 40, ts.Add(time.Minute)))
		chunks, err = repo.GetWritableChunks("index", ts, 0)
		require.NoError(t, err)
		assert.Equal(tt, 1, len(chunks))
		assert.Equal(tt, 1, len(chunks[0].S3Objects))
		obj1d, err := internal.DecodeS3Object(chunks[0].S3Objects[0])
		assert.Equal(tt, "blue/k1", obj1d.Key)
	})

	t.Run("Fail to update chunk after freezed_at", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k3",
			Region: "us-east-3",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))
		chunks, err := repo.GetWritableChunks("index", ts, 0)
		require.NoError(tt, err)

		assert.Equal(tt, internal.ErrUpdateChunk,
			repo.UpdateChunk(chunks[0], obj2, 5, ts.Add(time.Minute*6)))
		chunks, err = repo.GetWritableChunks("index", ts, 0)
		require.NoError(t, err)
		assert.Equal(tt, 1, len(chunks))
		assert.Equal(tt, 1, len(chunks[0].S3Objects))
		obj1d, err := internal.DecodeS3Object(chunks[0].S3Objects[0])
		assert.Equal(tt, "blue/k1", obj1d.Key)
	})

	t.Run("Fail to update removed chunk", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k3",
			Region: "us-east-3",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))
		chunks1, err := repo.GetWritableChunks("index", ts, 0)
		require.NoError(tt, err)

		_, err = repo.DeleteChunk(chunks1[0])
		require.NoError(t, err)

		assert.Equal(tt, internal.ErrUpdateChunk,
			repo.UpdateChunk(chunks1[0], obj2, 30, ts.Add(time.Minute)))
		chunks2, err := repo.GetWritableChunks("index", ts, 0)
		require.NoError(t, err)
		assert.Equal(t, 0, len(chunks2))
	})

	t.Run("Get readable chunks (no available chunks)", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k2",
			Region: "us-east-2",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))
		require.NoError(tt, repo.PutChunk(obj2, 40, "index", ts))

		chunks1, err := repo.GetReadableChunks("index", ts.Add(time.Minute))
		require.NoError(t, err)
		assert.Equal(t, 0, len(chunks1))
	})

	t.Run("Get readable chunks (chunkSizeMin exceeded)", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k2",
			Region: "us-east-2",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))
		require.NoError(tt, repo.PutChunk(obj2, 80, "index", ts))

		chunks1, err := repo.GetReadableChunks("index", ts.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, 1, len(chunks1))
		assert.Equal(t, int64(80), chunks1[0].TotalSize) // obj3
		obj, err := internal.DecodeS3Object(chunks1[0].S3Objects[0])
		require.NoError(t, err)
		assert.Equal(t, "blue/k2", obj.Key)
	})

	t.Run("Get readable chunks (after FreezedAt)", func(tt *testing.T) {
		repo := newRepo()

		obj1 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k1",
			Region: "us-east-1",
		}
		obj2 := internal.S3Object{
			Bucket: "test-bucket",
			Key:    "blue/k2",
			Region: "us-east-2",
		}

		require.NoError(tt, repo.PutChunk(obj1, 60, "index", ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, "index", ts.Add(time.Minute)))

		chunks1, err := repo.GetReadableChunks("index", ts.Add(time.Minute*5))
		require.NoError(t, err)
		require.Equal(t, 1, len(chunks1))
		assert.Equal(t, int64(60), chunks1[0].TotalSize) // obj3
		obj, err := internal.DecodeS3Object(chunks1[0].S3Objects[0])
		require.NoError(t, err)
		assert.Equal(t, "blue/k1", obj.Key)
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
