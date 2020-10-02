package service_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testChunkService(t *testing.T, newService func() *service.ChunkService) {
	ts := time.Now()

	const defaultSchema = "index"
	const defaultPartition = "dt=2020-01-01"

	t.Run("Put a new chunk", func(tt *testing.T) {
		repo := newService()
		obj1 := uuid.New().String()

		// No chunks are returned before putting chunk
		chunks, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 60)
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks))

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))

		// One chunk should be returned after put
		chunks, err = repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks))
		require.Equal(tt, 1, len(chunks[0].RecordIDs))
		require.Contains(tt, chunks[0].RecordIDs, obj1)
	})

	t.Run("Update existing chunk", func(tt *testing.T) {
		repo := newService()

		// Still only one chunk should be returned after UpdateChunk
		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 40, defaultSchema, defaultPartition, ts))

		writeSize := int64(33)
		chunks, err := repo.GetWritableChunks(defaultSchema, defaultPartition, writeSize)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks))
		require.NoError(tt, repo.UpdateChunk(chunks[0], obj2, 30))

		chunks, err = repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks))
		require.Equal(tt, 2, len(chunks[0].RecordIDs))
		assert.Equal(tt, int64(70), chunks[0].TotalSize)
	})

	t.Run("Put another chunk", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))

		chunks1, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)

		require.NoError(tt, repo.PutChunk(obj2, 50, defaultSchema, defaultPartition, ts))
		chunks2, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
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
		repo := newService()

		obj1 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))
		chunks1, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)

		require.Equal(tt, 1, len(chunks1))
		old, err := repo.DeleteChunk(chunks1[0])
		require.NoError(tt, err)
		assert.Equal(tt, chunks1[0].PK, old.PK)
		assert.Equal(tt, chunks1[0].SK, old.SK)

		chunks2, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks2))
	})

	t.Run("Fail to update chunk exceeding ChunkSizeMax", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))
		chunks, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)

		assert.Equal(tt, repository.ErrChunkNotWritable, repo.UpdateChunk(chunks[0], obj2, 40))
		chunks, err = repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 1, len(chunks))
		assert.Equal(tt, 1, len(chunks[0].RecordIDs))
		assert.Equal(tt, obj1, chunks[0].RecordIDs[0])
	})

	t.Run("Fail to update chunk exceeding ChunkSizeMin", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()
		obj3 := uuid.New().String()

		// Not exceeded
		require.NoError(tt, repo.PutChunk(obj1, 79, defaultSchema, defaultPartition, ts))
		chunks, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)
		require.Equal(t, 1, len(chunks))
		// Exceeded
		require.NoError(tt, repo.UpdateChunk(chunks[0], obj2, 1))

		// Updating exceeded chunk
		require.Equal(tt, repository.ErrChunkNotWritable, repo.UpdateChunk(chunks[0], obj3, 1))

		chunks, err = repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)
		require.Equal(tt, 0, len(chunks))

		mergable, err := repo.GetMergableChunks(defaultSchema, ts.Add(time.Hour))
		require.Equal(tt, 2, len(mergable[0].RecordIDs))
		assert.Contains(tt, mergable[0].RecordIDs, obj1)
		assert.Contains(tt, mergable[0].RecordIDs, obj2)
		assert.NotContains(tt, mergable[0].RecordIDs, obj3)
		assert.Equal(tt, int64(80), mergable[0].TotalSize)
	})

	t.Run("Fail to update removed chunk", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))
		chunks1, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)

		_, err = repo.DeleteChunk(chunks1[0])
		require.NoError(tt, err)

		assert.Equal(tt, repository.ErrChunkNotWritable,
			repo.UpdateChunk(chunks1[0], obj2, 30))
		chunks2, err := repo.GetWritableChunks(defaultSchema, defaultPartition, 0)
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks2))
	})

	t.Run("Get readable chunks (no available chunks)", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))
		require.NoError(tt, repo.PutChunk(obj2, 40, defaultSchema, defaultPartition, ts))

		chunks1, err := repo.GetMergableChunks(defaultSchema, ts.Add(testChunkFreezedAfter-time.Second))
		require.NoError(tt, err)
		assert.Equal(tt, 0, len(chunks1))
	})

	t.Run("Get readable chunks (chunkSizeMin exceeded)", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))
		require.NoError(tt, repo.PutChunk(obj2, 80, defaultSchema, defaultPartition, ts))

		chunks1, err := repo.GetMergableChunks(defaultSchema, ts.Add(time.Minute))
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks1))
		assert.Equal(tt, int64(80), chunks1[0].TotalSize) // obj3
		assert.Contains(tt, chunks1[0].RecordIDs, obj2)
	})

	t.Run("Get readable chunks (after FreezedAt)", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultSchema, defaultPartition, ts.Add(time.Minute)))

		chunks1, err := repo.GetMergableChunks(defaultSchema, ts.Add(testChunkFreezedAfter))
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks1))
		assert.Equal(tt, int64(60), chunks1[0].TotalSize) // obj3
		assert.Contains(tt, chunks1[0].RecordIDs, obj1)
	})

	t.Run("Put different partition chunks", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		const p1, p2 = "dt=2020-01-01", "dt=2020-01-02"
		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, p1, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultSchema, p2, ts))

		chunks1, err := repo.GetWritableChunks(defaultSchema, p1, 0)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks1))
		assert.Equal(tt, p1, chunks1[0].Partition)
		assert.Equal(tt, int64(60), chunks1[0].TotalSize)
	})

	t.Run("Update specific parition chunk", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()
		obj3 := uuid.New().String()

		const p1, p2 = "dt=2020-01-01", "dt=2020-01-02"
		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, p1, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultSchema, p2, ts))

		chunks1, err := repo.GetWritableChunks(defaultSchema, p2, 0)
		require.NoError(tt, err)
		require.NoError(tt, repo.UpdateChunk(chunks1[0], obj3, 29))

		chunks2, err := repo.GetWritableChunks(defaultSchema, p2, 0)
		require.NoError(tt, err)
		require.Equal(tt, 0, len(chunks2))

		chunks3, err := repo.GetMergableChunks(defaultSchema, ts.Add(time.Hour))
		require.Equal(tt, 2, len(chunks3))
		tgt := chunks3[0]
		if tgt.SK != chunks1[0].SK {
			tgt = chunks3[1]
		}
		require.Equal(tt, 2, len(tgt.RecordIDs))
		assert.Equal(tt, int64(99), tgt.TotalSize)

		assert.Contains(tt, tgt.RecordIDs, obj2)
		assert.Contains(tt, tgt.RecordIDs, obj3)
		assert.NotContains(tt, tgt.RecordIDs, obj1)
	})

	t.Run("Get readable chunks of different partitions", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()

		const p1, p2 = "dt=2020-01-01", "dt=2020-01-02"
		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, p1, ts))
		require.NoError(tt, repo.PutChunk(obj2, 70, defaultSchema, p2, ts))

		chunks1, err := repo.GetMergableChunks(defaultSchema, ts.Add(time.Hour))
		require.NoError(tt, err)
		require.Equal(tt, 2, len(chunks1))
		sizeList := []int64{chunks1[0].TotalSize, chunks1[1].TotalSize}
		assert.Contains(tt, sizeList, int64(60))
		assert.Contains(tt, sizeList, int64(70))
		assert.NotContains(tt, sizeList, int64(80))
	})

	t.Run("Can not update freezed chunk", func(tt *testing.T) {
		repo := newService()

		obj1 := uuid.New().String()
		obj2 := uuid.New().String()
		obj3 := uuid.New().String()

		require.NoError(tt, repo.PutChunk(obj1, 60, defaultSchema, defaultPartition, ts))

		chunks1, err := repo.GetMergableChunks(defaultSchema, ts.Add(time.Hour))
		require.NoError(tt, err)
		require.Equal(tt, 1, len(chunks1))

		// Can update after GetMergableChunks (but not freezed)
		require.NoError(tt, repo.UpdateChunk(chunks1[0], obj2, 10))

		// And freeze the chunk
		newChunk, err := repo.FreezeChunk(chunks1[0])

		// Can not update after freezed
		require.Equal(tt, repository.ErrChunkNotWritable, repo.UpdateChunk(chunks1[0], obj3, 1))

		assert.Contains(tt, newChunk.RecordIDs, obj1)
		assert.Contains(tt, newChunk.RecordIDs, obj2)
		assert.NotContains(tt, newChunk.RecordIDs, obj3)
	})
}

const (
	testChunkSizeMax      = 100
	testChunkSizeMin      = 80
	testChunkFreezedAfter = time.Minute * 5
)

func TestChunkDynamoDB(t *testing.T) {
	region := os.Getenv("MINERVA_TEST_REGION")
	table := os.Getenv("MINERVA_TEST_TABLE")

	if region == "" || table == "" {
		t.Skip("Both of MINERVA_TEST_REGION and MINERVA_TEST_TABLE are required")
	}

	newService := func() *service.ChunkService {
		repo := repository.NewChunkDynamoDB(region, table)

		// For independent testing
		repo.KeyPrefix = fmt.Sprintf("chunk/%s/", uuid.New().String())

		// To simplify test
		svc := service.NewChunkService(repo, &service.ChunkServiceArguments{
			FreezedAfter: testChunkFreezedAfter,
			ChunkMaxSize: testChunkSizeMax,
			ChunkMinSize: testChunkSizeMin,
		})

		return svc
	}

	testChunkService(t, newService)
}

func TestChunkMockDB(t *testing.T) {
	newService := func() *service.ChunkService {
		repo := mock.NewChunkMockDB()

		// To simplify test
		svc := service.NewChunkService(repo, &service.ChunkServiceArguments{
			FreezedAfter: testChunkFreezedAfter,
			ChunkMaxSize: testChunkSizeMax,
			ChunkMinSize: testChunkSizeMin,
		})

		return svc
	}

	testChunkService(t, newService)
}

// Test helper
func batchDecodeS3Object(encodedObjects []string) []models.S3Object {
	var output []models.S3Object
	for _, encobj := range encodedObjects {
		obj, err := models.DecodeS3Object(encobj)
		if err != nil {
			log.Fatalf("Failed decode S3 object: %s", encobj)
		}

		output = append(output, *obj)
	}

	return output
}
