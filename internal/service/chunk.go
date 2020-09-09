package service

import (
	"time"

	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/models"
)

const (
	DefaultChunkKeyPrefix    = "chunk/"
	DefaultChunkFreezedAfter = time.Minute * 5
	DefaultChunkChunkMaxSize = 200 * 1000 * 1000
	DefaultChunkChunkMinSize = 150 * 1000 * 1000
)

type ChunkService struct {
	repo repository.ChunkRepository
	args *ChunkServiceArguments
}

type ChunkServiceArguments struct {
	FreezedAfter time.Duration
	ChunkMaxSize int64
	ChunkMinSize int64
}

func NewChunkService(repo repository.ChunkRepository, args *ChunkServiceArguments) *ChunkService {
	service := &ChunkService{
		repo: repo,
		args: args,
	}

	if service.args == nil {
		service.args = &ChunkServiceArguments{}
	}

	if service.args.FreezedAfter == 0 {
		service.args.FreezedAfter = DefaultChunkFreezedAfter
	}
	if service.args.ChunkMaxSize == 0 {
		service.args.ChunkMaxSize = DefaultChunkChunkMaxSize
	}
	if service.args.ChunkMinSize == 0 {
		service.args.ChunkMinSize = DefaultChunkChunkMinSize
	}

	return service
}

func (x *ChunkService) GetWritableChunks(schema, partition string, objSize int64) ([]*models.Chunk, error) {
	writableTotalSize := minInt64(x.args.ChunkMinSize, x.args.ChunkMaxSize-objSize)
	return x.repo.GetWritableChunks(schema, partition, writableTotalSize)
}

func (x *ChunkService) GetMergableChunks(schema string, now time.Time) ([]*models.Chunk, error) {
	return x.repo.GetMergableChunks(schema, now.Add(-x.args.FreezedAfter), x.args.ChunkMinSize)
}

func (x *ChunkService) PutChunk(obj models.S3Object, size int64, schema, partition string, now time.Time) error {
	return x.repo.PutChunk(obj, size, schema, partition, now)
}

func (x *ChunkService) UpdateChunk(chunk *models.Chunk, obj models.S3Object, objSize int64) error {
	writableTotalSize := minInt64(x.args.ChunkMinSize, x.args.ChunkMaxSize-objSize)
	return x.repo.UpdateChunk(chunk, obj, objSize, writableTotalSize)
}

func (x *ChunkService) FreezeChunk(chunk *models.Chunk) (*models.Chunk, error) {
	return x.repo.FreezeChunk(chunk)
}

func (x *ChunkService) DeleteChunk(chunk *models.Chunk) (*models.Chunk, error) {
	return x.repo.DeleteChunk(chunk)
}

func (x *ChunkService) IsMergableChunk(chunk *models.Chunk, ts time.Time) bool {
	return x.args.ChunkMinSize <= chunk.TotalSize && chunk.CreatedAt <= ts.UTC().Add(x.args.FreezedAfter).Unix()
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
