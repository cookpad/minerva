package service

import (
	"time"

	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/models"
)

const (
	DefaultChunkKeyPrefix    = "chunk/"
	DefaultChunkFreezedAfter = time.Minute * 5
	DefaultChunkChunkMaxSize = 128 * 1000 * 1000
	DefaultChunkChunkMinSize = 100 * 1000 * 1000
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

func (x *ChunkService) GetWritableChunks(schema, partition string, ts time.Time, objSize int64) ([]*models.Chunk, error) {
	writableTotalSize := minInt64(x.args.ChunkMinSize, x.args.ChunkMaxSize-objSize)
	return x.repo.GetWritableChunks(schema, partition, ts, writableTotalSize)
}

func (x *ChunkService) GetMergableChunks(schema string, ts time.Time) ([]*models.Chunk, error) {
	return x.repo.GetMergableChunks(schema, ts, x.args.ChunkMinSize)
}

func (x *ChunkService) PutChunk(obj models.S3Object, size int64, schema, partition string, ts time.Time) error {
	return x.repo.PutChunk(obj, size, schema, partition, ts, ts.Add(x.args.FreezedAfter))
}

func (x *ChunkService) UpdateChunk(chunk *models.Chunk, obj models.S3Object, objSize int64, ts time.Time) error {
	writableTotalSize := minInt64(x.args.ChunkMinSize, x.args.ChunkMaxSize-objSize)
	return x.repo.UpdateChunk(chunk, obj, objSize, writableTotalSize, ts)
}

func (x *ChunkService) DeleteChunk(chunk *models.Chunk) (*models.Chunk, error) {
	return x.repo.DeleteChunk(chunk)
}

func (x *ChunkService) IsMergableChunk(chunk *models.Chunk, ts time.Time) bool {
	return x.args.ChunkMinSize <= chunk.TotalSize && chunk.FreezedAt <= ts.UTC().Unix()
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
