package service

import (
	"time"

	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/models"
)

const (
	defaultChunkKeyPrefix    = "chunk/"
	defaultChunkFreezedAfter = time.Minute * 5
	defaultChunkSizeMax      = 128 * 1000 * 1000
	defaultChunkSizeMin      = 100 * 1000 * 1000
)

type ChunkService struct {
	repo repository.ChunkRepository
	args *ChunkServiceArguments
}

type ChunkServiceArguments struct {
	FreezedAfter time.Duration
	SizeMax      int64
	SizeMin      int64
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
		service.args.FreezedAfter = defaultChunkFreezedAfter
	}
	if service.args.SizeMax == 0 {
		service.args.SizeMax = defaultChunkSizeMax
	}
	if service.args.SizeMin == 0 {
		service.args.SizeMin = defaultChunkSizeMax
	}

	return service
}

func (x *ChunkService) GetWritableChunks(schema, partition string, ts time.Time, size int64) ([]*models.Chunk, error) {
	return x.repo.GetWritableChunks(schema, partition, ts, x.args.SizeMax-size)
}

func (x *ChunkService) GetMergableChunks(schema string, ts time.Time) ([]*models.Chunk, error) {
	return x.repo.GetMergableChunks(schema, ts, x.args.SizeMin)
}

func (x *ChunkService) PutChunk(obj models.S3Object, size int64, schema, partition string, ts time.Time) error {
	return x.repo.PutChunk(obj, size, schema, partition, ts, ts.Add(x.args.FreezedAfter))
}

func (x *ChunkService) UpdateChunk(chunk *models.Chunk, obj models.S3Object, size int64, ts time.Time) error {
	return x.repo.UpdateChunk(chunk, obj, size, x.args.SizeMax-size, ts)
}

func (x *ChunkService) DeleteChunk(chunk *models.Chunk) (*models.Chunk, error) {
	return x.repo.DeleteChunk(chunk)
}

func (x *ChunkService) IsMergableChunk(chunk *models.Chunk, ts time.Time) bool {
	return x.args.SizeMin <= chunk.TotalSize && chunk.FreezedAt <= ts.UTC().Unix()
}
