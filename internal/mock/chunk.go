package mock

import (
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/models"
)

// ChunkMockDB is mock of ChunkDynamoDB
type ChunkMockDB struct {
	Data map[string]map[string]*models.Chunk
}

// NewChunkMockDB  is constructor of ChunkMockDB
func NewChunkMockDB() *ChunkMockDB {
	return &ChunkMockDB{
		Data: map[string]map[string]*models.Chunk{},
	}
}

// GetWritableChunks returns writable chunks for now (because chunks are not locked)
func (x *ChunkMockDB) GetWritableChunks(schema, partition string, ts time.Time, writableTotalSize int64) ([]*models.Chunk, error) {
	pk := "chunk/" + schema
	dataMap, ok := x.Data[pk]
	if !ok {
		return nil, nil
	}

	var output []*models.Chunk
	for sk, chunk := range dataMap {
		if !strings.HasPrefix(sk, partition) {
			continue
		}

		if chunk.TotalSize < writableTotalSize && ts.UTC().Unix() < chunk.FreezedAt {
			output = append(output, chunk)
		}
	}

	return output, nil
}

// GetMergableChunks returns mergable chunks exceeding freezedAt or minChunkSize
func (x *ChunkMockDB) GetMergableChunks(schema string, ts time.Time, minChunkSize int64) ([]*models.Chunk, error) {
	pk := "chunk/" + schema
	dataMap, ok := x.Data[pk]
	if !ok {
		return nil, nil
	}

	var output []*models.Chunk
	for _, chunk := range dataMap {
		if minChunkSize <= chunk.TotalSize || chunk.FreezedAt <= ts.UTC().Unix() {
			output = append(output, chunk)
		}
	}

	return output, nil
}

// PutChunk saves a new chunk into DB. The chunk must be overwritten by UUID.
func (x *ChunkMockDB) PutChunk(obj models.S3Object, objSize int64, schema, partition string, created time.Time, freezed time.Time) error {
	chunkKey := uuid.New().String()
	pk := "chunk/" + schema
	sk := partition + "/" + chunkKey

	chunk := &models.Chunk{
		PK: pk,
		SK: sk,

		Schema:    schema,
		Partition: partition,
		S3Objects: []string{obj.Encode()},
		TotalSize: objSize,
		CreatedAt: created.Unix(),
		FreezedAt: freezed.Unix(),
		ChunkKey:  chunkKey,
	}

	pkMap, ok := x.Data[pk]
	if !ok {
		pkMap = map[string]*models.Chunk{}
		x.Data[pk] = pkMap
	}

	pkMap[sk] = chunk

	return nil
}
func (x *ChunkMockDB) UpdateChunk(chunk *models.Chunk, obj models.S3Object, objSize, writableSize int64, ts time.Time) error {
	dataMap, ok := x.Data[chunk.PK]
	if !ok {
		return repository.ErrChunkNotWritable
	}
	target, ok := dataMap[chunk.SK]
	if !ok {
		return repository.ErrChunkNotWritable
	}

	// This statement is not in go manner. Because aligning to DynamoDB Filter condition
	if target.TotalSize < writableSize && ts.UTC().Unix() < target.FreezedAt {
		target.TotalSize += objSize
		target.S3Objects = append(target.S3Objects, obj.Encode())
	} else {
		return repository.ErrChunkNotWritable
	}

	return nil
}

func (x *ChunkMockDB) DeleteChunk(chunk *models.Chunk) (*models.Chunk, error) {
	dataMap, ok := x.Data[chunk.PK]
	if !ok {
		return nil, nil
	}
	old, ok := dataMap[chunk.SK]
	if !ok {
		return nil, nil
	}

	delete(dataMap, chunk.SK)
	return old, nil
}
