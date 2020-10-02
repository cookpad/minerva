package repository

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/guregu/dynamo"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

type ChunkRepository interface {
	GetWritableChunks(schema, partition string, writableTotalSize int64) ([]*models.Chunk, error)
	GetMergableChunks(schema string, createdBefore time.Time, minChunkSize int64) ([]*models.Chunk, error)
	PutChunk(recordID string, objSize int64, schema, partition string, created time.Time) error
	UpdateChunk(chunk *models.Chunk, recordID string, objSize, writableSize int64) error
	FreezeChunk(chunk *models.Chunk) (*models.Chunk, error)
	DeleteChunk(chunk *models.Chunk) (*models.Chunk, error)
}

var (
	// ErrChunkNotWritable means updating after FreezedAt or over TotalSize
	ErrChunkNotWritable = fmt.Errorf("Update condition is not matched")
)

const (
	defaultChunkKeyPrefix = "chunk/"
	mergableChunkLimit    = 256
)

// ChunkDynamoDB is implementation of ChunkRepository for DynamoDB
type ChunkDynamoDB struct {
	KeyPrefix string
	table     dynamo.Table
}

// NewChunkDynamoDB is constructor of ChunkDynamoDB
func NewChunkDynamoDB(region, tableName string) *ChunkDynamoDB {
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(region)})
	table := db.Table(tableName)

	return &ChunkDynamoDB{
		KeyPrefix: defaultChunkKeyPrefix,
		table:     table,
	}
}

func (x *ChunkDynamoDB) chunkPK(schema string) string {
	if schema != "index" && schema != "message" {
		internal.Logger.Fatalf("Unsupported schema of chunk key: %s", schema)
	}

	return fmt.Sprintf("%s%s", x.KeyPrefix, schema)
}

// GetMergableChunks returns mergable chunks exceeding freezedAt or minChunkSize
func (x *ChunkDynamoDB) GetMergableChunks(schema string, createdBefore time.Time, minChunkSize int64) ([]*models.Chunk, error) {
	var chunks []*models.Chunk
	query := x.table.
		Get("pk", x.chunkPK(schema)).
		Filter("? <= 'total_size' OR 'created_at' <= ?", minChunkSize, createdBefore.UTC().Unix()).Limit(mergableChunkLimit)

	if err := query.All(&chunks); err != nil {
		return nil, errors.Wrap(err, "Failed get chunks")
	}

	return chunks, nil
}

// GetWritableChunks returns writable chunks for now (because chunks are not locked)
func (x *ChunkDynamoDB) GetWritableChunks(schema, partition string, writableTotalSize int64) ([]*models.Chunk, error) {
	var chunks []*models.Chunk
	query := x.table.
		Get("pk", x.chunkPK(schema)).
		Range("sk", dynamo.BeginsWith, partition+"/").
		Filter("'total_size' < ? AND 'freezed' = ?", writableTotalSize, false)

	if err := query.All(&chunks); err != nil {
		return nil, errors.Wrap(err, "Failed get chunks")
	}

	return chunks, nil
}

func (x *ChunkDynamoDB) PutChunk(recordID string, size int64, schema, partition string, created time.Time) error {
	chunkKey := uuid.New().String()
	chunk := &models.Chunk{
		PK: x.chunkPK(schema),
		SK: partition + "/" + chunkKey,

		Schema:    schema,
		Partition: partition,
		RecordIDs: []string{recordID},
		TotalSize: size,
		CreatedAt: created.Unix(),
		ChunkKey:  chunkKey,
		Freezed:   false,
	}

	if err := x.table.Put(chunk).Run(); err != nil {
		return errors.Wrap(err, "Failed to put chunk")
	}

	return nil
}

func (x *ChunkDynamoDB) UpdateChunk(chunk *models.Chunk, recordID string, objSize, writableSize int64) error {
	query := x.table.
		Update("pk", chunk.PK).
		Range("sk", chunk.SK).
		AddStringsToSet("record_ids", recordID).
		Add("total_size", objSize).
		If("total_size < ? AND 'freezed' = ?", writableSize, false)

	if err := query.Run(); err != nil {
		if isConditionalCheckErr(err) || isResourceNotFoundErr(err) {
			return ErrChunkNotWritable
		}
		return errors.Wrap(err, "Failed to update chunk")
	}

	return nil
}

func (x *ChunkDynamoDB) FreezeChunk(chunk *models.Chunk) (*models.Chunk, error) {
	query := x.table.
		Update("pk", chunk.PK).
		Range("sk", chunk.SK).
		Set("freezed", true).
		If("attribute_exists(pk) AND attribute_exists(sk)")

	var newChunk models.Chunk
	if err := query.OldValue(&newChunk); err != nil {
		if isResourceNotFoundErr(err) {
			return nil, nil // Chunk is no longer available
		}
		return nil, errors.Wrap(err, "Failed to update chunk")
	}

	return &newChunk, nil
}

func (x *ChunkDynamoDB) DeleteChunk(chunk *models.Chunk) (*models.Chunk, error) {
	var old models.Chunk
	if err := x.table.Delete("pk", chunk.PK).Range("sk", chunk.SK).OldValue(&old); err != nil {
		if isNoItemFoundErr(err) {
			return nil, nil // Ignore if item not found
		}

		return nil, errors.Wrap(err, "Failed to delete chunk")
	}

	return &old, nil
}
