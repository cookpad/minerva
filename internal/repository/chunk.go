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
	GetWritableChunks(schema, partition string, ts time.Time, sizeLimit int64) ([]*models.Chunk, error)
	GetMergableChunks(schema string, ts time.Time, sizeLeast int64) ([]*models.Chunk, error)
	PutChunk(obj models.S3Object, size int64, schema, partition string, created time.Time, freezed time.Time) error
	UpdateChunk(chunk *models.Chunk, obj models.S3Object, size, writableSize int64, ts time.Time) error
	DeleteChunk(chunk *models.Chunk) (*models.Chunk, error)
}

var (
	// ErrChunkNotWritable means updating after FreezedAt or over TotalSize
	ErrChunkNotWritable = fmt.Errorf("Update condition is not matched")
)

const (
	defaultChunkKeyPrefix = "chunk/"
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

func (x *ChunkDynamoDB) GetMergableChunks(schema string, ts time.Time, sizeLeast int64) ([]*models.Chunk, error) {
	var chunks []*models.Chunk
	query := x.table.
		Get("pk", x.chunkPK(schema)).
		Filter("? <= 'total_size' OR 'freezed_at' <= ?", sizeLeast, ts.UTC().Unix())

	if err := query.All(&chunks); err != nil {
		return nil, errors.Wrap(err, "Failed get chunks")
	}

	return chunks, nil
}

func (x *ChunkDynamoDB) GetWritableChunks(schema, partition string, ts time.Time, sizeLimit int64) ([]*models.Chunk, error) {
	var chunks []*models.Chunk
	query := x.table.
		Get("pk", x.chunkPK(schema)).
		Range("sk", dynamo.BeginsWith, partition+"/").
		Filter("'total_size' <= ? AND ? < 'freezed_at'", sizeLimit, ts.UTC().Unix())

	if err := query.All(&chunks); err != nil {
		return nil, errors.Wrap(err, "Failed get chunks")
	}

	return chunks, nil
}

func (x *ChunkDynamoDB) PutChunk(obj models.S3Object, size int64, schema, partition string, created time.Time, freezed time.Time) error {
	chunk := &models.Chunk{
		PK: x.chunkPK(schema),
		SK: partition + "/" + uuid.New().String(),

		Schema:    schema,
		Partition: partition,
		S3Objects: []string{obj.Encode()},
		TotalSize: size,
		CreatedAt: created.Unix(),
		FreezedAt: freezed.Unix(),
	}

	if err := x.table.Put(chunk).Run(); err != nil {
		return errors.Wrap(err, "Failed to put chunk")
	}

	return nil
}

func (x *ChunkDynamoDB) UpdateChunk(chunk *models.Chunk, obj models.S3Object, size, writableSize int64, ts time.Time) error {
	query := x.table.
		Update("pk", chunk.PK).
		Range("sk", chunk.SK).
		AddStringsToSet("s3_objects", obj.Encode()).
		Add("total_size", size).
		If("total_size < ? AND ? < freezed_at", writableSize, ts.UTC().Unix())

	if err := query.Run(); err != nil {
		if isConditionalCheckErr(err) || isResourceNotFoundErr(err) {
			return ErrChunkNotWritable
		}
		return errors.Wrap(err, "Failed to update chunk")
	}

	return nil
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
