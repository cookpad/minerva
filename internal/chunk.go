package internal

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/guregu/dynamo"
	"github.com/pkg/errors"
)

type Chunk struct {
	PK string `dynamo:"pk"`
	SK string `dynamo:"sk"`

	Schema    string   `dynamo:"schema"`
	S3Objects []string `dynamo:"s3_objects,set"`
	TotalSize int64    `dynamo:"total_size"`
	CreatedAt int64    `dynamo:"created_at"`
	FreezedAt int64    `dynamo:"freezed_at"`
}

type ChunkRepository interface {
	GetChunks(schema string) ([]*Chunk, error)
	PutChunk(obj S3Object, size int64, schema string, ts time.Time) error
	UpdateChunk(chunk *Chunk, obj S3Object, size int64, ts time.Time) error
	DeleteChunk(chunk *Chunk) (*Chunk, error)
}

var (
	// ErrUpdateChunk means updating after FreezedAt or over TotalSize
	ErrUpdateChunk = fmt.Errorf("Update condition is not matched")
)

const (
	defaultChunkKeyPrefix    = "chunk/"
	defaultChunkFreezedAfter = time.Minute * 5
	defaultChunkSizeMax      = 128 * 1000 * 1000
)

// ChunkDynamoDB is implementation of ChunkRepository for DynamoDB
type ChunkDynamoDB struct {
	table dynamo.Table

	ChunkKeyPrefix    string
	ChunkFreezedAfter time.Duration
	ChunkSizeMax      int64
}

// NewChunkDynamoDB is constructor of ChunkDynamoDB
func NewChunkDynamoDB(region, tableName string) *ChunkDynamoDB {
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(region)})
	table := db.Table(tableName)

	return &ChunkDynamoDB{
		table:             table,
		ChunkFreezedAfter: defaultChunkFreezedAfter,
		ChunkKeyPrefix:    defaultChunkKeyPrefix,
		ChunkSizeMax:      defaultChunkSizeMax,
	}
}

func (x *ChunkDynamoDB) chunkKey(schema string) string {
	if schema != "index" && schema != "message" {
		Logger.Fatalf("Unsupported schema of chunk key: %s", schema)
	}

	return x.ChunkKeyPrefix + schema
}

func (x *ChunkDynamoDB) GetChunks(schema string) ([]*Chunk, error) {
	var chunks []*Chunk
	query := x.table.Get("pk", x.chunkKey(schema))

	if err := query.All(&chunks); err != nil {
		return nil, errors.Wrap(err, "Failed get chunks")
	}

	return chunks, nil
}

func (x *ChunkDynamoDB) PutChunk(obj S3Object, size int64, schema string, ts time.Time) error {
	chunk := &Chunk{
		PK: x.chunkKey(schema),
		SK: uuid.New().String(),

		Schema:    schema,
		S3Objects: []string{obj.Encode()},
		TotalSize: size,
		CreatedAt: ts.Unix(),
		FreezedAt: ts.Add(x.ChunkFreezedAfter).Unix(),
	}

	if err := x.table.Put(chunk).Run(); err != nil {
		return errors.Wrap(err, "Failed to put chunk")
	}

	return nil
}

func (x *ChunkDynamoDB) UpdateChunk(chunk *Chunk, obj S3Object, size int64, ts time.Time) error {
	query := x.table.
		Update("pk", chunk.PK).
		Range("sk", chunk.SK).
		AddStringsToSet("s3_objects", obj.Encode()).
		Add("total_size", size).
		If("total_size < ? AND ? < freezed_at", x.ChunkSizeMax-size, ts.Unix())

	if err := query.Run(); err != nil {
		if isConditionalCheckErr(err) || isResourceNotFoundErr(err) {
			return ErrUpdateChunk
		}
		return errors.Wrap(err, "Failed to update chunk")
	}

	return nil
}

func (x *ChunkDynamoDB) DeleteChunk(chunk *Chunk) (*Chunk, error) {
	var old Chunk
	if err := x.table.Delete("pk", chunk.PK).Range("sk", chunk.SK).OldValue(&old); err != nil {
		return nil, errors.Wrap(err, "Failed to delete chunk")
	}

	return &old, nil
}
