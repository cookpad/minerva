package models

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
)

type Chunk struct {
	Schema    string   `dynamo:"schema"`
	S3Objects []string `dynamo:"s3_objects,set"`
	TotalSize int64    `dynamo:"total_size"`
	Partition string   `dynamo:"partition"`
	CreatedAt int64    `dynamo:"created_at"`
	FreezedAt int64    `dynamo:"freezed_at"`

	// For DynamoDB
	PK string `dynamo:"pk"`
	SK string `dynamo:"sk"`
}

// NewChunkFromDynamoEvent builds Chunk by DynamoDBAttributeValue
func NewChunkFromDynamoEvent(image map[string]events.DynamoDBAttributeValue) (*Chunk, error) {
	totalSize, err := image["total_size"].Integer()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read total_size")
	}
	createdAt, err := image["created_at"].Integer()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read created_at")
	}
	freezedAt, err := image["freezed_at"].Integer()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read freezed_at")
	}

	return &Chunk{
		Schema:    image["schema"].String(),
		S3Objects: image["s3_objects"].StringSet(),
		TotalSize: totalSize,
		Partition: image["partition"].String(),
		CreatedAt: createdAt,
		FreezedAt: freezedAt,
		PK:        image["pk"].String(),
		SK:        image["sk"].String(),
	}, nil
}
