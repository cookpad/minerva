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
	ChunkKey  string   `dynamo:"chunk_key"`
	Freezed   bool     `dynamo:"freezed"`

	// For DynamoDB
	PK string `dynamo:"pk"`
	SK string `dynamo:"sk"`
}

func (x *Chunk) ToS3ObjectSlice() ([]*S3Object, error) {
	var output []*S3Object
	for _, encObj := range x.S3Objects {
		obj, err := DecodeS3Object(encObj)
		if err != nil {
			return nil, errors.Wrap(err, "Failed DecodeS3Object")
		}

		output = append(output, obj)
	}

	return output, nil
}

// NewChunkFromDynamoEvent builds Chunk by DynamoDBAttributeValue
func NewChunkFromDynamoEvent(image map[string]events.DynamoDBAttributeValue) (*Chunk, error) {
	chunk := &Chunk{
		Schema:    image["schema"].String(),
		S3Objects: image["s3_objects"].StringSet(),
		Partition: image["partition"].String(),
		ChunkKey:  image["chunk_key"].String(),
		PK:        image["pk"].String(),
		SK:        image["sk"].String(),
	}

	if v, ok := image["total_size"]; ok {
		totalSize, err := v.Integer()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to read total_size")
		}
		chunk.TotalSize = totalSize
	} else {
		return nil, errors.New("Failed to get total_size from DynamoRecord")
	}

	if v, ok := image["created_at"]; ok {
		createdAt, err := v.Integer()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to read created_at")
		}
		chunk.CreatedAt = createdAt
	} else {
		return nil, errors.New("Failed to get created_at from DynamoRecord")
	}

	if v, ok := image["freezed"]; ok {
		chunk.Freezed = v.Boolean()
	} else {
		return nil, errors.New("Failed to get freezed from DynamoRecord")
	}

	return chunk, nil
}
