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
	chunk := &Chunk{}

	if v, ok := image["schema"]; ok {
		chunk.Schema = v.String()
	} else {
		return nil, errors.New("Failed to get schema from DynamoRecord")
	}

	if v, ok := image["s3_objects"]; ok {
		chunk.S3Objects = v.StringSet()
	} else {
		return nil, errors.New("Failed to get s3_objects from DynamoRecord")
	}

	if v, ok := image["partition"]; ok {
		chunk.Partition = v.String()
	} else {
		return nil, errors.New("Failed to get partition from DynamoRecord")
	}

	if v, ok := image["chunk_key"]; ok {
		chunk.ChunkKey = v.String()
	} else {
		return nil, errors.New("Failed to get chunk_key from DynamoRecord")
	}

	if v, ok := image["pk"]; ok {
		chunk.PK = v.String()
	} else {
		return nil, errors.New("Failed to get pk from DynamoRecord")
	}

	if v, ok := image["sk"]; ok {
		chunk.SK = v.String()
	} else {
		return nil, errors.New("Failed to get sk from DynamoRecord")
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
