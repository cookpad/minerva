package main_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/lambda"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dispatcher "github.com/m-mizutani/minerva/lambda/dispatcher"
)

func TestDispatcher(t *testing.T) {
	ExceedSize := fmt.Sprintf("%d", service.DefaultChunkChunkMinSize)
	NotExceedSize := fmt.Sprintf("%d", service.DefaultChunkChunkMinSize-1)

	t.Run("Dispatch insert event", func(tt *testing.T) {
		event := createBaseEvent()
		event.Records[0].Change.NewImage["total_size"] = events.NewNumberAttribute(ExceedSize)

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := lambda.HandlerArguments{
			EnvVars: lambda.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: mock.NewChunkMockDB(),
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.NotNil(tt, sqsClient)
		require.Equal(tt, 1, len(sqsClient.Input))
		assert.Equal(tt, "https://sqs.eu-west-2.amazonaws.com/test-url", *sqsClient.Input[0].QueueUrl)

		var q models.MergeQueue
		require.NoError(tt, json.Unmarshal([]byte(*sqsClient.Input[0].MessageBody), &q))
		assert.Equal(tt, models.ParquetSchemaName("index"), q.Schema)
		assert.Equal(tt, 2, len(q.SrcObjects))
		assert.Contains(tt, q.SrcObjects, &models.S3Object{
			Region: "region1",
			Bucket: "bucket1",
			Key:    "key1",
		})
		assert.Contains(tt, q.SrcObjects, &models.S3Object{
			Region: "region2",
			Bucket: "bucket2",
			Key:    "key2",
		})
		assert.NotContains(tt, q.SrcObjects, &models.S3Object{
			Region: "region3",
			Bucket: "bucket3",
			Key:    "key3",
		})
	})

	t.Run("Dispatch update event", func(tt *testing.T) {
		event := createBaseEvent()
		event.Records[0].EventName = "MODIFY"
		event.Records[0].Change.NewImage["total_size"] = events.NewNumberAttribute(ExceedSize)

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := lambda.HandlerArguments{
			EnvVars: lambda.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: mock.NewChunkMockDB(),
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.NotNil(tt, sqsClient)
		require.Equal(tt, 1, len(sqsClient.Input))
		assert.Equal(tt, "https://sqs.eu-west-2.amazonaws.com/test-url", *sqsClient.Input[0].QueueUrl)

		var q models.MergeQueue
		require.NoError(tt, json.Unmarshal([]byte(*sqsClient.Input[0].MessageBody), &q))
		assert.Equal(tt, models.ParquetSchemaName("index"), q.Schema)
		assert.Equal(tt, 2, len(q.SrcObjects))
		assert.Contains(tt, q.SrcObjects, &models.S3Object{
			Region: "region1",
			Bucket: "bucket1",
			Key:    "key1",
		})
		assert.Contains(tt, q.SrcObjects, &models.S3Object{
			Region: "region2",
			Bucket: "bucket2",
			Key:    "key2",
		})
	})

	t.Run("Ignore remove event", func(tt *testing.T) {
		event := createBaseEvent()
		event.Records[0].EventName = "REMOVE"

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := lambda.HandlerArguments{
			EnvVars: lambda.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: mock.NewChunkMockDB(),
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.NotNil(tt, sqsClient)
		require.Equal(tt, 0, len(sqsClient.Input))
	})

	t.Run("Ignore not exceeded totalSize", func(tt *testing.T) {
		event := createBaseEvent()
		event.Records[0].Change.NewImage["total_size"] = events.NewNumberAttribute(NotExceedSize)

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := lambda.HandlerArguments{
			EnvVars: lambda.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: mock.NewChunkMockDB(),
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.NotNil(tt, sqsClient)
		require.Equal(tt, 0, len(sqsClient.Input))
	})

	t.Run("Dispatch mergable chunk", func(tt *testing.T) {
		now := time.Now().UTC()
		chunkRepo := mock.NewChunkMockDB()
		chunkRepo.PutChunk(models.NewS3Object("r1", "b1", "k1"), 123, "index", "dt=2020-03-04", now, now.Add(service.DefaultChunkFreezedAfter))
		event := events.DynamoDBEvent{}

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := lambda.HandlerArguments{
			EnvVars: lambda.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			ChunkRepo: chunkRepo,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.NotNil(tt, sqsClient)
		require.Equal(tt, 0, len(sqsClient.Input))
	})
}

func createBaseEvent() *events.DynamoDBEvent {
	return &events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk":         events.NewStringAttribute("chunk/index"),
						"sk":         events.NewStringAttribute("dt=2030-01-02/xxx-yyy-zzz"),
						"schema":     events.NewStringAttribute("index"),
						"s3_objects": events.NewStringSetAttribute([]string{"bucket1@region1:key1", "bucket2@region2:key2"}),
						"total_size": events.NewNumberAttribute("1234567"),
						"created_at": events.NewNumberAttribute("123456789"),
						"freezed_at": events.NewNumberAttribute("123456789"),
						"partition":  events.NewStringAttribute("dt=2030-01-02"),
						"chunk_key":  events.NewStringAttribute("xxx-yyy-zzz"),
					},
				},
			},
		},
	}
}
