package main_test

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dispatcher "github.com/m-mizutani/minerva/lambda/dispatcher"
)

func TestDispatcher(t *testing.T) {
	now := time.Now().UTC()

	t.Run("Dispatch insert event", func(tt *testing.T) {
		repo := mock.NewChunkMockDB()
		event := createBaseEvent(repo, service.DefaultChunkChunkMinSize, now)

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := handler.Arguments{
			EnvVars: handler.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: repo,
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.Equal(tt, 1, len(sqsClient.Input))
		assert.Equal(tt, "https://sqs.eu-west-2.amazonaws.com/test-url", *sqsClient.Input[0].QueueUrl)

		var q models.MergeQueue
		require.NoError(tt, json.Unmarshal([]byte(*sqsClient.Input[0].MessageBody), &q))
		assert.Equal(tt, models.ParquetSchemaName("index"), q.Schema)
		srcObjects, err := q.SrcObjects.Export()
		require.NoError(tt, err)
		assert.Equal(tt, 2, len(srcObjects))
		assert.Contains(tt, srcObjects, &models.S3Object{
			Region: "region1",
			Bucket: "bucket1",
			Key:    "key1",
		})
		assert.Contains(tt, srcObjects, &models.S3Object{
			Region: "region2",
			Bucket: "bucket2",
			Key:    "key2",
		})
		assert.NotContains(tt, srcObjects, &models.S3Object{
			Region: "region3",
			Bucket: "bucket3",
			Key:    "key3",
		})
	})

	t.Run("Dispatch update event", func(tt *testing.T) {
		repo := mock.NewChunkMockDB()
		event := createBaseEvent(repo, service.DefaultChunkChunkMinSize, now)
		event.Records[0].EventName = "MODIFY"

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := handler.Arguments{
			EnvVars: handler.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: repo,
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.Equal(tt, 1, len(sqsClient.Input))
		assert.Equal(tt, "https://sqs.eu-west-2.amazonaws.com/test-url", *sqsClient.Input[0].QueueUrl)

		var q models.MergeQueue
		require.NoError(tt, json.Unmarshal([]byte(*sqsClient.Input[0].MessageBody), &q))
		assert.Equal(tt, models.ParquetSchemaName("index"), q.Schema)

		srcObjects, err := q.SrcObjects.Export()
		require.NoError(tt, err)
		assert.Equal(tt, 2, len(srcObjects))

		assert.Contains(tt, srcObjects, &models.S3Object{
			Region: "region1",
			Bucket: "bucket1",
			Key:    "key1",
		})
		assert.Contains(tt, srcObjects, &models.S3Object{
			Region: "region2",
			Bucket: "bucket2",
			Key:    "key2",
		})
	})

	t.Run("Ignore remove event", func(tt *testing.T) {
		repo := mock.NewChunkMockDB()
		event := createBaseEvent(repo, service.DefaultChunkChunkMinSize, now)
		event.Records[0].EventName = "REMOVE"

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := handler.Arguments{
			EnvVars: handler.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: repo,
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.Equal(tt, 0, len(sqsClient.Input))
	})

	t.Run("Ignore not exceeded totalSize", func(tt *testing.T) {
		repo := mock.NewChunkMockDB()
		event := createBaseEvent(repo, service.DefaultChunkChunkMinSize-1, now)

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := handler.Arguments{
			EnvVars: handler.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
			ChunkRepo: repo,
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.Equal(tt, 0, len(sqsClient.Input))
	})

	t.Run("Dispatch mergable chunk", func(tt *testing.T) {
		now := time.Now().UTC()
		chunkRepo := mock.NewChunkMockDB()
		chunkRepo.PutChunk(models.NewS3Object("r1", "b1", "k1"), 123, "index", "dt=2020-03-04", now)
		event := events.DynamoDBEvent{}

		sqsClient := mock.NewSQSClient("dummy").(*mock.SQSClient)
		args := handler.Arguments{
			EnvVars: handler.EnvVars{
				MergeQueueURL: "https://sqs.eu-west-2.amazonaws.com/test-url",
			},
			Event:     event,
			ChunkRepo: chunkRepo,
			NewSQS:    func(region string) adaptor.SQSClient { return sqsClient },
		}

		require.NoError(tt, dispatcher.Handler(args))
		require.Equal(tt, 0, len(sqsClient.Input))
	})
}

func createBaseEvent(repo repository.ChunkRepository, size int64, now time.Time) *events.DynamoDBEvent {
	ptn := "dt=2030-01-02"

	if err := repo.PutChunk(models.NewS3Object("region1", "bucket1", "key1"), size-1, "index", ptn, now); err != nil {
		log.Fatal("Failed to put chunk", err)
	}

	chunks, err := repo.GetWritableChunks("index", ptn, service.DefaultChunkChunkMinSize*10)
	if err != nil {
		log.Fatal("Failed to get chunk", err)
	}

	if err := repo.UpdateChunk(chunks[0], models.NewS3Object("region2", "bucket2", "key2"), 1, service.DefaultChunkChunkMinSize*10); err != nil {
		log.Fatal("Failed to update chunk", err)
	}

	return &events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk":         events.NewStringAttribute(chunks[0].PK),
						"sk":         events.NewStringAttribute(chunks[0].SK),
						"schema":     events.NewStringAttribute("index"),
						"s3_objects": events.NewStringSetAttribute([]string{"bucket1@region1:key1", "bucket2@region2:key2"}),
						"total_size": events.NewNumberAttribute(fmt.Sprintf("%d", size)),
						"created_at": events.NewNumberAttribute(fmt.Sprintf("%d", chunks[0].CreatedAt)),
						"partition":  events.NewStringAttribute(ptn),
						"chunk_key":  events.NewStringAttribute(chunks[0].ChunkKey),
						"freezed":    events.NewBooleanAttribute(false),
					},
				},
			},
		},
	}
}
