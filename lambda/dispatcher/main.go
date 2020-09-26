package main

import (
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

var logger = handler.Logger

func main() {
	handler.StartLambda(Handler)
}

// Handler is main procedure of dispatcher
func Handler(args handler.Arguments) error {
	var event events.DynamoDBEvent
	if err := args.BindEvent(&event); err != nil {
		return err
	}

	now := time.Now().UTC()
	chunkService := args.ChunkService()
	sqsService := args.SQSService()

	var chunks []*models.Chunk
	if len(event.Records) > 0 {
		for _, record := range event.Records {
			if record.EventName != "MODIFY" && record.EventName != "INSERT" {
				continue
			}
			if record.Change.NewImage == nil {
				continue
			}

			chunk, err := models.NewChunkFromDynamoEvent(record.Change.NewImage)
			if err != nil {
				logger.WithField("record", record).Error("NewChunkFromDynamoEvent")
				return errors.Wrap(err, "Failed to parse record.Change.NewImage")
			}

			if chunkService.IsMergableChunk(chunk, now) {
				chunks = append(chunks, chunk)
			}
		}
	} else {
		idxChunks, err := chunkService.GetMergableChunks("index", now)
		if err != nil {
			return errors.Wrap(err, "Failed GetMergableChunks")
		}
		msgChunks, err := chunkService.GetMergableChunks("message", now)
		if err != nil {
			return errors.Wrap(err, "Failed GetMergableChunks")
		}
		chunks = append(chunks, idxChunks...)
		chunks = append(chunks, msgChunks...)
	}

	for _, old := range chunks {
		chunk, err := chunkService.FreezeChunk(old)
		if chunk == nil {
			continue // The chunk is no longer avaiable
		}
		if err != nil {
			return errors.Wrap(err, "chunkService.FreezeChunk")
		}

		logger.WithField("chunk", chunk).Info("composing chunk")

		src, err := chunk.ToS3ObjectSlice()
		if err != nil {
			return errors.Wrap(err, "Failed ToS3ObjectSlice")
		}

		s3Key := models.BuildMergedS3ObjectKey(args.S3Prefix, chunk.Schema, chunk.Partition, chunk.ChunkKey)
		dst := models.NewS3Object(args.S3Region, args.S3Bucket, s3Key)

		srcObjects, err := models.NewS3Objects(src)

		if err != nil {
			return errors.Wrap(err, "Failed EncodeS3Objects")
		}

		q := models.MergeQueue{
			Schema:     models.ParquetSchemaName(chunk.Schema),
			TotalSize:  chunk.TotalSize,
			SrcObjects: srcObjects,
			DstObject:  dst,
		}

		if err := sqsService.SendSQS(q, args.MergeQueueURL); err != nil {
			logger.WithField("queue", q).Error("internal.SendSQS")
			return errors.Wrap(err, "Failed SendSQS")
		}

		if _, err := chunkService.DeleteChunk(chunk); err != nil {
			logger.WithField("chunk", chunk).WithError(err).Error("DeleteChunk")
			return errors.Wrap(err, "Failed chunkService.DeleteChunk")
		}
	}

	return nil
}
