package main

import (
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/pkg/lambda"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

var logger = lambda.Logger

func main() {
	lambda.StartHandler(handler)
}

func handler(args lambda.HandlerArguments) error {
	var event events.DynamoDBEvent
	if err := args.BindEvent(&event); err != nil {
		return err
	}

	now := time.Now().UTC()
	chunkService := args.ChunkService()

	var chunks []*models.Chunk
	if len(event.Records) > 0 {
		for _, record := range event.Records {
			chunk, err := models.NewChunkFromDynamoEvent(record.Change.NewImage)
			if err != nil {
				logger.WithField("record", record).Error("NewChunkFromDynamoEvent")
				return errors.Wrap(err, "Failed to parse record.Change.NewImage")
			}

			chunks = append(chunks, chunk)
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

	logger.WithField("chunks", chunks).Info("waiwai")

	for _, chunk := range chunks {
		if _, err := chunkService.DeleteChunk(chunk); err != nil {
			logger.WithField("chunk", chunk).WithError(err).Error("DeleteChunk")
			return errors.Wrap(err, "Failed chunkService.DeleteChunk")
		}
	}

	return nil
}
