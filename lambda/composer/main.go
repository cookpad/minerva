package main

import (
	"time"

	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/lambda"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

var logger = lambda.Logger

func main() {
	lambda.StartHandler(Handler)
}

// Handler is exported for testing
func Handler(args lambda.HandlerArguments) error {
	records, err := args.DecapSQSEvent()
	if err != nil {
		return err
	}

	for _, record := range records {
		var q models.ComposeQueue
		if err := record.Bind(&q); err != nil {
			return err
		}

		logger.WithField("queue", q).Info("Run composer")

		if err := composeChunk(args, &q); err != nil {
			return errors.Wrap(err, "Failed composeChunka")
		}
	}

	return nil
}

func composeChunk(args lambda.HandlerArguments, q *models.ComposeQueue) error {
	chunkService := args.ChunkService()
	now := time.Now().UTC()

	chunks, err := chunkService.GetWritableChunks(q.Schema, q.Partition, q.Size)
	if err != nil {
		return errors.Wrap(err, "Failed GetChunks")
	}

	for _, chunk := range chunks {
		err := chunkService.UpdateChunk(chunk, q.S3Object, q.Size)
		if err != nil {
			if err == repository.ErrChunkNotWritable {
				continue
			}

			return errors.Wrap(err, "Failed UpdateChunk")
		}

		return nil
	}

	// No writable chunk OR all chunk are not writable already.
	if err := chunkService.PutChunk(q.S3Object, q.Size, q.Schema, q.Partition, now); err != nil {
		return errors.Wrap(err, "Failed PutChunk")
	}

	return nil
}
