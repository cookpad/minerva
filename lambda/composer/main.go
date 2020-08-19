package main

import (
	"time"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/lambda"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

var logger = lambda.Logger

func main() {
	lambda.StartHandler(handler)
}

func handler(args lambda.HandlerArguments) error {
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
	chunkRepo := args.ChunkTable()
	now := time.Now().UTC()

	chunks, err := chunkRepo.GetWritableChunks(q.Schema, q.Partition, now, q.Size)
	if err != nil {
		return errors.Wrap(err, "Failed GetChunks")
	}

	for _, chunk := range chunks {
		err := chunkRepo.UpdateChunk(chunk, q.S3Object, q.Size, now)
		if err != nil {
			if err == internal.ErrUpdateChunk {
				continue
			}

			return errors.Wrap(err, "Failed UpdateChunk")
		}

		return nil
	}

	// No writable chunk OR all chunk are not writable already.
	if err := chunkRepo.PutChunk(q.S3Object, q.Size, q.Schema, q.Partition, now); err != nil {
		return errors.Wrap(err, "Failed PutChunk")
	}

	return nil
}
