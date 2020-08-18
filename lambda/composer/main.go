package main

import (
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

	return nil
}
