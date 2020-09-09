package main

import (
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/merger"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

var logger = internal.Logger

func main() {
	handler.StartLambda(mergeHandler)
}

func mergeHandler(args handler.Arguments) error {
	records, err := args.DecapSQSEvent()
	if err != nil {
		return err
	}

	for _, record := range records {
		var q models.MergeQueue
		if err := record.Bind(&q); err != nil {
			return err
		}

		if err := merger.MergeChunk(args, &q); err != nil {
			return errors.Wrap(err, "Failed composeChunk")
		}
	}

	return nil
}
