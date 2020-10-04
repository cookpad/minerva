package main

import (
	"github.com/m-mizutani/minerva/internal/util"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/merger"
	"github.com/m-mizutani/minerva/pkg/models"
)

func mergeProc(args *handler.Arguments) error {
	sqsService := args.SQSService()
	timer := util.NewExpRetryTimer(0)
	var q models.MergeQueue
	var receipt *string

	err := timer.Run(func(i int) (bool, error) {
		res, err := sqsService.ReceiveMessage(args.MergeQueueURL, 300, &q)
		if err != nil {
			return true, err
		}
		if res != nil {
			receipt = res
			return true, nil
		}

		logger.WithField("count", i).Debug("Retry sqsService.ReceiveMessage")
		return false, nil
	})
	if err != nil {
		return err
	}

	if err := merger.MergeChunk(*args, &q, nil); err != nil {
		return err
	}

	if err := sqsService.DeleteMessage(args.MergeQueueURL, *receipt); err != nil {
		return err
	}

	return nil
}
