package main

import (
	"time"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/merger"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/urfave/cli/v2"
)

func loopCommand(hdlrArgs *handler.Arguments) *cli.Command {
	return &cli.Command{
		Name:  "loop",
		Usage: "Loop to receive SQS message",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "merge-queue-url",
				Aliases:     []string{"q"},
				EnvVars:     []string{"MERGE_QUEUE_URL"},
				Destination: &hdlrArgs.MergeQueueURL,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {
			configure(hdlrArgs)
			loopHandler(hdlrArgs)
			return nil
		},
	}
}

func loopHandler(args *handler.Arguments) {
	for {
		if err := mergeProc(args); err != nil {
			logger.WithField("args", args).WithError(err).Error("Failed to merge")
			internal.HandleError(err)
			internal.FlushError()
			time.Sleep(time.Second * 10) // Pause 10 seconds to prevent spin loop
		}
	}
}

func mergeProc(args *handler.Arguments) error {
	sqsService := args.SQSService()
	timer := retryTimer{}
	var q models.MergeQueue
	var err error
	var receipt *string

	for receipt == nil {
		receipt, err = sqsService.ReceiveMessage(args.MergeQueueURL, 300, &q)
		if err != nil {
			return err
		}
		if receipt != nil {
			break
		}

		timer.sleep()
		logger.Debug("Retry sqsService.ReceiveMessage")
	}

	if err := merger.MergeChunk(*args, &q, nil); err != nil {
		return err
	}

	if err := sqsService.DeleteMessage(args.MergeQueueURL, *receipt); err != nil {
		return err
	}

	return nil
}
