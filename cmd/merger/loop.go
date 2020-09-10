package main

import (
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
			if err := loopHandler(hdlrArgs); err != nil {
				return err
			}
			return nil
		},
	}
}

func loopHandler(hdlrArgs *handler.Arguments) error {
	sqsService := hdlrArgs.SQSService()
	timer := retryTimer{}

	for {
		var q models.MergeQueue
		receipt, err := sqsService.ReceiveMessage(hdlrArgs.MergeQueueURL, 300, &q)
		if err != nil {
			return err
		}

		if receipt == nil {
			timer.sleep()
			continue
		}
		timer.clear()

		if err := merger.MergeChunk(*hdlrArgs, &q, nil); err != nil {
			return err
		}

		if err := sqsService.DeleteMessage(hdlrArgs.MergeQueueURL, *receipt); err != nil {
			return err
		}
	}
}
