package main

import (
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/merger"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/urfave/cli/v2"
)

func oneshotCommand(hdlrArgs *handler.Arguments) *cli.Command {
	return &cli.Command{
		Name:  "oneshot",
		Usage: "Oneshot run to receive SQS message",
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
			if err := oneshotHandler(hdlrArgs); err != nil {
				return err
			}
			return nil
		},
	}
}

func oneshotHandler(hdlrArgs *handler.Arguments) error {
	sqsService := hdlrArgs.SQSService()
	timer := retryTimer{}
	var receipt *string
	var err error
	var q models.MergeQueue

	for receipt == nil {
		receipt, err = sqsService.ReceiveMessage(hdlrArgs.MergeQueueURL, 300, &q)
		if err != nil {
			return err
		}

		timer.sleep()
	}

	if err := merger.MergeChunk(*hdlrArgs, &q, nil); err != nil {
		return err
	}

	if err := sqsService.DeleteMessage(hdlrArgs.MergeQueueURL, *receipt); err != nil {
		return err
	}

	return nil
}
