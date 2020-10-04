package main

import (
	"github.com/m-mizutani/minerva/pkg/handler"
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
			configure(hdlrArgs)

			if err := mergeProc(hdlrArgs); err != nil {
				return err
			}
			return nil
		},
	}
}
