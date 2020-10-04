package main

import (
	"time"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/handler"
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
