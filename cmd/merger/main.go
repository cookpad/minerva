package main

import (
	"os"
	"time"

	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/merger"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/urfave/cli/v2"
)

var logger = handler.Logger

func main() {
	args := handler.Arguments{
		NewS3:  adaptor.NewS3Client,
		NewSQS: adaptor.NewSQSClient,
	}

	app := &cli.App{
		Name:  "indexer",
		Usage: "Minerva Indexer",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "merge-queue-url",
				Aliases:     []string{"l"},
				EnvVars:     []string{"MERGE_QUEUE_URL"},
				Destination: &args.LogLevel,
				Required:    true,
			},

			&cli.StringFlag{
				Name:        "sentry-dsn",
				EnvVars:     []string{"SENTRY_DSN"},
				Destination: &args.SentryDSN,
			},
			&cli.StringFlag{
				Name:        "sentry-env",
				EnvVars:     []string{"SENTRY_ENVIRONMENT"},
				Destination: &args.SentryEnv,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Aliases:     []string{"l"},
				EnvVars:     []string{"LOG_LEVEL"},
				Destination: &args.LogLevel,
			},
		},
		Action: func(c *cli.Context) error {
			if err := mergeHandler(args); err != nil {
				return err
			}
			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.Fatal(err)
	}
}

type retryTimer struct {
	retryCount int
}

func (x *retryTimer) sleep() {
	time.Sleep(time.Second * 10)
}

func (x *retryTimer) clear() {
	x.retryCount = 0
}

func mergeHandler(args handler.Arguments) error {
	sqsService := args.SQSService()
	timer := retryTimer{}

	for {
		var q models.MergeQueue
		receipt, err := sqsService.ReceiveMessage(args.MergeQueueURL, 300, &q)
		if err != nil {
			return err
		}

		if receipt == nil {
			timer.sleep()
			continue
		}
		timer.clear()

		if err := merger.MergeChunk(args, q); err != nil {
			return err
		}

		if err := sqsService.DeleteMessage(args.MergeQueueURL, *receipt); err != nil {
			return err
		}
	}
}
