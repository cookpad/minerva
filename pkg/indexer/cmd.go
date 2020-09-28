package indexer

import (
	"encoding/json"
	"math"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

func configure(args *handler.Arguments) {
	handler.SetLogLevel(args.LogLevel)
}

func RunIndexerProc(reader *rlogs.Reader) {
	args := &handler.Arguments{}
	args.Reader = reader

	app := &cli.App{
		Name:  "indexer",
		Usage: "Minerva Indexer",
		Flags: []cli.Flag{

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

		Commands: []*cli.Command{
			loopCommand(args),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.Fatal(err)
	}
}

func loopCommand(args *handler.Arguments) *cli.Command {
	var url string
	return &cli.Command{
		Name:  "loop",
		Usage: "Loop to receive SQS message",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "index-queue-url",
				Aliases:     []string{"q"},
				EnvVars:     []string{"INDEX_QUEUE_URL"},
				Destination: &url,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {
			configure(args)

			if err := args.BindEnvVars(); err != nil {
				return err
			}

			loopHandler(args, url)
			return nil
		},
	}
}

func loopHandler(args *handler.Arguments, queueURL string) {
	for {
		if err := indexerProc(args, queueURL); err != nil {
			logger.WithField("args", args).WithError(err).Error("Failed to merge")
			internal.HandleError(err)
			internal.FlushError()
			time.Sleep(time.Second * 10) // Pause 10 seconds to prevent spin loop
		}
	}
}

func indexerProc(args *handler.Arguments, queueURL string) error {
	var entity events.SNSEntity
	sqsService := args.SQSService()
	timer := retryTimer{}
	var err error
	var receipt *string

	for {
		receipt, err = sqsService.ReceiveMessage(queueURL, 300, &entity)
		if err != nil {
			return err
		}
		if receipt != nil {
			break
		}

		timer.sleep()
		logger.Debug("Retry sqsService.ReceiveMessage")
	}

	var s3Event events.S3Event
	if err := json.Unmarshal([]byte(entity.Message), &s3Event); err != nil {
		logger.WithField("entity", entity).Error("json.Unmarshal")
		return errors.Wrap(err, "Failed to parse S3 event")
	}

	if err := handleS3Event(*args, s3Event); err != nil {
		return err
	}

	if err := sqsService.DeleteMessage(queueURL, *receipt); err != nil {
		return err
	}

	return nil
}

type retryTimer struct {
	retryCount int
}

func (x *retryTimer) sleep() {
	waitTime := x.calcWaitTime()
	time.Sleep(waitTime)
}

func (x *retryTimer) calcWaitTime() time.Duration {
	wait := math.Pow(2.0, float64(x.retryCount)) / 8
	if wait > 10 {
		wait = 10
	}
	mSec := time.Millisecond * time.Duration(wait*1000)
	x.retryCount++
	return mSec
}

func (x *retryTimer) clear() {
	x.retryCount = 0
}
