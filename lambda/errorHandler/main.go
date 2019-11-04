package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))

	lambda.Start(func(ctx context.Context, event events.SQSEvent) error {
		logger.WithField("sqs", event).Info("Start ErrorHandler")

		args := arguments{
			SQSEvent:      event,
			RetryQueueURL: os.Getenv("RETRY_QUEUE"),
			Region:        os.Getenv("AWS_REGION"),
			IndexerDLQ:    os.Getenv("INDEXER_DLQ"),
		}

		return handler(args)
	})
}
