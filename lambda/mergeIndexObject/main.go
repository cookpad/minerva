package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

func handleRequest(ctx context.Context, event events.SQSEvent) error {
	defer internal.FlushError()
	logger.WithField("event.records.len", len(event.Records)).Debug("Start handler")

	for _, record := range event.Records {
		var queue internal.MergeQueue
		if err := json.Unmarshal([]byte(record.Body), &queue); err != nil {
			err = errors.Wrapf(err, "Fail to unmarshal SQS message body: %s", record.Body)
			internal.HandleError(err)
			return err
		}

		args := arguments{
			Queue: queue,
		}

		logger.WithField("args", args).Info("Start indexer")
		if err := mergeParquet(args); err != nil {
			err = errors.Wrap(err, "Fail to merge parquet files")
			internal.HandleError(err)
			return err
		}
	}

	return nil
}

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))
	lambda.Start(handleRequest)
}
