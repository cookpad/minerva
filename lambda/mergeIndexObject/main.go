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
	logger.WithField("event.records.len", len(event.Records)).Debug("Start handler")

	for _, record := range event.Records {
		var queue internal.MergeQueue
		if err := json.Unmarshal([]byte(record.Body), &queue); err != nil {
			return errors.Wrapf(err, "Fail to unmarshal SQS message body: %s", record.Body)
		}

		args := arguments{
			Queue: queue,
		}

		logger.WithField("args", args).Info("Start indexer")
		if err := mergeCSV(args); err != nil {
			return errors.Wrap(err, "Fail to merge parquet files")
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
