package main

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

type listParquetEvent struct {
	BaseTime *time.Time `json:"base_time"`
}

func handleRequest(ctx context.Context, event listParquetEvent) error {
	logger.WithField("event", event).Debug("Start handler")

	args := arguments{
		BaseRegion:    os.Getenv("AWS_REGION"),
		S3Region:      os.Getenv("S3_REGION"),
		S3Bucket:      os.Getenv("S3_BUCKET"),
		S3Prefix:      os.Getenv("S3_PREFIX"),
		MergeQueueURL: os.Getenv("MERGE_QUEUE"),
	}

	if event.BaseTime != nil {
		args.BaseTime = *event.BaseTime
	} else {
		args.BaseTime = time.Now().UTC().Add(-time.Hour)
	}

	logger.WithField("args", args).Info("Start indexer")
	if err := listParquet(args); err != nil {
		return errors.Wrap(err, "Fail to list parquet files")
	}

	return nil
}

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))
	lambda.Start(handleRequest)
}
