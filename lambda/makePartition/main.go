package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

func handleRequest(ctx context.Context, event events.SQSEvent) error {
	logger.WithField("event", event).Debug("Start handler")

	for _, msg := range event.Records {
		var q internal.PartitionQueue
		if err := json.Unmarshal([]byte(msg.Body), &q); err != nil {
			return errors.Wrapf(err, "Fail to unmarshal PartitionQueue: %s", msg.Body)
		}

		args := arguments{
			Region:         os.Getenv("AWS_REGION"),
			MetaTableName:  os.Getenv("META_TABLE_NAME"),
			AthenaDBName:   os.Getenv("ATHENA_DB_NAME"),
			OutputLocation: fmt.Sprintf("s3://%s/%soutput", os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX")),
			Queue:          q,
		}

		logger.WithField("args", args).Info("Start partitioning")
		if err := makePartition(args); err != nil {
			return errors.Wrapf(err, "Fail to make partition: %v", args)
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