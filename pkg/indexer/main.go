package indexer

import (
	"context"
	"encoding/json"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

// RunIndexer is main handler of indexer. It requires log reader based on rlogs
func RunIndexer(ctx context.Context, event events.SNSEvent, reader *rlogs.Reader) error {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))

	logger.WithField("event", event).Debug("Start handler")

	for _, record := range event.Records {
		var s3event events.S3Event
		if err := json.Unmarshal([]byte(record.SNS.Message), &s3event); err != nil {
			return errors.Wrapf(err, "Fail to unmarshal S3 event in SNS message: %s", record.SNS.Message)
		}

		for _, s3record := range s3event.Records {
			args := arguments{
				IndexTable:   os.Getenv("INDEX_TABLE_NAME"),
				MessageTable: os.Getenv("MESSAGE_TABLE_NAME"),

				MetaTable:      os.Getenv("META_TABLE_NAME"),
				ObjectQueue:    os.Getenv("OBJECT_QUEUE"),
				PartitionQueue: os.Getenv("PARTITION_QUEUE"),
				BaseRegion:     os.Getenv("AWS_REGION"),

				Src: s3Loc{
					Region: s3record.AWSRegion,
					Bucket: s3record.S3.Bucket.Name,
				},
				Dst: s3Loc{
					Region: os.Getenv("S3_REGION"),
					Bucket: os.Getenv("S3_BUCKET"),
					Prefix: os.Getenv("S3_PREFIX"),
				},
				Reader: reader,
			}
			args.Src.SetKey(s3record.S3.Object.Key)

			logger.WithField("args", args).Info("Start indexer")
			if err := makeIndex(args); err != nil {
				return errors.Wrap(err, "Fail to create inverted index")
			}
		}
	}

	return nil
}

/*
func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))
	lambda.Start(handleRequest)
}
*/
