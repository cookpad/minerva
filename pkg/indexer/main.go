package indexer

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

func handleEvent(ctx context.Context, sqsEvent events.SQSEvent, reader *rlogs.Reader) error {
	logger.WithField("event", sqsEvent).Info("Star handleEvent")

	for _, sqsRecord := range sqsEvent.Records {
		var snsEntity events.SNSEntity
		if err := json.Unmarshal([]byte(sqsRecord.Body), &snsEntity); err != nil {
			return errors.Wrapf(err, "Fail to unmarshal SNS event in SQS message: %s", sqsRecord.Body)
		}
		logger.WithField("snsEntity", snsEntity).Debug("Received SNS Event")

		var s3Event events.S3Event
		if err := json.Unmarshal([]byte(snsEntity.Message), &s3Event); err != nil {
			return errors.Wrapf(err, "Fail to unmarshal S3 event in SNS message: %s", snsEntity.Message)
		}
		logger.WithField("s3Event", s3Event).Debug("Received S3 Event")

		for _, s3record := range s3Event.Records {
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
			if s3record.S3.Object.Key == "" || strings.HasSuffix(s3record.S3.Object.Key, "/") {
				logger.WithField("s3", s3record).Warn("No key of S3 object OR invalid object key")
				continue
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

// RunIndexer is main handler of indexer. It requires log reader based on rlogs
func RunIndexer(ctx context.Context, sqsEvent events.SQSEvent, reader *rlogs.Reader) error {
	defer internal.FlushError()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))
	logger.WithField("event", sqsEvent).Debug("Start handler")

	if err := handleEvent(ctx, sqsEvent, reader); err != nil {
		internal.HandleError(err)
		return err
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
