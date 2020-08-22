package indexer

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/lambda"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = lambda.Logger

// RunIndexer is main handler of indexer. It requires log reader based on rlogs.
// Main procedures are in handleEvent() to reduce number of internal.HandleError().
func RunIndexer(ctx context.Context, sqsEvent events.SQSEvent, reader *rlogs.Reader) error {
	defer internal.FlushError()

	args := arguments{
		Event:  sqsEvent,
		Reader: reader,
	}
	if err := handleEvent(args); err != nil {
		internal.HandleError(err)
		return err
	}

	return nil
}

type arguments struct {
	lambda.EnvVars
	Event  events.SQSEvent
	Reader *rlogs.Reader

	NewS3 adaptor.S3ClientFactory
}

func handleEvent(args arguments) error {
	if err := args.BindEnvVars(); err != nil {
		return err
	}

	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(args.LogLevel)
	logger.WithField("event", args.Event).Debug("Start handler")

	for _, sqsRecord := range args.Event.Records {
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
			s3Key := s3record.S3.Object.Key
			if s3Key == "" || strings.HasSuffix(s3Key, "/") {
				logger.WithField("s3", s3record).Warn("No key of S3 object OR invalid object key")
				continue
			}

			logger.WithField("args", args).Info("Start indexer")
			if err := makeIndex(args, s3record); err != nil {
				return errors.Wrap(err, "Fail to create inverted index")
			}
		}
	}

	return nil
}

const (
	indexQueueSize = 128
)

// makeIndex is a process for one S3 object to make index file.
func makeIndex(args arguments, record events.S3EventRecord) error {
	srcObject := models.NewS3ObjectFromRecord(record)
	s3Service := service.NewS3Service(args.NewS3)

	ch := makeLogChannel(srcObject, args.Reader)
	meta := repository.NewMetaDynamoDB(args.AwsRegion, args.MetaTableName)

	dumpers, err := dumpParquetFiles(ch, meta)
	logger.WithFields(logrus.Fields{
		"dumpers": dumpers,
	}).Debug("Done dump parquet file(s)")
	if err != nil {
		return errors.Wrap(err, "Fail to dump parquet")
	}

	for _, dumper := range dumpers {
		for _, f := range dumper.Files() {
			f.dst.Prefix = args.S3Prefix
			dstObject := models.NewS3Object(args.S3Region, args.S3Bucket, f.dst.S3Key())

			if err := s3Service.UploadFileToS3(f.filePath, dstObject); err != nil {
				logger.WithField("dst", dstObject).Error("internal.UploadFileToS3")
				return errors.Wrapf(err, "Failed UploadFileToS3")
			}

			if err := os.Remove(f.filePath); err != nil {
				return errors.Wrapf(err, "Fail to remove dump file: %s", f.filePath)
			}

			partQueue := models.PartitionQueue{
				Location:  f.dst.PartitionLocation(),
				TableName: f.dst.TableName(),
				Keys:      f.dst.PartitionKeys(),
			}
			logger.WithField("q", partQueue).Info("Partition queue")
			if err := internal.SendSQS(&partQueue, args.AwsRegion, args.PartitionQueueURL); err != nil {
				return errors.Wrap(err, "Fail to send parition queue")
			}

			composeQueue := models.ComposeQueue{
				S3Object:  dstObject,
				Size:      int64(f.dataSize),
				Schema:    dumper.Type(),
				Partition: f.dst.Partition(),
			}
			logger.WithField("q", composeQueue).Info("Compose queue")
			if err := internal.SendSQS(&composeQueue, args.AwsRegion, args.ComposeQueueURL); err != nil {
				return errors.Wrap(err, "Fail to send parition queue")
			}

		}
	}

	return nil
}
