package indexer

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

var logger = handler.Logger

// RunIndexer is main handler of indexer. It requires log reader based on rlogs.
// Main procedures are in handleEvent() to reduce number of internal.HandleError().
func RunIndexer(ctx context.Context, sqsEvent events.SQSEvent, reader *rlogs.Reader) error {
	defer internal.FlushError()

	args := Arguments{
		Event:      sqsEvent,
		Reader:     reader,
		NewS3:      adaptor.NewS3Client,
		NewSQS:     adaptor.NewSQSClient,
		NewEncoder: adaptor.NewMsgpackEncoder,
	}
	if err := handleEvent(args); err != nil {
		internal.HandleError(err)
		return err
	}

	return nil
}

type Arguments struct {
	handler.EnvVars
	Event  events.SQSEvent
	Reader *rlogs.Reader

	NewS3      adaptor.S3ClientFactory  `json:"-"`
	NewSQS     adaptor.SQSClientFactory `json:"-"`
	NewEncoder adaptor.EncoderFactory   `json:"-"`
	NewDecoder adaptor.DecoderFactory   `json:"-"`
}

func handleEvent(args Arguments) error {
	if err := args.BindEnvVars(); err != nil {
		return err
	}

	internal.SetupLogger(args.LogLevel)
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
			if err := MakeIndex(args, s3record); err != nil {
				return errors.Wrap(err, "Fail to create inverted index")
			}
		}
	}

	return nil
}

// MakeIndex is a process for one S3 object to make index file.
func MakeIndex(args Arguments, record events.S3EventRecord) error {
	srcObject := models.NewS3ObjectFromRecord(record)
	sqsService := service.NewSQSService(args.NewSQS)

	meta := repository.NewMetaDynamoDB(args.AwsRegion, args.MetaTableName)
	objectID, err := meta.GetObjecID(srcObject.Bucket, srcObject.Key)
	if err != nil {
		return errors.Wrap(err, "Failed GetObjectID")
	}

	dstBase := models.NewS3Object(args.S3Region, args.S3Bucket, args.S3Prefix)
	recordService := service.NewRecordService(args.NewS3, args.NewEncoder, args.NewDecoder)
	for q := range makeLogChannel(srcObject, args.Reader) {
		if q.Err != nil {
			return q.Err
		}

		if err := recordService.Dump(q, objectID, &dstBase); err != nil {
			return err
		}
	}

	if err := recordService.Close(); err != nil {
		return err
	}

	for _, obj := range recordService.RawObjects() {
		partQueue := models.PartitionQueue{
			Location:  obj.PartitionPath(),
			TableName: obj.TableName(),
			Keys:      obj.PartitionKeys(),
		}
		logger.WithField("q", partQueue).Info("Partition queue")
		if err := sqsService.SendSQS(&partQueue, args.PartitionQueueURL); err != nil {
			return errors.Wrap(err, "Fail to send parition queue")
		}

		composeQueue := models.ComposeQueue{
			S3Object:  *obj.Object(),
			Size:      obj.DataSize,
			Schema:    obj.Schema(),
			Partition: obj.Partition(),
		}
		logger.WithField("q", composeQueue).Info("Compose queue")
		if err := sqsService.SendSQS(&composeQueue, args.ComposeQueueURL); err != nil {
			return errors.Wrap(err, "Fail to send parition queue")
		}
	}

	return nil
}
