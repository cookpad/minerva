package indexer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

var logger = handler.Logger

// RunIndexer is main handler of indexer. It requires log reader based on rlogs.
// Main procedures are in handleEvent() to reduce number of internal.HandleError().
func RunIndexer(reader *rlogs.Reader) {
	handler.StartLambda(handleEvent, reader)
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

func handleEvent(args handler.Arguments) error {
	records, err := args.DecapSQSEvent()
	if err != nil {
		return err
	}

	for _, record := range records {
		var snsEntity events.SNSEntity
		if err := record.Bind(&snsEntity); err != nil {
			return err
		}

		var s3Event events.S3Event
		if err := json.Unmarshal([]byte(snsEntity.Message), &s3Event); err != nil {
			return errors.Wrapf(err, "Fail to unmarshal S3 event in SNS message: %s", snsEntity.Message)
		}
		logger.WithField("s3Event", s3Event).Info("Received S3 Event")

		if err := handleS3Event(args, s3Event); err != nil {
			return err
		}
	}

	return nil
}

func handleS3Event(args handler.Arguments, s3Event events.S3Event) error {
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
	return nil
}

// MakeIndex is a process for one S3 object to make index file.
func MakeIndex(args handler.Arguments, record events.S3EventRecord) error {
	logger.WithField("record", record).Info("Start MakeIndex")

	if err := validateArguments(args); err != nil {
		return errors.Wrap(err, "Invalid indexer arguments")
	}

	srcObject := models.NewS3ObjectFromRecord(record)
	sqsService := args.SQSService()

	meta := args.MetaService()
	objectID, err := meta.GetObjectID(srcObject.Bucket, srcObject.Key)
	if err != nil {
		return errors.Wrap(err, "Failed GetObjectID")
	}

	dstBase := models.NewS3Object(args.S3Region, args.S3Bucket, args.S3Prefix)
	recordService := args.RecordService()

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

	rawObjects := recordService.RawObjects()
	var records []*repository.MetaRecordObject
	for seq, obj := range rawObjects {
		recordID := fmt.Sprintf("%d/%d", objectID, seq)

		records = append(records, &repository.MetaRecordObject{
			RecordID: recordID,
			S3Object: *obj.Object(),
			Schema:   models.ParquetSchemaName(obj.Schema()),
		})
	}
	if err := meta.PutObjects(records); err != nil {
		return errors.Wrap(err, "Failed to put record objects")
	}
	logger.WithField("records", records).Debug("Done PutRecordObject")

	for seq, obj := range rawObjects {
		recordID := fmt.Sprintf("%d/%d", objectID, seq)

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
			RecordID:  recordID,
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

func validateArguments(args handler.Arguments) error {
	if args.S3Region == "" {
		return errors.New("S3_REGION is not set")
	}
	if args.S3Bucket == "" {
		return errors.New("S3_BUCKET is not set")
	}
	if args.S3Prefix == "" {
		return errors.New("S3_PREFIX is not set")
	}
	if args.MetaTableName == "" {
		return errors.New("META_TABLE_NAME is not set")
	}
	if args.PartitionQueueURL == "" {
		return errors.New("PARTITION_QUEUE_URL is not set")
	}
	if args.ComposeQueueURL == "" {
		return errors.New("COMPOSE_QUEUE_URL is not set")
	}
	if args.AwsRegion == "" {
		return errors.New("AWS_REGION is not set")
	}

	return nil
}
