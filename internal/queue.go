package internal

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
)

// PartitionQueue is arguments of partitioner to add a new partition
type PartitionQueue struct {
	Location  string            `json:"location"`
	TableName string            `json:"table_name"`
	Keys      map[string]string `json:"keys"`
}

// ObjectQueue is argument to create object table (map of objectID and S3 path)
type ObjectQueue struct {
	ID       int64  `json:"id"`
	S3Bucket string `json:"s3_bucket"`
	S3Key    string `json:"s3_key"`
	Date     string `json:"date"`
}

// S3Location has basic location information of S3 Object.
type S3Location struct {
	Region string `json:"region"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Size   int64  `json:"size,omitempty"`
}

// RecvObjectQueue has received ObjectQueue from SQS and error if something wrong
type RecvObjectQueue struct {
	Obj ObjectQueue
	Err error
}

type sqsClient interface {
	SendMessage(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error)
}

var newSqsClient = newAwsSqsClient

func newAwsSqsClient(region string) sqsClient {
	ssn := session.New(&aws.Config{Region: aws.String(region)})
	client := sqs.New(ssn)
	return client
}

// SendSQS is wrapper of sqs:SendMessage of AWS
func SendSQS(msg interface{}, region, target string) error {
	client := newSqsClient(region)

	raw, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrapf(err, "Fail to marshal message: %v", msg)
	}

	input := sqs.SendMessageInput{
		QueueUrl:    &target,
		MessageBody: aws.String(string(raw)),
	}
	resp, err := client.SendMessage(&input)

	if err != nil {
		return errors.Wrapf(err, "Fail to send SQS message: %v", input)
	}

	Logger.WithField("resp", resp).Trace("Sent SQS message")

	return nil
}
