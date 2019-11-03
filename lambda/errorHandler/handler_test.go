package main_test

import (
	"encoding/json"
	"testing"

	"github.com/m-mizutani/minerva/internal"
	main "github.com/m-mizutani/minerva/lambda/errorHandler"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeSQSEvent(t *testing.T) events.SQSEvent {
	s3Event := events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{
						Name: "blue",
					},
					Object: events.S3Object{
						Key: "five",
					},
				},
				EventSource: "aws:s3",
			},
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{
						Name: "orange",
					},
					Object: events.S3Object{
						Key: "six",
					},
				},
				EventSource: "aws:s3",
			},
		},
	}

	s3EventData, err := json.Marshal(s3Event)
	require.NoError(t, err)

	snsEvent := events.SNSEvent{
		Records: []events.SNSEventRecord{
			{
				SNS: events.SNSEntity{
					Message: string(s3EventData),
				},
				EventSource: "aws:sns",
			},
		},
	}
	snsEventData, err := json.Marshal(snsEvent)
	require.NoError(t, err)

	sqsEvent := events.SQSEvent{
		Records: []events.SQSMessage{
			{
				Body:           string(snsEventData),
				EventSource:    "aws:sqs",
				EventSourceARN: "indexerQueueARN",
			},
		},
	}

	return sqsEvent
}

type dummySqsClient struct {
	count   int
	message string
	url     string
}

func (x *dummySqsClient) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	x.count++
	x.message = aws.StringValue(input.MessageBody)
	x.url = aws.StringValue(input.QueueUrl)
	return &sqs.SendMessageOutput{}, nil
}

func TestHandler(t *testing.T) {
	dummy := dummySqsClient{}
	internal.TestInjectNewSqsClient(&dummy)
	defer internal.TestFixNewSqsClient()

	args := main.NewArgument()
	args.SQSEvent = makeSQSEvent(t)
	args.RetryQueueURL = "retryQueueURL"
	args.Region = "my-region"
	args.IndexerDLQ = "indexerQueueARN"

	err := main.Handler(args)
	require.NoError(t, err)

	assert.Equal(t, 1, dummy.count)
	assert.Equal(t, "retryQueueURL", dummy.url)
	var s3event events.S3Event
	err = json.Unmarshal([]byte(dummy.message), &s3event)
	require.NoError(t, err)

	assert.Equal(t, 2, len(s3event.Records))
	assert.Equal(t, "blue", s3event.Records[0].S3.Bucket.Name)
	assert.Equal(t, "five", s3event.Records[0].S3.Object.Key)
	assert.Equal(t, "orange", s3event.Records[1].S3.Bucket.Name)
	assert.Equal(t, "six", s3event.Records[1].S3.Object.Key)
}

func TestHandlerNotFromIndexer(t *testing.T) {
	dummy := dummySqsClient{}
	internal.TestInjectNewSqsClient(&dummy)
	defer internal.TestFixNewSqsClient()

	args := main.NewArgument()
	args.SQSEvent = makeSQSEvent(t)
	args.RetryQueueURL = "retryQueueURL"
	args.Region = "my-region"
	args.IndexerDLQ = "indexerQueueARN"

	args.SQSEvent.Records[0].EventSourceARN = "generalDLQ"

	err := main.Handler(args)
	require.NoError(t, err)
	assert.Equal(t, 0, dummy.count)
}
