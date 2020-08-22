package adaptor

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SQSClientFactory is interface SQSClient constructor
type SQSClientFactory func(region string) SQSClient

// SQSClient is interface of AWS SDK SQS
type SQSClient interface {
	SendMessage(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error)
}

// NewSQSClient creates actual AWS SQS SDK client
func NewSQSClient(region string) SQSClient {
	ssn := session.New(&aws.Config{Region: aws.String(region)})
	return sqs.New(ssn)
}
