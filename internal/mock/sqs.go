package mock

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/m-mizutani/minerva/internal/adaptor"
)

// SQSClientFactory is interface SQSClient constructor
type SQSClientFactory func(region string) adaptor.SQSClient

// SQSClient is mock of AWS SQS SDK
type SQSClient struct {
	Input  []*sqs.SendMessageInput
	Region string
}

// NewSQSClient creates mock SQS client
func NewSQSClient(region string) adaptor.SQSClient {
	return &SQSClient{
		Region: region,
	}
}

// SendMessage of mock just stores SendMessage input
func (x *SQSClient) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	x.Input = append(x.Input, input)
	return nil, nil
}
