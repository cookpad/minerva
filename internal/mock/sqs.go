package mock

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal/adaptor"
)

// SQSClientFactory is interface SQSClient constructor
type SQSClientFactory func(region string) adaptor.SQSClient

// SQSClient is mock of AWS SQS SDK
type SQSClient struct {
	Input      []*sqs.SendMessageInput
	queueIndex int
	receipts   map[string]struct{}
	Region     string
}

// NewSQSClient creates mock SQS client
func NewSQSClient(region string) adaptor.SQSClient {
	return &SQSClient{
		Region:   region,
		receipts: make(map[string]struct{}),
	}
}

// SendMessage of mock just stores SendMessage input
func (x *SQSClient) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	x.Input = append(x.Input, input)
	return nil, nil
}

func (x *SQSClient) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	if len(x.Input) <= x.queueIndex {
		return &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{},
		}, nil
	}

	receipt := uuid.New().String()
	x.receipts[receipt] = struct{}{}
	output := sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				Body:          x.Input[x.queueIndex].MessageBody,
				ReceiptHandle: &receipt,
			},
		},
	}
	x.queueIndex++
	return &output, nil
}

func (x *SQSClient) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	if _, ok := x.receipts[*input.ReceiptHandle]; !ok {
		return nil, fmt.Errorf("no such receipt")
	}

	delete(x.receipts, *input.ReceiptHandle)
	return &sqs.DeleteMessageOutput{}, nil
}
