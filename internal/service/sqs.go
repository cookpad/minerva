package service

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/pkg/errors"
)

// SQSService is accessor to SQS
type SQSService struct {
	newSQS adaptor.SQSClientFactory
}

// NewSQSService is constructor of
func NewSQSService(newSQS adaptor.SQSClientFactory) *SQSService {
	return &SQSService{
		newSQS: newSQS,
	}
}

// SendSQS is wrapper of sqs:SendMessage of AWS
func (x *SQSService) SendSQS(msg interface{}, url string) error {
	// QueueURL sample: https://sqs.eu-west-2.amazonaws.com/
	urlParts := strings.Split(url, "/")
	if len(urlParts) < 3 {
		logger.WithField("url", url).Error("Failed to parse URL (not enough slash)")
		return errors.New("Invalid SQS Queue URL")
	}
	domainParts := strings.Split(urlParts[2], ".")
	if len(domainParts) != 4 {
		logger.WithField("url", url).Error("Failed to parse URL (not enough dot in FQDN")
		return errors.New("Invalid SQS Queue URL")
	}

	client := x.newSQS(domainParts[1])

	raw, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrapf(err, "Fail to marshal message: %v", msg)
	}

	input := sqs.SendMessageInput{
		QueueUrl:    aws.String(url),
		MessageBody: aws.String(string(raw)),
	}
	resp, err := client.SendMessage(&input)

	if err != nil {
		return errors.Wrapf(err, "Fail to send SQS message: %v", input)
	}

	logger.WithField("resp", resp).Trace("Sent SQS message")

	return nil
}
