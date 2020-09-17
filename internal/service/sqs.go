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
	newSQS   adaptor.SQSClientFactory
	queueMap map[string]*sqs.ReceiveMessageOutput
	msgIndex int
}

// NewSQSService is constructor of
func NewSQSService(newSQS adaptor.SQSClientFactory) *SQSService {
	return &SQSService{
		queueMap: make(map[string]*sqs.ReceiveMessageOutput),
		newSQS:   newSQS,
	}
}

func extractSQSRegion(url string) (string, error) {
	// QueueURL sample: https://sqs.eu-west-2.amazonaws.com/
	urlParts := strings.Split(url, "/")
	if len(urlParts) < 3 {
		logger.WithField("url", url).Error("Failed to parse URL (not enough slash)")
		return "", errors.New("Invalid SQS Queue URL")
	}
	domainParts := strings.Split(urlParts[2], ".")
	if len(domainParts) != 4 {
		logger.WithField("url", url).Error("Failed to parse URL (not enough dot in FQDN")
		return "", errors.New("Invalid SQS Queue URL")
	}

	return domainParts[1], nil
}

// SendSQS is wrapper of sqs:SendMessage of AWS
func (x *SQSService) SendSQS(msg interface{}, url string) error {
	region, err := extractSQSRegion(url)
	if err != nil {
		return err
	}

	client := x.newSQS(region)

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

// ReceiveMessage is wrapper of sqs:ReceiveMessage
func (x *SQSService) ReceiveMessage(url string, timeout int64, msg interface{}) (*string, error) {
	output, ok := x.queueMap[url]
	if !ok || output == nil {
		region, err := extractSQSRegion(url)
		if err != nil {
			return nil, err
		}

		client := x.newSQS(region)
		out, err := client.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:          aws.String(url),
			VisibilityTimeout: aws.Int64(timeout),
		})
		if err != nil {
			return nil, errors.Wrap(err, "Failed client.ReceiveMessage")
		}

		x.queueMap[url] = out
		x.msgIndex = 0
		output = out
	}

	if len(output.Messages) <= x.msgIndex {
		delete(x.queueMap, url)
		return nil, nil // no message
	}

	tgt := output.Messages[x.msgIndex]
	if err := json.Unmarshal([]byte(aws.StringValue(tgt.Body)), msg); err != nil {
		return nil, errors.Wrap(err, "Failed Unmarshal message body of SQS")
	}

	receipt := tgt.ReceiptHandle
	x.msgIndex++

	return receipt, nil
}

// DeleteMessage is wrapper of sqs:DeleteMessage
func (x *SQSService) DeleteMessage(url string, receipt string) error {
	region, err := extractSQSRegion(url)
	if err != nil {
		return err
	}

	client := x.newSQS(region)
	_, err = client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: aws.String(receipt),
	})

	if err != nil {
		return errors.Wrap(err, "Failed DeleteMessageInput")
	}

	return nil
}
