package main

import (
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
)

type arguments struct {
	SrcSQS string
	DstSQS string
	Region string
}

func sqsURLtoRegion(url string) (string, error) {
	urlPattern := []string{
		// https://sqs.ap-northeast-1.amazonaws.com/21xxxxxxxxxxx/test-queue
		`https://sqs\.([a-z0-9\-]+)\.amazonaws\.com`,

		// https://us-west-1.queue.amazonaws.com/2xxxxxxxxxx/test-queue
		`https://([a-z0-9\-]+)\.queue\.amazonaws\.com`,
	}

	for _, ptn := range urlPattern {
		re := regexp.MustCompile(ptn)
		group := re.FindSubmatch([]byte(url))
		if len(group) == 2 {
			return string(group[1]), nil
		}
	}

	return "", errors.New("unsupported SQS URL syntax")
}

const maxQueueCount = 1024

func handler(args arguments) error {
	srcRegion, err := sqsURLtoRegion(args.SrcSQS)
	if err != nil {
		return errors.Wrapf(err, "Fail to parse source SQS URL: %s", args.SrcSQS)
	}
	dstRegion, err := sqsURLtoRegion(args.DstSQS)
	if err != nil {
		return errors.Wrapf(err, "Fail to parse destination SQS URL: %s", args.SrcSQS)
	}

	ssn := session.Must(session.NewSession(&aws.Config{Region: aws.String(srcRegion)}))
	sqsClient := sqs.New(ssn)

	for i := 0; i < maxQueueCount; i++ {
		resp, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: &args.SrcSQS,
		})

		if err != nil {
			return errors.Wrapf(err, "Fail to receive retry queue: %s", args.SrcSQS)
		}
		if len(resp.Messages) == 0 {
			break
		}

		logger.WithField("resp", resp).Debug("Recv message")

		for _, msg := range resp.Messages {
			logger.WithField("msg", msg).Info("Message")

			if err := internal.SendSQS(msg, dstRegion, args.DstSQS); err != nil {
				return errors.Wrap(err, "Fail to send SQS for retry")
			}

			out, err := sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      &args.SrcSQS,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				return errors.Wrap(err, "Fail to delete alert message in SQS")
			}

			logger.WithField("out", out).Debug("Deleted")
		}
	}

	return nil
}
