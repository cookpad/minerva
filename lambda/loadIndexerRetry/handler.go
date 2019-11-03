package main

import (
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
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
	sqsRegion, err := sqsURLtoRegion(args.SrcSQS)
	if err != nil {
		return errors.Wrapf(err, "Fail to parse SQS URL: %s", args.SrcSQS)
	}

	ssn := session.Must(session.NewSession(&aws.Config{Region: aws.String(sqsRegion)}))
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
			/*
				body := aws.StringValue(msg.Body)
				if err := json.Unmarshal([]byte(body), &alert); err != nil {
					return nil, errors.Wrapf(err, "Fail to unmarshal sqs message to alert: %s", body)
				}

				alerts = append(alerts, alert)

				out, err := client.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &alertQueueURL,
					ReceiptHandle: msg.ReceiptHandle,
				})
				if err != nil {
					return nil, errors.Wrap(err, "Fail to delete alert message in SQS")
				}
				Logger.WithField("out", out).Debug("Deleted")
			*/
		}
	}

	return nil
}
