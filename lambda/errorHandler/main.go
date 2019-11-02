package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

func handleRequest(ctx context.Context, event events.SQSEvent) error {
	logger.WithField("sqs", event).Info("Start ErrorHandler")
	for _, record := range event.Records {
		var v interface{}

		if err := json.Unmarshal([]byte(record.Body), &v); err != nil {
			return errors.Wrap(err, "Fail to parse")
		}

		logger.WithFields(logrus.Fields{
			"event":        v,
			"ErrorCode":    record.MessageAttributes["ErrorCode"],
			"ErrorMessage": record.MessageAttributes["ErrorMessage"],
			"RequestID":    record.MessageAttributes["RequestID"],
		}).Error("Lambda Error")

		var sns events.SNSEvent
		if err := json.Unmarshal([]byte(record.Body), &sns); err != nil {
			continue
		}

		for _, snsRecord := range sns.Records {
			var req interface{}
			if err := json.Unmarshal([]byte(snsRecord.SNS.Message), &req); err != nil {
				continue
			}

			logger.WithField("request", req).Info("Requested message")
		}
	}
	return nil
}

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))
	lambda.Start(handleRequest)
}
