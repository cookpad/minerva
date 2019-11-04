package main

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type arguments struct {
	SQSEvent      events.SQSEvent
	RetryQueueURL string
	Region        string
	IndexerDLQ    string
}

func handler(args arguments) error {

	for _, dlqRecord := range args.SQSEvent.Records {
		var v interface{}

		if err := json.Unmarshal([]byte(dlqRecord.Body), &v); err != nil {
			return errors.Wrap(err, "Fail to parse")
		}

		logger.WithFields(logrus.Fields{
			"event":        v,
			"ErrorCode":    dlqRecord.MessageAttributes["ErrorCode"],
			"ErrorMessage": dlqRecord.MessageAttributes["ErrorMessage"],
			"RequestID":    dlqRecord.MessageAttributes["RequestID"],
		}).Error("Lambda Error")

		records, err := jsonToRecords(dlqRecord.Body)
		if err != nil {
			return err
		}

		for _, record := range records {
			var req interface{}

			switch record.(type) {
			case *events.SNSEventRecord:
				r := record.(*events.SNSEventRecord)
				if err := json.Unmarshal([]byte(r.SNS.Message), &req); err != nil {
					return errors.Wrapf(err, "Fail to unmarshal original event in SNS: %v", r.SNS.Message)
				}
			case *events.SQSMessage:
				r := record.(*events.SQSMessage)
				if err := json.Unmarshal([]byte(r.Body), &req); err != nil {
					return errors.Wrapf(err, "Fail to unmarshal original event in SQS: %v", r.Body)
				}
			}

			if dlqRecord.EventSourceARN == args.IndexerDLQ && req != nil {
				if err := internal.SendSQS(req, args.Region, args.RetryQueueURL); err != nil {
					return err
				}
			}

			logger.WithField("request", req).Info("Requested message")
		}
	}

	return nil
}
