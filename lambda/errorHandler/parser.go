package main

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
)

type awsEvent struct {
	Records []map[string]interface{} `json:"Records"`
}

func jsonToRecords(jdata string) ([]interface{}, error) {
	var records []interface{}

	var event awsEvent
	if err := json.Unmarshal([]byte(jdata), &event); err != nil {
		return nil, errors.Wrapf(err, "Fail to unmarshal base event: %v", jdata)
	}

	for _, record := range event.Records {
		recordData, err := json.Marshal(record)
		if err != nil {
			return nil, errors.Wrapf(err, "Fail to marshal inner SNS record: %v", record)
		}

		switch record["EventSource"] {
		case "aws:sns":
			var snsRecord events.SNSEventRecord
			if err := json.Unmarshal(recordData, &snsRecord); err != nil {
				return nil, errors.Wrapf(err, "Fail to unmarshal inner SNS record: %v", string(recordData))
			}

			records = append(records, &snsRecord)

		case "aws:sqs":
			var sqsRecord events.SQSMessage
			if err := json.Unmarshal(recordData, &sqsRecord); err != nil {
				return nil, errors.Wrapf(err, "Fail to unmarshal inner SQS record: %v", string(recordData))
			}

			records = append(records, &sqsRecord)

		default:
			var rec map[string]interface{}
			if err := json.Unmarshal(recordData, rec); err != nil {
				return nil, errors.Wrapf(err, "Fail to unmarshal inner record: %v", string(recordData))
			}
			records = append(records, rec)

		}
	}

	return records, nil
}
