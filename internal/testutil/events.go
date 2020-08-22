package testutil

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-lambda-go/events"
)

// EncapBySQS encapslates data by events.SQSEvent and returns it.
func EncapBySQS(data interface{}) *events.SQSEvent {
	raw, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Can not marshal: %+v: %v", err, data)
	}

	return &events.SQSEvent{
		Records: []events.SQSMessage{
			{Body: string(raw)},
		},
	}
}
