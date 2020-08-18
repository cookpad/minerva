package lambda

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
)

// HandlerArguments has environment variables, Event record and adaptor
type HandlerArguments struct {
	EnvVars
	Event interface{}
}

// EventRecord is decapslated event data (e.g. Body of SQS event)
type EventRecord []byte

// Bind unmarshal event record to object
func (x EventRecord) Bind(ev interface{}) error {
	if err := json.Unmarshal(x, ev); err != nil {
		Logger.WithField("raw", string(x)).Error("json.Unmarshal")
		return errors.Wrap(err, "Failed json.Unmarshal in DecodeEvent")
	}
	return nil
}

// DecapSQSEvent decapslates wrapped body data in SQSEvent
func (x *HandlerArguments) DecapSQSEvent() ([]EventRecord, error) {
	var sqsEvent events.SQSEvent
	if err := x.BindEvent(&sqsEvent); err != nil {
		return nil, err
	}

	var output []EventRecord
	for _, record := range sqsEvent.Records {
		output = append(output, EventRecord(record.Body))
	}

	return output, nil
}

// BindEvent directly decode event data and unmarshal to ev object.
func (x *HandlerArguments) BindEvent(ev interface{}) error {
	raw, err := json.Marshal(x.Event)
	if err != nil {
		Logger.WithField("event", x.Event).Error("json.Marshal")
		return errors.Wrap(err, "Failed to marshal lambda event in BindEvent")
	}

	if err := json.Unmarshal(raw, ev); err != nil {
		Logger.WithField("raw", string(raw)).Error("json.Unmarshal")
		return errors.Wrap(err, "Failed json.Unmarshal in BindEvent")
	}

	return nil
}

// ChunkTable provides ChunkRepository implementation (DynamoDB)
func (x *HandlerArguments) ChunkTable() internal.ChunkRepository {
	return internal.NewChunkDynamoDB(x.AwsRegion, x.ChunkTableName)
}
