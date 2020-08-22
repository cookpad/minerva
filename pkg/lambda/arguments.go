package lambda

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/pkg/errors"
)

// HandlerArguments has environment variables, Event record and adaptor
type HandlerArguments struct {
	EnvVars
	Event interface{}

	NewS3     adaptor.S3ClientFactory
	NewSQS    adaptor.SQSClientFactory
	ChunkRepo repository.ChunkRepository
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

// ChunkService provides ChunkRepository implementation (DynamoDB)
func (x *HandlerArguments) ChunkService() *service.ChunkService {
	var repo repository.ChunkRepository

	if x.ChunkRepo != nil {
		repo = x.ChunkRepo
	} else {
		repo = repository.NewChunkDynamoDB(x.AwsRegion, x.ChunkTableName)
	}

	return service.NewChunkService(repo, nil)
}

// S3Service provides service.S3Service with S3 adaptor
func (x *HandlerArguments) S3Service() *service.S3Service {
	if x.NewS3 != nil {
		return service.NewS3Service(x.NewS3)
	}
	return service.NewS3Service(adaptor.NewS3Client)
}

// SQSService provides service.SQSService with SQS adaptor
func (x *HandlerArguments) SQSService() *service.SQSService {
	if x.NewSQS != nil {
		return service.NewSQSService(x.NewSQS)
	}
	return service.NewSQSService(adaptor.NewSQSClient)
}
