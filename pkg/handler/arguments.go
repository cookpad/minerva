package handler

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/pkg/errors"
)

// Arguments has environment variables, Event record and adaptor
type Arguments struct {
	EnvVars
	Event interface{}

	NewS3      adaptor.S3ClientFactory    `json:"-"`
	NewSQS     adaptor.SQSClientFactory   `json:"-"`
	ChunkRepo  repository.ChunkRepository `json:"-"`
	NewEncoder adaptor.EncoderFactory     `json:"-"`
	NewDecoder adaptor.DecoderFactory     `json:"-"`
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
func (x *Arguments) DecapSQSEvent() ([]EventRecord, error) {
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
func (x *Arguments) BindEvent(ev interface{}) error {
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
func (x *Arguments) ChunkService() *service.ChunkService {
	var repo repository.ChunkRepository

	if x.ChunkRepo != nil {
		repo = x.ChunkRepo
	} else {
		repo = repository.NewChunkDynamoDB(x.AwsRegion, x.ChunkTableName)
	}

	return service.NewChunkService(repo, nil)
}

// S3Service provides service.S3Service with S3 adaptor
func (x *Arguments) S3Service() *service.S3Service {
	return service.NewS3Service(x.newS3())
}

// SQSService provides service.SQSService with SQS adaptor
func (x *Arguments) SQSService() *service.SQSService {
	return service.NewSQSService(x.newSQS())
}

// RecordService provides encode/decode logic and S3 access for normalized log data
func (x *Arguments) RecordService() *service.RecordService {
	return service.NewRecordService(x.newS3(), x.newEncoder(), x.newDecoder())
}

func (x *Arguments) newS3() adaptor.S3ClientFactory {
	if x.NewS3 != nil {
		return x.NewS3
	} else {
		return adaptor.NewS3Client
	}
}
func (x *Arguments) newSQS() adaptor.SQSClientFactory {
	if x.NewSQS != nil {
		return x.NewSQS
	} else {
		return adaptor.NewSQSClient
	}
}
func (x *Arguments) newEncoder() adaptor.EncoderFactory {
	if x.NewEncoder != nil {
		return x.NewEncoder
	} else {
		return adaptor.NewMsgpackEncoder
	}
}
func (x *Arguments) newDecoder() adaptor.DecoderFactory {
	if x.NewDecoder != nil {
		return x.NewDecoder
	} else {
		return adaptor.NewMsgpackDecoder
	}
}
