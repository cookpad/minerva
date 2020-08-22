package main_test

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/pkg/lambda"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"

	composer "github.com/m-mizutani/minerva/lambda/composer"
)

func encapToSQS(data interface{}) *events.SQSEvent {
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

func TestComposer(t *testing.T) {
	chunkRepo := mock.NewChunkMockDB()

	event := encapToSQS(models.ComposeQueue{
		S3Object: models.S3Object{
			Bucket: "test",
			Key:    "k1",
			Region: "ap-northeast-1",
		},
		Partition: "dt=1983-04-20",
		Schema:    "index",
		Size:      100,
	})
	args := lambda.HandlerArguments{
		EnvVars:   lambda.EnvVars{},
		Event:     event,
		ChunkRepo: chunkRepo,
	}

	require.NoError(t, composer.Handler(args))
	assert.Equal(t, 1, len(chunkRepo.Data["chunk/index"]))
}
