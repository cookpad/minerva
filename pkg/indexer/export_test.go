package indexer

import (
	"github.com/m-mizutani/minerva/pkg/models"
)

var (
	TestLoadMessage        = testLoadMessage
	TestLoadMessageChannel = testLoadMessageChannel
)

var Logger = logger

func testLoadMessage(obj models.S3Object, queues []*models.LogQueue) chan *models.LogQueue {
	ch := make(chan *models.LogQueue, 128)
	go func() {
		defer close(ch)

		for _, q := range queues {
			q.Src = obj
			ch <- (*models.LogQueue)(q)
		}
	}()

	return ch
}

func testLoadMessageChannel(obj models.S3Object, input chan *models.LogQueue) chan *models.LogQueue {
	ch := make(chan *models.LogQueue, 128)
	go func() {
		defer close(ch)

		for q := range input {
			q.Src = obj
			ch <- (*models.LogQueue)(q)
		}
	}()

	return ch
}
