package indexer

import (
	"github.com/m-mizutani/minerva/pkg/models"
)

var (
	ToKeyValuePairs = toKeyValuePairs

	TestLoadMessage        = testLoadMessage
	TestLoadMessageChannel = testLoadMessageChannel
	DumpParquetFiles       = dumpParquetFiles
)

var Logger = logger

type LogQueue logQueue

func LookupValue(kvList []keyValuePair, key string) interface{} {
	for _, kv := range kvList {
		if kv.Key == key {
			return kv.Value
		}
	}
	return nil
}

func testLoadMessage(obj models.S3Object, queues []*LogQueue) chan *logQueue {
	ch := make(chan *logQueue, 128)
	go func() {
		defer close(ch)

		for _, q := range queues {
			q.Src = obj
			ch <- (*logQueue)(q)
		}
	}()

	return ch
}

func testLoadMessageChannel(obj models.S3Object, input chan *LogQueue) chan *logQueue {
	ch := make(chan *logQueue, 128)
	go func() {
		defer close(ch)

		for q := range input {
			q.Src = obj
			ch <- (*logQueue)(q)
		}
	}()

	return ch
}

func (x *parquetFile) FilePath() string            { return x.filePath }
func (x *parquetFile) Dst() models.ParquetLocation { return x.dst }
