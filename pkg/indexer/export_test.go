package indexer

import "github.com/m-mizutani/minerva/internal"

var (
	ToKeyValuePairs = toKeyValuePairs

	TestLoadMessage        = testLoadMessage
	TestLoadMessageChannel = testLoadMessageChannel
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

func testLoadMessage(loc s3Loc, queues []*LogQueue) chan *logQueue {
	ch := make(chan *logQueue, 128)
	go func() {
		defer close(ch)

		for _, q := range queues {
			q.Src = loc
			ch <- (*logQueue)(q)
		}
	}()

	return ch
}

func testLoadMessageChannel(loc s3Loc, input chan *LogQueue) chan *logQueue {
	ch := make(chan *logQueue, 128)
	go func() {
		defer close(ch)

		for q := range input {
			q.Src = loc
			ch <- (*logQueue)(q)
		}
	}()

	return ch
}

func (x *parquetFile) FilePath() string              { return x.filePath }
func (x *parquetFile) Dst() internal.ParquetLocation { return x.dst }
