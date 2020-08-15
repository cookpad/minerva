package indexer

import (
	"encoding/json"
	"time"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

type logQueue struct {
	Err       error
	Timestamp time.Time
	Tag       string
	Message   string
	Value     interface{}
	Seq       int32
	Src       internal.S3Object
}

// makeLogChannel loads log data from S3 bucket
func makeLogChannel(src internal.S3Object, reader *rlogs.Reader) chan *logQueue {
	ch := make(chan *logQueue, indexQueueSize)

	go func() {
		defer close(ch)

		for log := range reader.Read(&rlogs.AwsS3LogSource{
			Region: src.Region(),
			Bucket: src.Bucket(),
			Key:    src.Key(),
		}) {
			if log.Error != nil {
				ch <- &logQueue{Err: log.Error}
				return
			}

			raw, err := json.Marshal(log.Log.Values)
			if err != nil {
				ch <- &logQueue{Err: errors.Wrapf(err, "Fail to marshal log message: %v", log.Log.Values)}
				return
			}

			ch <- &logQueue{
				Message:   string(raw),
				Timestamp: log.Log.Timestamp,
				Value:     log.Log.Values,
				Tag:       log.Log.Tag,
				Seq:       int32(log.Log.Seq),
				Src:       src,
			}
		}
	}()

	return ch
}
