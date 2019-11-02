package indexer

import (
	"encoding/json"
	"time"

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
	Src       s3Loc
}

func loadMessage(src s3Loc, reader *rlogs.Reader) chan *logQueue {
	ch := make(chan *logQueue, indexQueueSize)

	go func() {
		defer close(ch)

		for log := range reader.Read(&rlogs.AwsS3LogSource{
			Region: src.Region,
			Bucket: src.Bucket,
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

			q := new(logQueue)
			*q = logQueue{
				Message:   string(raw),
				Timestamp: log.Log.Timestamp,
				Value:     log.Log.Values,
				Tag:       log.Log.Tag,
				Seq:       int32(log.Log.Seq),
				Src:       src,
			}
			ch <- q
		}
	}()

	return ch
}
