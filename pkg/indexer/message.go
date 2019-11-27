package indexer

import (
	"encoding/json"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

type logQueue struct {
	Err     error
	Records []logRecord
}

type logRecord struct {
	Err       error
	Timestamp time.Time
	Tag       string
	Message   string
	Value     interface{}
	Seq       int32
	Src       s3Loc
}

const loadMessageBatchSize = 4098

// LoadMessage load log data from S3 bucket
func LoadMessage(src s3Loc, reader *rlogs.Reader) chan *logQueue {
	ch := make(chan *logQueue, indexQueueSize)

	go func() {
		defer close(ch)

		var records []logRecord
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

			r := logRecord{
				Message:   string(raw),
				Timestamp: log.Log.Timestamp,
				Value:     log.Log.Values,
				Tag:       log.Log.Tag,
				Seq:       int32(log.Log.Seq),
				Src:       src,
			}

			records = append(records, r)
			if len(records) >= loadMessageBatchSize {
				q := logQueue{Records: records}
				ch <- &q
				records = []logRecord{}
			}
		}

		if len(records) > 0 {
			q := logQueue{Records: records}
			ch <- &q
		}
	}()

	return ch
}
