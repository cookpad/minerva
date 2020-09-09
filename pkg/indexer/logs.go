package indexer

import (
	"encoding/json"

	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

const (
	indexQueueSize = 128
)

// makeLogChannel loads log data from S3 bucket
func makeLogChannel(src models.S3Object, reader *rlogs.Reader) chan *models.LogQueue {
	ch := make(chan *models.LogQueue, indexQueueSize)

	go func() {
		defer close(ch)

		logSource := &rlogs.AwsS3LogSource{
			Region: src.Region,
			Bucket: src.Bucket,
			Key:    src.Key,
		}

		for log := range reader.Read(logSource) {
			if log.Error != nil {
				ch <- &models.LogQueue{Err: log.Error}
				return
			}

			raw, err := json.Marshal(log.Log.Values)
			if err != nil {
				ch <- &models.LogQueue{Err: errors.Wrapf(err, "Fail to marshal log message: %v", log.Log.Values)}
				return
			}

			ch <- &models.LogQueue{
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
