package main

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	objectTotalSizeLimit  int64 = 38 * 1000 * 1000 // 38MB
	messageTotalSizeLimit       = 228 * 1000       // 228KB
)

type mergeQueueEmitter struct {
	Dst              internal.ParquetLocation
	BaseRegion       string
	TargetURL        string
	ObjectTotalSize  int64
	MessageTotalSize int
	Objects          []internal.S3Location
}

func (x *mergeQueueEmitter) Flush() error {
	if len(x.Objects) == 0 {
		return nil
	}

	fname := time.Now().UTC().Format("20060102_150405_") + strings.Replace(uuid.New().String(), "-", "_", -1) + ".parquet"

	x.Dst.SrcKey = fname
	q := internal.MergeQueue{
		Schema:     x.Dst.Schema,
		SrcObjects: x.Objects,
		DstObject: internal.S3Location{
			Region: x.Dst.Region,
			Bucket: x.Dst.Bucket,
			Key:    x.Dst.S3Key(),
		},
	}
	if err := internal.SendSQS(&q, x.BaseRegion, x.TargetURL); err != nil {
		return err
	}

	logger.WithFields(logrus.Fields{
		"MessageTotalSize": x.MessageTotalSize,
		"ObjectTotalSize":  x.ObjectTotalSize,
		"len(x.Objects)":   len(x.Objects),
		"dst":              q.DstObject,
		"schema":           q.Schema,
	}).Debug("Emit merge queue")

	x.ObjectTotalSize = 0
	x.MessageTotalSize = 0
	x.Objects = []internal.S3Location{}
	return nil
}

func (x *mergeQueueEmitter) Push(obj *s3.Object) error {
	if obj == nil {
		logger.Warn("mergeQueueEmitter.Push received nil object")
		return nil // nothing to do
	}

	loc := internal.S3Location{
		Region: x.Dst.Region,
		Bucket: x.Dst.Bucket,
		Key:    aws.StringValue(obj.Key),
		Size:   aws.Int64Value(obj.Size),
	}

	raw, err := json.Marshal(loc)
	if err != nil {
		return errors.Wrapf(err, "Fail to marshal S3Location: %v", loc)
	}
	msgSize := len(raw)
	objSize := aws.Int64Value(obj.Size)

	if len(x.Objects) > 0 {
		if x.MessageTotalSize+msgSize > messageTotalSizeLimit {
			logger.WithFields(logrus.Fields{
				"MessageTotalSize":      x.MessageTotalSize,
				"msgSize":               msgSize,
				"messageTotalSizeLimit": messageTotalSizeLimit,
				"len(x.Objects)":        len(x.Objects),
			}).Debug("Emit because messageTotalSizeLimit exceeded")

			if err := x.Flush(); err != nil {
				return err
			}
		}

		if x.ObjectTotalSize+objSize > objectTotalSizeLimit {
			logger.WithFields(logrus.Fields{
				"ObjectTotalSize":      x.ObjectTotalSize,
				"msgSize":              objSize,
				"objectTotalSizeLimit": objectTotalSizeLimit,
				"len(x.Objects)":       len(x.Objects),
			}).Debug("Emit because objectTotalSizeLimit exceeded")

			if err := x.Flush(); err != nil {
				return err
			}
		}
	}

	x.Objects = append(x.Objects, loc)
	x.ObjectTotalSize += objSize
	x.MessageTotalSize += msgSize

	return nil
}

// sendMergeQueue summarize objects and sends objects list to MergeQueue.
// Assumption: objects received from ch have same prefix including tag key section 'tg=xxx'
func sendMergeQueue(ch chan *internal.FindS3ObjectQueue, dst internal.ParquetLocation, baseRegion, targetURL string) error {
	emitter := mergeQueueEmitter{
		Dst:        dst,
		BaseRegion: baseRegion,
		TargetURL:  targetURL,
	}
	defer emitter.Flush()

	for q := range ch {
		if q.Err != nil {
			return errors.Wrap(q.Err, "Fail in sendMergeQueue")
		}

		if err := emitter.Push(q.Object); err != nil {
			return err
		}
	}

	if err := emitter.Flush(); err != nil {
		return err
	}

	return nil
}
