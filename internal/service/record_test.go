package service_test

import (
	"testing"
	"time"

	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newService() *service.RecordService {
	return service.NewRecordService(mock.NewS3Client, adaptor.NewMsgpackEncoder, adaptor.NewMsgpackDecoder)
}

type testLog struct {
	Word string
}

func TestRecordService(t *testing.T) {
	t.Run("Simple dump and load", func(tt *testing.T) {
		svc := newService()
		now := time.Now()
		queues := []*models.LogQueue{
			{
				Value:     &testLog{Word: "blue"},
				Seq:       1,
				Tag:       "test.log",
				Timestamp: now,
			},
		}
		for _, q := range queues {
			require.NoError(tt, svc.Dump(q, 123, &models.S3Object{Region: "x", Bucket: "b", Key: "k"}))
		}

		require.NoError(tt, svc.Close())
		objects := svc.RawObjects()
		require.Equal(tt, 2, len(objects))

		idxObj := objects[0]
		if objects[1].Schema() == "index" {
			idxObj = objects[1]
		}

		var records []models.Record
		ch := make(chan *models.RecordQueue, 1)
		go func() {
			defer close(ch)
			err := svc.Load(idxObj.Object(), models.ParquetSchemaName(idxObj.Schema()), ch)
			require.NoError(tt, err)
		}()

		for q := range ch {
			records = append(records, q.Records...)
		}

		require.Equal(tt, 1, len(records))
		idxRecord, ok := records[0].(*models.IndexRecord)
		require.True(tt, ok)
		assert.Equal(tt, "Word", idxRecord.Field)
		assert.Equal(tt, "blue", idxRecord.Term)
		assert.Equal(tt, "test.log", idxRecord.Tag)
		assert.Equal(tt, now.Unix(), idxRecord.Timestamp)
	})
}
