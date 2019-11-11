package main_test

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/m-mizutani/minerva/internal"

	"github.com/aws/aws-sdk-go/service/s3"
	main "github.com/m-mizutani/minerva/lambda/mergeIndexObject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type dummyS3ClientMessage struct {
	c1, c2, c3     io.ReadCloser
	dumpFile       string
	deletedBucket  string
	deletedObjects []string

	internal.TestS3ClientBase
}

func (x *dummyS3ClientMessage) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	fd, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()

	buf := make([]byte, 4096)
	for {
		n, err := input.Body.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		} else if _, err := fd.Write(buf[:n]); err != nil {
			log.Fatal(err)
		}
	}

	x.dumpFile = fd.Name()
	return &s3.PutObjectOutput{}, nil
}

func (x *dummyS3ClientMessage) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	switch *input.Key {
	case "c1.parquet":
		return &s3.GetObjectOutput{Body: x.c1}, nil
	case "c2.parquet":
		return &s3.GetObjectOutput{Body: x.c2}, nil
	case "c3.parquet":
		return &s3.GetObjectOutput{Body: x.c3}, nil
	}
	return nil, nil
}

func (x *dummyS3ClientMessage) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	x.deletedBucket = *input.Bucket
	for _, obj := range input.Delete.Objects {
		x.deletedObjects = append(x.deletedObjects, *obj.Key)
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func dumpMessageParquet(rows []internal.MessageRecord) string {
	fd, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		log.Fatal(err)
	}
	fd.Close()
	filePath := fd.Name()

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(internal.MessageRecord), 4)
	if err != nil {
		log.Fatal(err)
	}
	defer pw.WriteStop()

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := range rows {
		if err := pw.Write(rows[i]); err != nil {
			log.Fatal(err)
		}
	}

	return filePath
}

func loadMessageParquet(fpath string) []internal.MessageRecord {
	batchSize := 2
	buf := make([]internal.MessageRecord, batchSize)
	var rows []internal.MessageRecord

	fr, err := local.NewLocalFileReader(fpath)
	if err != nil {
		log.Fatal(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(internal.MessageRecord), 4)
	if err != nil {
		log.Fatal(err)
	}
	defer pr.ReadStop()

	for i := 0; int64(i) < pr.GetNumRows(); i += batchSize {
		if err := pr.Read(&buf); err != nil {
			log.Fatal(err)
		}
		rows = append(rows, buf...)
	}

	return rows
}

func TestHandlerMessage(t *testing.T) {
	c1path := dumpMessageParquet([]internal.MessageRecord{
		{ObjectID: 1, Timestamp: 100, Seq: 0, Message: "not"},
		{ObjectID: 1, Timestamp: 100, Seq: 1, Message: "sane"},
	})
	c2path := dumpMessageParquet([]internal.MessageRecord{
		{ObjectID: 2, Timestamp: 100, Seq: 1, Message: "five"},
		{ObjectID: 2, Timestamp: 100, Seq: 2, Message: "timeless"},
		{ObjectID: 2, Timestamp: 100, Seq: 3, Message: "words"},
	})
	c3path := dumpMessageParquet([]internal.MessageRecord{
		{ObjectID: 2, Timestamp: 100, Seq: 1, Message: "fifth"},
		{ObjectID: 2, Timestamp: 100, Seq: 2, Message: "magic"},
	})

	defer os.Remove(c1path)
	defer os.Remove(c2path)
	defer os.Remove(c3path)

	dummyS3 := dummyS3ClientMessage{
		c1: mustOpen(c1path),
		c2: mustOpen(c2path),
		c3: mustOpen(c3path),
	}
	internal.TestInjectNewS3Client(&dummyS3)
	defer internal.TestFixNewS3Client()

	args := main.NewArgument()
	args.Queue = internal.MergeQueue{
		Schema: internal.ParquetSchemaMessage,
		SrcObjects: []internal.S3Location{
			{Region: "t1", Bucket: "b1", Key: "c1.parquet"},
			{Region: "t2", Bucket: "b1", Key: "c2.parquet"},
			{Region: "t2", Bucket: "b1", Key: "c3.parquet"},
		},
		DstObject: internal.S3Location{
			Region: "t1",
			Bucket: "b1",
			Key:    "k3.parquet",
		},
	}

	err := main.MergeParquet(args)
	require.NoError(t, err)
	rows := loadMessageParquet(dummyS3.dumpFile)
	assert.Equal(t, 7, len(rows))

	hasV5term := false
	for _, r := range rows {
		if r.Message == "five" {
			hasV5term = true
			break
		}
	}
	assert.True(t, hasV5term)

	assert.Equal(t, "b1", dummyS3.deletedBucket)
	assert.Equal(t, 3, len(dummyS3.deletedObjects))
	assert.Contains(t, dummyS3.deletedObjects, "c1.parquet")
	assert.Contains(t, dummyS3.deletedObjects, "c2.parquet")
	assert.Contains(t, dummyS3.deletedObjects, "c3.parquet")
}
