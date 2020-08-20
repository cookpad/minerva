package main_test

import (
	"io"
	"io/ioutil"
	"log"

	"github.com/m-mizutani/minerva/internal"

	"github.com/aws/aws-sdk-go/service/s3"
)

type dummyS3ClientIndex struct {
	k1, k2   io.ReadCloser
	dumpFile string
	internal.TestS3ClientBase
}

func (x *dummyS3ClientIndex) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	fd, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()

	buf := make([]byte, 4096)
	for {
		n, err := input.Body.Read(buf)
		if n > 0 {
			if _, err := fd.Write(buf[:n]); err != nil {
				log.Fatal(err)
			}
		}

		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}

	x.dumpFile = fd.Name()
	return &s3.PutObjectOutput{}, nil
}

func (x *dummyS3ClientIndex) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	switch *input.Key {
	case "k1.parquet":
		return &s3.GetObjectOutput{Body: x.k1}, nil
	case "k2.parquet":
		return &s3.GetObjectOutput{Body: x.k2}, nil
	}
	return nil, nil
}

/*
func dumpIndexParquet(rows []internal.IndexRecord) string {
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

	pw, err := writer.NewParquetWriter(fw, new(internal.IndexRecord), 4)
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

func loadIndexParquet(fpath string) []internal.IndexRecord {
	batchSize := 2
	buf := make([]internal.IndexRecord, batchSize)
	var rows []internal.IndexRecord

	fr, err := local.NewLocalFileReader(fpath)
	if err != nil {
		log.Fatal(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(internal.IndexRecord), 4)
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

func TestHandlerIndex(t *testing.T) {
	k1path := dumpIndexParquet([]internal.IndexRecord{
		{Tag: "t1", Field: "f1", Term: "v1"},
		{Tag: "t1", Field: "f2", Term: "v2"},
	})
	k2path := dumpIndexParquet([]internal.IndexRecord{
		{Tag: "t2", Field: "f3", Term: "v3"},
		{Tag: "t2", Field: "f4", Term: "v4"},
		{Tag: "t2", Field: "f4", Term: "v5"},
	})
	defer os.Remove(k1path)
	defer os.Remove(k2path)

	dummyS3 := dummyS3ClientIndex{
		k1: mustOpen(k1path),
		k2: mustOpen(k2path),
	}
	internal.TestInjectNewS3Client(&dummyS3)
	defer internal.TestFixNewS3Client()

	args := main.NewArgument()
	args.Queue = internal.MergeQueue{
		Schema: internal.ParquetSchemaIndex,
		SrcObjects: []internal.S3Location{
			{Region: "t1", Bucket: "b1", Key: "k1.parquet"},
			{Region: "t2", Bucket: "b1", Key: "k2.parquet"},
		},
		DstObject: internal.S3Location{
			Region: "t1",
			Bucket: "b1",
			Key:    "k3.parquet",
		},
	}

	err := main.MergeParquet(args)
	require.NoError(t, err)
	rows := loadIndexParquet(dummyS3.dumpFile)
	assert.Equal(t, 5, len(rows))

	hasV5term := false
	for _, r := range rows {
		if r.Term == "v5" {
			hasV5term = true
			break
		}
	}
	assert.True(t, hasV5term)
}

func TestHandlerIndexOneParquetFile(t *testing.T) {
	k1path := dumpIndexParquet([]internal.IndexRecord{
		{Tag: "t1", Field: "f1", Term: "v1"},
		{Tag: "t1", Field: "f2", Term: "v2"},
	})
	defer os.Remove(k1path)

	dummyS3 := dummyS3ClientIndex{
		k1: mustOpen(k1path),
	}
	internal.TestInjectNewS3Client(&dummyS3)
	defer internal.TestFixNewS3Client()

	args := main.NewArgument()
	args.Queue = internal.MergeQueue{
		Schema: internal.ParquetSchemaIndex,
		SrcObjects: []internal.S3Location{
			{Region: "t1", Bucket: "b1", Key: "k1.parquet"},
		},
		DstObject: internal.S3Location{
			Region: "t1",
			Bucket: "b1",
			Key:    "k3.parquet",
		},
	}

	err := main.MergeParquet(args)
	require.NoError(t, err)
	rows := loadIndexParquet(dummyS3.dumpFile)
	assert.Equal(t, 2, len(rows))

	hasV5term := false
	for _, r := range rows {
		if r.Term == "v1" {
			hasV5term = true
			break
		}
	}
	assert.True(t, hasV5term)
}

*/
