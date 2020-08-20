package indexer_test

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/indexer"
	"github.com/m-mizutani/minerva/pkg/models"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type dummyMeta struct{}

func newDummyMeta() *dummyMeta {
	return &dummyMeta{}
}

func (x *dummyMeta) GetObjecID(s3bucket, s3key string) (int64, error) { return 5, nil }
func (x *dummyMeta) HeadPartition(partitionKey string) (bool, error)  { return false, nil }
func (x *dummyMeta) PutPartition(partitionKey string) error           { return nil }

type dummyS3Client struct {
	origin  io.ReadCloser
	parquet io.ReadCloser
	internal.TestS3ClientBase
}

func (x *dummyS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	switch {
	case *input.Bucket == "src-bucket" && *input.Key == "k1.json":
		return &s3.GetObjectOutput{Body: x.origin}, nil
	case *input.Bucket == "dst-bucket" && *input.Key == "dst-prefix/indices/dt=2019-09-18/tg=aws.cloudtrail/23/src-bucket/k1.json.parquet":
		return &s3.GetObjectOutput{Body: x.origin}, nil
	}
	return nil, nil
}

func TestCreateParquet(t *testing.T) {
	type logMessage struct {
		SrcIP  string
		DstIP  string
		Method string
	}

	queues := []*indexer.LogQueue{
		{
			Timestamp: time.Date(2019, 9, 18, 23, 0, 0, 0, time.UTC),
			Tag:       "mylog",
			Message:   "test message 1",
			Value:     &logMessage{"10.0.0.1", "10.0.0.2", "Get"},
			Seq:       0,
		},
		{
			Timestamp: time.Date(2019, 9, 18, 23, 1, 0, 0, time.UTC),
			Tag:       "mylog",
			Message:   "test message 2",
			Value:     &logMessage{"10.0.0.3", "10.0.3.2", "Get"},
			Seq:       0,
		},
		{
			Timestamp: time.Date(2019, 9, 18, 23, 2, 0, 0, time.UTC),
			Tag:       "mylog",
			Message:   "test message 3",
			Value:     &logMessage{"10.0.0.4", "10.0.1.2", "Put"},
			Seq:       0,
		},
	}

	srcObj := models.NewS3Object("ap-northeast-1", "src-bucket", "k1.json")
	ch := indexer.TestLoadMessage(srcObj, queues)

	meta := newDummyMeta()
	dumpers, err := indexer.DumpParquetFiles(ch, meta)
	require.NoError(t, err)
	require.Equal(t, 2, len(dumpers))

	idxDumper, msgDumper := dumpers[0], dumpers[1]
	if idxDumper.Schema() != models.ParquetSchemaIndex {
		// Use index dumper for test
		idxDumper, msgDumper = dumpers[1], dumpers[0]
	}

	idxFiles := idxDumper.Files()
	require.Equal(t, 1, len(idxFiles))
	dst := idxFiles[0].Dst()
	dst.Bucket = "dst-bucket"
	dst.Prefix = "dst-prefix/"
	assert.Equal(t, "dst-prefix/raw/indices/dt=2019-09-18-23/src-bucket/k1.json.parquet", dst.S3Key())
	assert.Equal(t, "s3://dst-bucket/dst-prefix/indices/dt=2019-09-18-23/", dst.PartitionLocation())

	///read
	idxFileList := idxDumper.Files()
	assert.Equal(t, 1, len(idxFileList))
	fr, err := local.NewLocalFileReader(idxFileList[0].FilePath())
	require.NoError(t, err)

	pr, err := reader.NewParquetReader(fr, new(models.IndexRecord), 4)
	require.NoError(t, err)

	num := int(pr.GetNumRows())
	require.NotEqual(t, 0, num)
	read := false

	rec := make([]models.IndexRecord, 1) //read 1 rows
	err = pr.Read(&rec)
	require.NoError(t, err)
	assert.NotEmpty(t, rec[0].Field)
	assert.Equal(t, "mylog", rec[0].Tag)
	assert.Equal(t, int64(5), rec[0].ObjectID)
	read = true

	assert.True(t, read)
	pr.ReadStop()
	fr.Close()

	///read message records
	msgFileList := msgDumper.Files()
	assert.Equal(t, 1, len(msgFileList))
	fr, err = local.NewLocalFileReader(msgFileList[0].FilePath())
	require.NoError(t, err)

	pr, err = reader.NewParquetReader(fr, new(models.MessageRecord), 4)
	require.NoError(t, err)

	num = int(pr.GetNumRows())
	assert.NotEqual(t, 0, num)
	mrec := make([]models.MessageRecord, 1) //read 1 rows
	err = pr.Read(&mrec)
	require.NoError(t, err)
	assert.NotEmpty(t, mrec[0].Message)
	assert.Equal(t, int64(5), mrec[0].ObjectID)

	pr.ReadStop()
	fr.Close()

	for _, d := range dumpers {
		require.NoError(t, d.Delete())
	}
}

func TestSplitLargeParquetFiles(t *testing.T) {
	type logMessage struct {
		SrcIP  string
		DstIP  string
		Method string
		Data   string
	}

	src := models.NewS3Object("ap-northeast-1", "src-bucket", "k1.json")
	input := make(chan *indexer.LogQueue, 1024)
	ch := indexer.TestLoadMessageChannel(src, input)

	oldLimit := indexer.DumperParquetSizeLimit
	defer func() { indexer.DumperParquetSizeLimit = oldLimit }()
	indexer.DumperParquetSizeLimit = 10 * 1000

	go func() {
		defer close(input)
		for i := 0; i < 1000; i++ {
			input <- &indexer.LogQueue{
				Timestamp: time.Date(2019, 9, 18, 23, 0, 0, 0, time.UTC),
				Tag:       "mylog",
				Message:   "test message 1",
				Value:     &logMessage{"10.0.0.1", "10.0.0.2", "Get", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},
				Seq:       0,
			}
		}
	}()

	meta := newDummyMeta()
	dumpers, err := indexer.DumpParquetFiles(ch, meta)
	require.NoError(t, err)
	require.Equal(t, 2, len(dumpers))

	idxDumper, msgDumper := dumpers[0], dumpers[1]
	if idxDumper.Schema() != models.ParquetSchemaIndex {
		// Use index dumper for test
		idxDumper, msgDumper = dumpers[1], dumpers[0]
	}

	defer func() {
		for _, f := range idxDumper.Files() {
			os.Remove(f.FilePath())
		}
		for _, f := range msgDumper.Files() {
			os.Remove(f.FilePath())
		}
	}()

	assert.NotEqual(t, 1, len(idxDumper.Files()))
	assert.NotEqual(t, 1, len(msgDumper.Files()))

	idxFiles := idxDumper.Files()
	k0 := idxFiles[0].Dst().S3Key()
	k1 := idxFiles[1].Dst().S3Key()

	assert.NotEqual(t, k0, k1)
	assert.NotEqual(t, len(k0), len(k1))
}
