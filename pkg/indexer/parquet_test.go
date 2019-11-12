package indexer_test

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"
	"testing"
	"time"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/indexer"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	case *input.Bucket == "dst-bucket" && *input.Key == "dst-prefix/indices/dt=2019-09-18-23/unmerged/23/src-bucket/k1.json.csv.gz":
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

	ch := indexer.TestLoadMessage(indexer.NewS3Loc("ap-northeast-1", "src-bucket", "k1.json"), queues)

	meta := newDummyMeta()
	dumpers, err := indexer.DumpParquetFiles(ch, meta)
	require.NoError(t, err)
	require.Equal(t, 2, len(dumpers))

	idxDumper, msgDumper := dumpers[0], dumpers[1]
	if idxDumper.Schema() != internal.ParquetSchemaIndex {
		// Use index dumper for test
		idxDumper, msgDumper = dumpers[1], dumpers[0]
	}

	idxFiles := idxDumper.Files()
	require.Equal(t, 1, len(idxFiles))
	dst := idxFiles[0].Dst()
	dst.Bucket = "dst-bucket"
	dst.Prefix = "dst-prefix/"
	assert.Equal(t, "dst-prefix/indices/dt=2019-09-18-23/unmerged/src-bucket/k1.json.csv.gz", dst.S3Key())
	assert.Equal(t, "s3://dst-bucket/dst-prefix/indices/dt=2019-09-18-23/", dst.PartitionLocation())

	///read
	idxFileList := idxDumper.Files()
	assert.Equal(t, 1, len(idxFileList))
	fr1, err := os.Open(idxFileList[0].FilePath())
	require.NoError(t, err)
	defer fr1.Close()

	gr1, err := gzip.NewReader(fr1)
	require.NoError(t, err)
	defer gr1.Close()
	cr1 := csv.NewReader(gr1)
	rows, err := cr1.ReadAll()
	require.NotEqual(t, 0, len(rows))

	assert.Equal(t, 5, len(rows[0]))
	assert.Equal(t, "mylog", rows[0][0])
	assert.Equal(t, "5", rows[0][1])

	///read message records
	msgFileList := msgDumper.Files()
	assert.Equal(t, 1, len(msgFileList))
	fr2, err := os.Open(msgFileList[0].FilePath())
	require.NoError(t, err)
	defer fr2.Close()

	gr2, err := gzip.NewReader(fr2)
	require.NoError(t, err)
	defer gr2.Close()
	cr2 := csv.NewReader(gr2)
	rows, err = cr2.ReadAll()
	require.NotEqual(t, 0, len(rows))

	assert.NotEqual(t, 0, len(rows))
	assert.Equal(t, 4, len(rows[0]))
	assert.Equal(t, "test message 1", rows[0][3])

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

	src := indexer.NewS3Loc("ap-northeast-1", "src-bucket", "k1.json")
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
	if idxDumper.Schema() != internal.ParquetSchemaIndex {
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
