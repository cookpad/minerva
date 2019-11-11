package main_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/m-mizutani/minerva/internal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	main "github.com/m-mizutani/minerva/lambda/listIndexObject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyS3ClientBase struct{}

func (x *dummyS3ClientBase) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return nil, nil
}
func (x *dummyS3ClientBase) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return nil, nil
}
func (x *dummyS3ClientBase) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return nil, nil
}

type dummyS3Client struct {
	sentInput []s3.ListObjectsV2Input
	dummyS3ClientBase
}

func (x *dummyS3Client) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	x.sentInput = append(x.sentInput, *input)

	if input.Delimiter != nil {
		if strings.HasPrefix(aws.StringValue(input.Prefix), "test-prefix/indices/") {
			output := &s3.ListObjectsV2Output{
				CommonPrefixes: []*s3.CommonPrefix{
					{Prefix: aws.String("test-prefix/indices/tg=mylog1/dt=2019-10-12")},
					{Prefix: aws.String("test-prefix/indices/tg=mylog2/dt=2019-10-12")},
				},
			}

			return output, nil
		} else {
			return &s3.ListObjectsV2Output{}, nil
		}
	} else {
		switch aws.StringValue(input.Prefix) {
		case "test-prefix/indices/tg=mylog1/dt=2019-10-12/unmerged/":
			return &s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("test-prefix/indices/tg=mylog1/dt=2019-10-12/b1/obj1.parquet"), Size: aws.Int64(12)},
					{Key: aws.String("test-prefix/indices/tg=mylog1/dt=2019-10-12/b1/obj2.parquet"), Size: aws.Int64(32)},
				},
				IsTruncated: aws.Bool(false),
			}, nil

		case "test-prefix/indices/tg=mylog2/dt=2019-10-12/unmerged/":
			return &s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("test-prefix/indices/tg=mylog2/dt=2019-10-12/b2/objx.parquet"), Size: aws.Int64(12)},
					{Key: aws.String("test-prefix/indices/tg=mylog2/dt=2019-10-12/b2/objy.parquet"), Size: aws.Int64(32)},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		}

		return nil, fmt.Errorf("No valid prefix: %v", *input)
	}
}

type dummySqsClient struct {
	sentInput []sqs.SendMessageInput
}

func (x *dummySqsClient) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	x.sentInput = append(x.sentInput, *input)
	return &sqs.SendMessageOutput{}, nil
}

func TestHandlerBasic(t *testing.T) {
	var dummyS3 dummyS3Client
	internal.TestInjectNewS3Client(&dummyS3)
	defer internal.TestFixNewS3Client()

	var dummySQS dummySqsClient
	internal.TestInjectNewSqsClient(&dummySQS)
	defer internal.TestFixNewSqsClient()

	args := main.NewArgument()
	ts, err := time.Parse("2006-01-02T15:04:05", "2019-10-12T10:00:00")
	require.NoError(t, err)

	args.BaseTime = ts
	args.S3Region = "ap-northeast-1"
	args.S3Bucket = "test-bucket"
	args.S3Prefix = "test-prefix/"

	err = main.ListParquet(args)
	require.NoError(t, err)

	require.Equal(t, 4, len(dummyS3.sentInput))

	assert.Equal(t, "test-bucket", *dummyS3.sentInput[0].Bucket)
	assert.Equal(t, "test-prefix/indices/", *dummyS3.sentInput[0].Prefix)
	assert.Equal(t, "/", *dummyS3.sentInput[0].Delimiter)

	assert.Equal(t, "test-bucket", *dummyS3.sentInput[1].Bucket)
	assert.Equal(t, "test-prefix/indices/tg=mylog1/dt=2019-10-12/unmerged/", *dummyS3.sentInput[1].Prefix)
	assert.Nil(t, dummyS3.sentInput[1].Delimiter)

	require.Equal(t, 2, len(dummySQS.sentInput))
	var q internal.MergeQueue
	err = json.Unmarshal([]byte(*dummySQS.sentInput[0].MessageBody), &q)
	require.NoError(t, err)
	assert.Equal(t, internal.ParquetSchemaIndex, q.Schema)
	assert.Equal(t, "test-bucket", q.DstObject.Bucket)
	assert.Equal(t, "ap-northeast-1", q.DstObject.Region)
	assert.Contains(t, q.DstObject.Key, "test-prefix/indices/tg=mylog1/dt=2019-10-12/merged/")
	assert.Equal(t, 2, len(q.SrcObjects))
	assert.Equal(t, "ap-northeast-1", q.SrcObjects[0].Region)
	assert.Equal(t, "test-bucket", q.SrcObjects[0].Bucket)
	assert.Equal(t, "test-prefix/indices/tg=mylog1/dt=2019-10-12/b1/obj1.parquet", q.SrcObjects[0].Key)
}

type dummyS3ClientObjSizeTest struct {
	sentInput   []s3.ListObjectsV2Input
	contentSize []int64
	dummyS3ClientBase
}

func (x *dummyS3ClientObjSizeTest) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	x.sentInput = append(x.sentInput, *input)

	if !strings.HasPrefix(aws.StringValue(input.Prefix), "test-prefix/indices/") {
		return &s3.ListObjectsV2Output{}, nil
	}

	if input.Delimiter != nil {
		output := &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("test-prefix/indices/tg=mylog1/dt=2019-10-12")},
			},
		}
		return output, nil
	} else {
		var contents []*s3.Object
		for _, s := range x.contentSize {
			contents = append(contents, &s3.Object{
				Key:  aws.String("test-prefix/indices/tg=mylog1/dt=2019-10-12/b1/" + uuid.New().String() + ".parquet"),
				Size: &s,
			})
		}

		return &s3.ListObjectsV2Output{
			Contents:    contents,
			IsTruncated: aws.Bool(false),
		}, nil
	}
}

func TestHandlerSplitObject(t *testing.T) {
	var dummyS3 dummyS3ClientObjSizeTest
	internal.TestInjectNewS3Client(&dummyS3)
	defer internal.TestFixNewS3Client()

	var dummySQS dummySqsClient
	internal.TestInjectNewSqsClient(&dummySQS)
	defer internal.TestFixNewSqsClient()

	args := main.NewArgument()
	ts, err := time.Parse("2006-01-02T15:04:05", "2019-10-12T10:00:00")
	require.NoError(t, err)

	args.BaseTime = ts
	args.S3Region = "ap-northeast-1"
	args.S3Bucket = "test-bucket"
	args.S3Prefix = "test-prefix/"

	dummyS3.contentSize = []int64{
		30 * 1000 * 1000,
		30 * 1000 * 1000,
		// should split
		30 * 1000 * 1000,
	}
	dummySQS.sentInput = []sqs.SendMessageInput{}

	err = main.ListParquet(args)
	require.NoError(t, err)
	assert.Equal(t, 3, len(dummySQS.sentInput))

	dummyS3.contentSize = []int64{
		20 * 1000 * 1000,
		20 * 1000 * 1000,
		20 * 1000 * 1000,
		// should split
		20 * 1000 * 1000,
		20 * 1000 * 1000,
		20 * 1000 * 1000,
		// should split
		20 * 1000 * 1000,
	}
	dummySQS.sentInput = []sqs.SendMessageInput{}
	err = main.ListParquet(args)
	require.NoError(t, err)
	assert.Equal(t, 4, len(dummySQS.sentInput))
}

type dummyS3ClientMsgSizeTest struct {
	dummyS3ClientBase
	sentInput []s3.ListObjectsV2Input
	msgCount  int
}

func (x *dummyS3ClientMsgSizeTest) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	x.sentInput = append(x.sentInput, *input)

	if !strings.HasPrefix(aws.StringValue(input.Prefix), "test-prefix/indices/") {
		return &s3.ListObjectsV2Output{}, nil
	}

	if input.Delimiter != nil {
		output := &s3.ListObjectsV2Output{
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("test-prefix/indices/tg=mylog1/dt=2019-10-12")},
			},
		}
		return output, nil
	} else {
		var contents []*s3.Object
		for i := 0; i < x.msgCount; i++ {
			contents = append(contents, &s3.Object{
				Key:  aws.String("test-prefix/indices/tg=mylog1/dt=2019-10-12/b1/" + uuid.New().String() + ".parquet"),
				Size: aws.Int64(1),
			})
		}

		return &s3.ListObjectsV2Output{
			Contents:    contents,
			IsTruncated: aws.Bool(false),
		}, nil
	}
}

func TestHandlerSplitMessage(t *testing.T) {
	var dummyS3 dummyS3ClientMsgSizeTest
	internal.TestInjectNewS3Client(&dummyS3)
	defer internal.TestFixNewS3Client()

	var dummySQS dummySqsClient
	internal.TestInjectNewSqsClient(&dummySQS)
	defer internal.TestFixNewSqsClient()

	args := main.NewArgument()
	ts, err := time.Parse("2006-01-02T15:04:05", "2019-10-12T10:00:00")
	require.NoError(t, err)

	args.BaseTime = ts
	args.S3Region = "ap-northeast-1"
	args.S3Bucket = "test-bucket"
	args.S3Prefix = "test-prefix/"

	// average message size may be around 100 bytes.
	// Then, total size of 5,000 messages is estimated over 196KB.
	// Messages should be splitted to over 3 SQS requests.
	dummyS3.msgCount = 5 * 1000
	dummySQS.sentInput = []sqs.SendMessageInput{}

	err = main.ListParquet(args)
	require.NoError(t, err)
	assert.True(t, 3 <= len(dummySQS.sentInput))

	msgSize0 := len([]byte(*dummySQS.sentInput[0].MessageBody))
	assert.True(t, 250*1000 > msgSize0)
	msgSize1 := len([]byte(*dummySQS.sentInput[1].MessageBody))
	assert.True(t, 250*1000 > msgSize1)
}
