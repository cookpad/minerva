package internal_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/models"
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
	bucket  string
	key     string
	prefix  string
	body    []byte
	delim   string
	deleted [][]string
	dummyS3ClientBase
}

func (x *dummyS3Client) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	x.bucket = aws.StringValue(input.Bucket)
	x.key = aws.StringValue(input.Key)
	raw, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	x.body = raw

	return &s3.PutObjectOutput{}, nil
}
func (x *dummyS3Client) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	x.bucket = aws.StringValue(input.Bucket)
	x.prefix = aws.StringValue(input.Prefix)
	x.delim = aws.StringValue(input.Delimiter)

	output := &s3.ListObjectsV2Output{
		CommonPrefixes: []*s3.CommonPrefix{
			{Prefix: aws.String("blue")},
			{Prefix: aws.String("orange")},
			{Prefix: aws.String("red")},
		},
	}
	return output, nil
}

func (x *dummyS3Client) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	x.bucket = aws.StringValue(input.Bucket)

	var keys []string
	for _, obj := range input.Delete.Objects {
		keys = append(keys, *obj.Key)
	}
	x.deleted = append(x.deleted, keys)
	output := &s3.DeleteObjectsOutput{}
	return output, nil
}

type dummyS3ClientIter struct {
	bucket     string
	key        string
	prefix     string
	body       []byte
	delim      string
	startAfter string
	dummyS3ClientBase
}

func (x *dummyS3ClientIter) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	x.bucket = aws.StringValue(input.Bucket)
	x.prefix = aws.StringValue(input.Prefix)
	x.delim = aws.StringValue(input.Delimiter)

	if input.StartAfter == nil {
		return &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("blue.txt"), Size: aws.Int64(128)},
				{Key: aws.String("orange.txt"), Size: aws.Int64(128)},
			},
			IsTruncated: aws.Bool(true),
		}, nil
	} else {
		x.startAfter = aws.StringValue(input.StartAfter)
		return &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("five.txt"), Size: aws.Int64(128)},
			},
			IsTruncated: aws.Bool(false),
		}, nil
	}
}

func TestS3PutObject(t *testing.T) {
	var dummy dummyS3Client
	internal.TestInjectNewS3Client(&dummy)
	defer internal.TestFixNewS3Client()

	fd, err := ioutil.TempFile("", "*.txt")
	require.NoError(t, err)
	defer os.Remove(fd.Name())
	fd.Write([]byte("five timeless words"))

	filePath := fd.Name()
	dst := models.NewS3Object("dokoka", "nanika", "sowaka.txt")
	err = internal.UploadFileToS3(filePath, dst)
	require.NoError(t, err)

	assert.Equal(t, "nanika", dummy.bucket)
	assert.Equal(t, "sowaka.txt", dummy.key)
	assert.Equal(t, "five timeless words", string(dummy.body))
}

func TestListS3Objects(t *testing.T) {
	var dummy dummyS3Client
	internal.TestInjectNewS3Client(&dummy)
	defer internal.TestFixNewS3Client()

	dirs, err := internal.ListS3Objects("dokoka", "nanika", "sowaka")
	require.NoError(t, err)

	assert.Equal(t, "nanika", dummy.bucket)
	assert.Equal(t, "sowaka", dummy.prefix)
	assert.Equal(t, "blue", dirs[0])
	assert.Equal(t, "orange", dirs[1])
}

func TestFindS3Objects(t *testing.T) {
	var dummy dummyS3ClientIter
	internal.TestInjectNewS3Client(&dummy)
	defer internal.TestFixNewS3Client()

	ch := internal.FindS3Objects("dokoka", "nanika", "sowaka")

	q1 := <-ch
	q2 := <-ch
	q3 := <-ch
	q4 := <-ch

	assert.NotNil(t, q1)
	assert.NotNil(t, q2)
	assert.NotNil(t, q3)
	assert.Nil(t, q4)
	assert.Equal(t, "blue.txt", *q1.Object.Key)
	assert.Equal(t, "orange.txt", *q2.Object.Key)
	assert.Equal(t, "five.txt", *q3.Object.Key)
	assert.Equal(t, "orange.txt", dummy.startAfter)
}

func TestDeleteObjects(t *testing.T) {
	var dummy dummyS3Client
	internal.TestInjectNewS3Client(&dummy)
	defer internal.TestFixNewS3Client()

	var tgt []models.S3Object
	var last string
	for i := 0; i < 2019; i++ {
		last = uuid.New().String()
		tgt = append(tgt, models.S3Object{Bucket: "b1", Key: last})
	}

	err := internal.DeleteS3Objects(tgt)
	require.NoError(t, err)
	assert.Equal(t, 3, len(dummy.deleted))
	assert.Equal(t, 1000, len(dummy.deleted[0]))
	assert.Equal(t, 1000, len(dummy.deleted[1]))
	assert.Equal(t, 19, len(dummy.deleted[2]))
	assert.Equal(t, last, dummy.deleted[2][18])
}

func TestIntegrationS3ObjectDownload(t *testing.T) {
	s3region, s3bucket, s3key := os.Getenv("S3_REGION"), os.Getenv("S3_BUCKET"), os.Getenv("S3_KEY")
	if s3region == "" || s3bucket == "" || s3key == "" {
		t.Skip("S3_REGION, S3_BUCKET or S3_KEY is not available")
	}

	tmpFile, err := internal.DownloadS3Object(models.NewS3Object(s3region, s3bucket, s3key))
	assert.NoError(t, err)
	assert.NotNil(t, tmpFile)
	fmt.Println("downloaded =", *tmpFile)
}
