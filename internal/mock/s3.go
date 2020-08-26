package mock

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/m-mizutani/minerva/internal/adaptor"
)

// NewS3Client is constructor of S3 Mock
func NewS3Client(region string) adaptor.S3Client {
	return &S3Client{
		data: mockS3ClientDataStore,
	}
}

// S3Client is on memory S3Client mock
type S3Client struct {
	data map[string]map[string][]byte
}

var mockS3ClientDataStore = map[string]map[string][]byte{}

// GetObject of S3Client loads []bytes from memory
func (x *S3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	bucket, ok := x.data[*input.Bucket]
	if !ok {
		return nil, errors.New(s3.ErrCodeNoSuchKey)
	}
	obj, ok := bucket[*input.Key]
	if !ok {
		return nil, errors.New(s3.ErrCodeNoSuchKey)
	}

	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(obj)),
	}, nil
}

// PutObject of S3Client saves []bytes to memory
func (x *S3Client) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	raw, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	bucket, ok := x.data[*input.Bucket]
	if !ok {
		bucket = map[string][]byte{}
		x.data[*input.Bucket] = bucket
	}

	bucket[*input.Key] = raw

	return &s3.PutObjectOutput{}, nil
}

// DeleteObjects of S3Client remove []bytes from memory
func (x *S3Client) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	bucket, ok := x.data[*input.Bucket]
	if !ok {
		return nil, errors.New(s3.ErrCodeNoSuchKey)
	}

	for _, obj := range input.Delete.Objects {
		if _, ok := bucket[*obj.Key]; !ok {
			return nil, errors.New(s3.ErrCodeNoSuchKey)
		}

		delete(bucket, *obj.Key)
	}

	return nil, nil
}

// Upload of S3Client put data from io.Reader
func (x *S3Client) Upload(bucket, key string, body io.Reader, encoding string) error {
	raw, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}

	bkt, ok := x.data[bucket]
	if !ok {
		bkt = map[string][]byte{}
		x.data[bucket] = bkt
	}

	bkt[key] = raw

	return nil
}
