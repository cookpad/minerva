package adaptor

import (
	"bytes"
	"errors"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3ClientFactory is interface S3Client constructor
type S3ClientFactory func(region string) S3Client

// S3Client is interface of AWS S3 SDK
type S3Client interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
}

// NewS3Client creates actual AWS S3 SDK client
func NewS3Client(region string) S3Client {
	ssn := session.New(&aws.Config{Region: aws.String(region)})
	return s3.New(ssn)
}

// NewS3Mock is constructor of S3 Mock
func NewS3Mock(region string) S3Client {
	return &S3Mock{
		data: s3MockDataStore,
	}
}

// S3Mock is on memory S3Client mock
type S3Mock struct {
	data map[string]map[string][]byte
}

var s3MockDataStore = map[string]map[string][]byte{}

// GetObject of S3Mock loads []bytes from memory
func (x *S3Mock) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
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

// PutObject of S3Mock saves []bytes to memory
func (x *S3Mock) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
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

// DeleteObjects of S3Mock remove []bytes from memory
func (x *S3Mock) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
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
