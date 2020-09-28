package mock

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/klauspost/compress/gzip"
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
	data map[string]map[string]*s3Object
}

type s3Object struct {
	data     []byte
	encoding string
}

var mockS3ClientDataStore = map[string]map[string]*s3Object{}

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

	var body io.Reader
	if obj.encoding == "gzip" {
		gr, err := gzip.NewReader(bytes.NewReader(obj.data))
		if err != nil {
			log.Fatal("gzip.NewReader", err)
		}
		body = gr
	} else {
		body = bytes.NewReader(obj.data)
	}
	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(body),
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
		bucket = map[string]*s3Object{}
		x.data[*input.Bucket] = bucket
	}

	bucket[*input.Key] = &s3Object{
		data: raw,
	}

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
		bkt = make(map[string]*s3Object)
		x.data[bucket] = bkt
	}

	bkt[key] = &s3Object{
		data:     raw,
		encoding: encoding,
	}
	return nil
}

func (x *S3Client) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	bucket, ok := x.data[*input.Bucket]
	if !ok {
		return nil, errors.New(s3.ErrCodeNoSuchKey)
	}
	_, ok = bucket[*input.Key]
	if !ok {
		return nil, errors.New(s3.ErrCodeNoSuchKey)
	}

	return &s3.HeadObjectOutput{}, nil
}
