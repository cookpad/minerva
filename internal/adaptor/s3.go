package adaptor

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3ClientFactory is interface S3Client constructor
type S3ClientFactory func(region string) S3Client

// S3Client is interface of AWS S3 SDK
type S3Client interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
	HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	Upload(bucket, key string, body io.Reader, encoding string) error
}

var awsS3ClientCache map[string]*s3.S3

// NewS3Client creates actual AWS S3 SDK client
func NewS3Client(region string) S3Client {
	if client, ok := awsS3ClientCache[region]; ok {
		return &awsS3Client{client: client}
	}

	ssn := session.New(&aws.Config{Region: aws.String(region)})
	client := s3.New(ssn)
	awsS3ClientCache[region] = client
	return &awsS3Client{client: client}
}

type awsS3Client struct {
	client *s3.S3
}

func (x *awsS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return x.client.GetObject(input)
}

func (x *awsS3Client) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return x.client.PutObject(input)
}

func (x *awsS3Client) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return x.client.DeleteObjects(input)
}

func (x *awsS3Client) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return x.client.HeadObject(input)
}

func (x *awsS3Client) Upload(bucket, key string, body io.Reader, encoding string) error {
	uploader := s3manager.NewUploaderWithClient(x.client)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		Body:            body,
		ContentEncoding: aws.String(encoding),
	})
	return err
}
