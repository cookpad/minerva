package adaptor

import (
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
