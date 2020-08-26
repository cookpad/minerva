package models

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"
)

type S3Object struct {
	Region string `json:"region"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func NewS3Object(region, bucket, key string) S3Object {
	return S3Object{
		Region: region,
		Bucket: bucket,
		Key:    key,
	}
}

func NewS3ObjectFromRecord(record events.S3EventRecord) S3Object {
	return S3Object{
		Region: record.AWSRegion,
		Bucket: record.S3.Bucket.Name,
		Key:    record.S3.Object.Key,
	}
}

func (x *S3Object) AppendKey(postfix string) *S3Object {
	newObj := *x
	newObj.Key += postfix
	return &newObj
}

func (x *S3Object) Encode() string {
	return fmt.Sprintf("%s@%s:%s", x.Bucket, x.Region, x.Key)
}

// Path returns full path by s3 bucket name and key.
// e.g.) s3://your-bucket/some/key
func (x *S3Object) Path() string {
	return fmt.Sprintf("s3://%s/%s", x.Bucket, x.Key)
}

func DecodeS3Object(raw string) (*S3Object, error) {
	p1 := strings.Split(raw, "@")
	if len(p1) != 2 {
		return nil, errors.New("Invalid S3 path encode (@ is required)")
	}

	p2 := strings.Split(p1[1], ":")
	if len(p2) < 2 {
		return nil, errors.New("Invalid S3 path encode (: is required)")
	}

	return &S3Object{
		Bucket: p1[0],
		Region: p2[0],
		Key:    strings.Join(p2[1:], ":"),
	}, nil
}
