package internal

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type s3Client interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
}

var newS3Client = newAwsS3Client

func newAwsS3Client(region string) s3Client {
	ssn := session.New(&aws.Config{Region: aws.String(region)})
	return s3.New(ssn)
}

// UploadFileToS3 upload a specified local file to S3
func UploadFileToS3(filePath string, dst models.S3Object) error {
	fd, err := os.Open(filePath)
	if err != nil {
		return errors.Wrapf(err, "Fail to open a parquet file: %s", filePath)
	}
	defer fd.Close()

	client := newS3Client(dst.Region)
	input := &s3.PutObjectInput{
		Body:   fd,
		Bucket: aws.String(dst.Bucket),
		Key:    aws.String(dst.Key),
	}

	resp, err := client.PutObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return errors.Wrapf(aerr, "Fail to upload a parquet file in AWS: %s/%s", dst.Bucket, dst.Key)
			}
		} else {
			return errors.Wrapf(aerr, "Fail to upload a parquet file in https: %s/%s", dst.Bucket, dst.Key)
		}
	}

	Logger.WithFields(logrus.Fields{
		"resp":   resp,
		"bucket": dst.Bucket,
		"key":    dst.Key,
	}).Debug("Uploaded a parquet file")

	return nil
}

const s3DownloadBufferSize = 2 * 1024 * 1024 // 2MB

// Downloadmodels.S3Object downloads a specified remote object from S3
func DownloadS3Object(s3region, s3bucket, s3key string) (*string, error) {
	client := newS3Client(s3region)
	input := &s3.GetObjectInput{
		Bucket: &s3bucket,
		Key:    &s3key,
	}

	resp, err := client.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				Logger.WithFields(logrus.Fields{
					"bucket": s3bucket,
					"key":    s3key,
				}).Warn("No such key, ignored")
				return nil, nil
			}

			return nil, errors.Wrapf(aerr, "Fail to upload a parquet file in AWS: %s/%s", s3bucket, s3key)
		}

		return nil, errors.Wrapf(err, "Fail to upload a parquet file in https: %s/%s", s3bucket, s3key)

	}

	defer resp.Body.Close()

	Logger.WithField("resp", resp).Trace("Downloading a parquet file")

	fd, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		return nil, errors.Wrap(err, "Fail to create a temp parquet file")
	}
	defer fd.Close()

	buf := make([]byte, s3DownloadBufferSize)
	readBytes, writeBytes := 0, 0

	for {
		r, rErr := resp.Body.Read(buf)
		readBytes += r

		if r > 0 {
			w, wErr := fd.Write(buf[:r])
			if wErr != nil {
				return nil, errors.Wrap(wErr, "Fail to write a temp parquet file")
			}
			writeBytes += w
		}

		if rErr == io.EOF {
			break
		} else if rErr != nil {
			return nil, errors.Wrap(rErr, "Fail to read a parquet file from S3")
		}
	}

	fname := fd.Name()

	Logger.WithFields(logrus.Fields{
		"write": writeBytes, "read": readBytes,
		"fpath": fname, "srckey": s3key,
	}).Trace("Downloaded S3 object")

	return &fname, nil
}

// ListS3Objects is warpper of s3.ListObjectsV2
func ListS3Objects(s3region, s3bucket, s3prefix string) ([]string, error) {
	client := newS3Client(s3region)
	input := &s3.ListObjectsV2Input{
		Bucket:    &s3bucket,
		Prefix:    &s3prefix,
		Delimiter: aws.String("/"),
	}

	resp, err := client.ListObjectsV2(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return nil, errors.Wrapf(aerr, "Fail to list objects in AWS: %s/%s", s3bucket, s3prefix)
			}
		} else {
			return nil, errors.Wrapf(aerr, "Fail to list objects in http request: %s/%s", s3bucket, s3prefix)
		}
	}

	Logger.WithField("resp", resp).Trace("listed a parquet file")
	var objects []string
	for _, prefix := range resp.CommonPrefixes {
		objects = append(objects, *prefix.Prefix)
	}

	return objects, nil
}

// FindS3ObjectQueue is a result of FindS3Objects
type FindS3ObjectQueue struct {
	Err    error
	Object *s3.Object
}

const findS3ObjectQueueSize = 128

// FindS3Objects is warpper of s3.ListObjectsV2
func FindS3Objects(s3region, s3bucket, s3prefix string) chan *FindS3ObjectQueue {
	ch := make(chan *FindS3ObjectQueue, findS3ObjectQueueSize)

	go func() {
		defer close(ch)

		client := newS3Client(s3region)
		var startAfter *string

		for {
			input := &s3.ListObjectsV2Input{
				Bucket:     &s3bucket,
				Prefix:     &s3prefix,
				StartAfter: startAfter,
			}

			resp, err := client.ListObjectsV2(input)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					ch <- &FindS3ObjectQueue{
						Err: errors.Wrapf(aerr, "Fail to list objects in AWS: %s/%s", s3bucket, s3prefix),
					}
				} else {
					ch <- &FindS3ObjectQueue{
						Err: errors.Wrapf(err, "Fail to list objects in http request: %s/%s", s3bucket, s3prefix),
					}
				}
				return
			}

			Logger.WithField("resp", resp).Trace("listed a parquet file")

			for _, content := range resp.Contents {
				q := new(FindS3ObjectQueue)
				q.Object = content
				ch <- q
			}

			if resp.IsTruncated == nil || !(*resp.IsTruncated) {
				return
			}

			startAfter = resp.Contents[len(resp.Contents)-1].Key

			Logger.WithField("startAfter", *startAfter).Debug("Truncated")
		}
	}()

	return ch
}

const (
	// DeleteObjects can have a list of up to 1000 keys
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
	maxNumberOfS3DeletableObject = 1000
)

// DeleteS3Objects is warpper of s3.DeleteObjects
func DeleteS3Objects(locations []S3Location) error {
	if len(locations) == 0 {
		Logger.Warn("No target for DeleteObjects")
		return nil
	}

	Logger.WithField("len(locations)", len(locations)).Debug("Try to delete objects")

	var objects []*s3.ObjectIdentifier

	for i := range locations {
		if locations[i].Bucket != locations[0].Bucket {
			return fmt.Errorf("Multiple buckets are not allowed: %s and %s", locations[i].Bucket, locations[0].Bucket)
		}

		objects = append(objects, &s3.ObjectIdentifier{Key: &locations[i].Key})
	}

	client := newS3Client(locations[0].Region)

	for s := 0; s < len(objects); s += maxNumberOfS3DeletableObject {
		end := len(objects)
		if s+maxNumberOfS3DeletableObject < len(objects) {
			end = s + maxNumberOfS3DeletableObject
		}

		input := s3.DeleteObjectsInput{
			Bucket: &locations[0].Bucket,
			Delete: &s3.Delete{
				Objects: objects[s:end],
			},
		}

		resp, err := client.DeleteObjects(&input)
		if err != nil {
			return errors.Wrapf(err, "Fail to delete objects: %v", resp)
		}
	}

	return nil
}
