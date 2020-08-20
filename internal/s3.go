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

const (
	// DeleteObjects can have a list of up to 1000 keys
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
	maxNumberOfS3DeletableObject = 1000

	s3DownloadBufferSize = 2 * 1024 * 1024 // 2MB
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

// DownloadS3Object downloads a specified remote object from S3
func DownloadS3Object(obj models.S3Object) (*string, error) {
	client := newS3Client(obj.Region)
	input := &s3.GetObjectInput{
		Bucket: &obj.Bucket,
		Key:    &obj.Key,
	}

	resp, err := client.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				Logger.WithFields(logrus.Fields{
					"bucket": obj.Bucket,
					"key":    obj.Key,
				}).Warn("No such key, ignored")
				return nil, nil
			}

			return nil, errors.Wrapf(aerr, "Fail to upload a parquet file in AWS: %s/%s", obj.Bucket, obj.Key)
		}

		return nil, errors.Wrapf(err, "Fail to upload a parquet file in https: %s/%s", obj.Bucket, obj.Key)

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
		"fpath": fname, "srckey": obj.Key,
	}).Trace("Downloaded S3 object")

	return &fname, nil
}

// DeleteS3Objects is warpper of s3.DeleteObjects
func DeleteS3Objects(locations []models.S3Object) error {
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
