package service

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/m-mizutani/minerva/internal/adaptor"
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

// S3Service is accessor to S3
type S3Service struct {
	newS3 adaptor.S3ClientFactory
}

// NewS3Service is constructor of
func NewS3Service(newS3 adaptor.S3ClientFactory) *S3Service {
	return &S3Service{
		newS3: newS3,
	}
}

// AsyncUpload is for uploading object by io.Reader.
func (x *S3Service) AsyncUpload(body io.Reader, dst models.S3Object, encoding string) error {
	client := x.newS3(dst.Region)
	if err := client.Upload(dst.Bucket, dst.Key, body, encoding); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return errors.Wrapf(aerr, "Fail to upload a record file in AWS: %s/%s", dst.Bucket, dst.Key)
			}
		} else {
			return errors.Wrapf(aerr, "Fail to upload a record file in https: %s/%s", dst.Bucket, dst.Key)
		}
	}

	return nil
}

// AsyncDownload is for downloading data via io.ReadCloser
func (x *S3Service) AsyncDownload(src models.S3Object) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(src.Bucket),
		Key:    aws.String(src.Key),
	}
	client := x.newS3(src.Region)
	output, err := client.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return nil, errors.Wrapf(aerr, "Fail to upload a parquet file in AWS: %s/%s", src.Bucket, src.Key)
			}
		} else {
			return nil, errors.Wrapf(aerr, "Fail to upload a parquet file in https: %s/%s", src.Bucket, src.Key)
		}
	}

	return output.Body, nil
}

// UploadFileToS3 upload a specified local file to S3
func (x *S3Service) UploadFileToS3(filePath string, dst models.S3Object) error {
	fd, err := os.Open(filePath)
	if err != nil {
		return errors.Wrapf(err, "Fail to open a parquet file: %s", filePath)
	}
	defer fd.Close()

	client := x.newS3(dst.Region)
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

	logger.WithFields(logrus.Fields{
		"resp":   resp,
		"bucket": dst.Bucket,
		"key":    dst.Key,
	}).Debug("Uploaded a parquet file")

	return nil
}

// DownloadS3Object downloads a specified remote object from S3
func (x *S3Service) DownloadS3Object(obj models.S3Object) (*string, error) {
	client := x.newS3(obj.Region)
	input := &s3.GetObjectInput{
		Bucket: &obj.Bucket,
		Key:    &obj.Key,
	}

	resp, err := client.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				logger.WithFields(logrus.Fields{
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

	logger.WithField("resp", resp).Trace("Downloading a parquet file")

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

	logger.WithFields(logrus.Fields{
		"write": writeBytes, "read": readBytes,
		"fpath": fname, "srckey": obj.Key,
	}).Trace("Downloaded S3 object")

	return &fname, nil
}

// DeleteS3Objects is warpper of s3.DeleteObjects
func (x *S3Service) DeleteS3Objects(objects []*models.S3Object) error {
	if len(objects) == 0 {
		logger.Warn("No target for DeleteObjects")
		return nil
	}

	logger.WithField("len(objects)", len(objects)).Debug("Try to delete objects")

	var objectIDs []*s3.ObjectIdentifier

	for i := range objects {
		if objects[i].Bucket != objects[0].Bucket {
			return fmt.Errorf("Multiple buckets are not allowed: %s and %s", objects[i].Bucket, objects[0].Bucket)
		}

		objectIDs = append(objectIDs, &s3.ObjectIdentifier{Key: &objects[i].Key})
	}

	client := x.newS3(objects[0].Region)

	for s := 0; s < len(objectIDs); s += maxNumberOfS3DeletableObject {
		end := len(objectIDs)
		if s+maxNumberOfS3DeletableObject < len(objectIDs) {
			end = s + maxNumberOfS3DeletableObject
		}

		input := s3.DeleteObjectsInput{
			Bucket: &objects[0].Bucket,
			Delete: &s3.Delete{
				Objects: objectIDs[s:end],
			},
		}

		resp, err := client.DeleteObjects(&input)
		if err != nil {
			return errors.Wrapf(err, "Fail to delete objects: %v", resp)
		}
	}

	return nil
}
