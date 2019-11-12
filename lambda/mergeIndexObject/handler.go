package main

import (
	"bufio"
	"compress/gzip"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
)

type arguments struct {
	Queue     internal.MergeQueue
	NotDelete bool
}

func mergeCSV(args arguments) error {
	pr, pw := io.Pipe()

	go func() {
		gw := gzip.NewWriter(pw)
		defer pw.Close()
		defer gw.Close()

		for _, src := range args.Queue.SrcObjects {
			ssn := session.New(&aws.Config{Region: aws.String(src.Region)})
			srcClient := s3.New(ssn)

			input := &s3.GetObjectInput{
				Bucket: &src.Bucket,
				Key:    &src.Key,
			}

			resp, err := srcClient.GetObject(input)
			if err != nil {
				logger.WithError(err).WithField("resp", resp).Fatal("Fail to download S3 object")
			}

			defer resp.Body.Close()
			gr, err := gzip.NewReader(resp.Body)
			if err != nil {
				logger.WithError(err).WithField("resp", resp).Fatal("Fail to open S3 gzip object")
			}
			scanner := bufio.NewScanner(gr)

			for scanner.Scan() {
				if _, err := gw.Write([]byte(scanner.Text() + "\n")); err != nil {
					logger.WithError(err).Fatal("Fail to write S3 gzip object")
				}
			}
		}
	}()

	ssn := session.New(&aws.Config{Region: aws.String(args.Queue.DstObject.Region)})
	dstBucket := args.Queue.DstObject.Bucket
	dstKey := args.Queue.DstObject.Key

	uploader := s3manager.NewUploader(ssn)
	input := &s3manager.UploadInput{
		Bucket: &dstBucket,
		Key:    &dstKey,
		Body:   pr,
	}

	resp, err := uploader.Upload(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return errors.Wrapf(aerr, "Fail to upload a parquet file in AWS: %s/%s", dstBucket, dstKey)
			}
		} else {
			return errors.Wrapf(aerr, "Fail to upload a parquet file in https: %s/%s", dstBucket, dstKey)
		}
	}

	logger.WithField("resp", resp).Debug("Success to upload merged object")

	if err := internal.DeleteS3Objects(args.Queue.SrcObjects); err != nil {
		return err
	}

	return nil
}
