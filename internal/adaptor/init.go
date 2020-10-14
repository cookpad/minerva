package adaptor

import "github.com/aws/aws-sdk-go/service/s3"

func init() {
	awsS3ClientCache = make(map[string]*s3.S3)
}
