package internal

import "github.com/aws/aws-sdk-go/service/s3"

// TestInjectNewSqsClient replaces mock sqsClient for testing. Use the function in only test case.
func TestInjectNewSqsClient(c sqsClient) {
	newSqsClient = func(s string) sqsClient { return c }
}

// TestFixNewSqsClient fixes sqsClient constructor with original one. Use the function in only test case.
func TestFixNewSqsClient() { newSqsClient = newAwsSqsClient }

// TestInjectNewS3Client replaces mock s3Client for testing. Use the function in only test case.
func TestInjectNewS3Client(c s3Client) {
	newS3Client = func(s string) s3Client { return c }
}

// TestFixNewS3Client fixes s3Client constructor with original one. Use the function in only test case.
func TestFixNewS3Client() { newS3Client = newAwsS3Client }

// TestS3ClientBase is base s3 client interface structure. The structure do nothing.
type TestS3ClientBase struct{}

// PutObject is dummy function. It should be overwritten if required in test.
func (x *TestS3ClientBase) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return nil, nil
}

// GetObject is dummy function. It should be overwritten if required in test.
func (x *TestS3ClientBase) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return nil, nil
}

// ListObjectsV2 is dummy function. It should be overwritten if required in test.
func (x *TestS3ClientBase) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return nil, nil
}

// DeleteObjects is dummy function. It should be overwritten if required in test.
func (x *TestS3ClientBase) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return nil, nil
}
