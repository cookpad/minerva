package internal

// TestInjectNewSqsClient replaces mock sqsClient for testing. Use the function in only test case.
func TestInjectNewSqsClient(c sqsClient) {
	newSqsClient = func(s string) sqsClient { return c }
}

// TestFixNewSqsClient fixes sqsClient constructor with original one. Use the function in only test case.
func TestFixNewSqsClient() { newSqsClient = newAwsSqsClient }

// TestS3ClientBase is base s3 client interface structure. The structure do nothing.
type TestS3ClientBase struct{}
