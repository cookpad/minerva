package internal_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m-mizutani/minerva/internal"
)

type dummySqsClient struct {
	message string
	url     string
}

func (x *dummySqsClient) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	x.message = aws.StringValue(input.MessageBody)
	x.url = aws.StringValue(input.QueueUrl)
	return &sqs.SendMessageOutput{}, nil
}

func TestSendSQS(t *testing.T) {
	dummy := dummySqsClient{}
	internal.TestInjectNewSqsClient(&dummy)
	defer internal.TestFixNewSqsClient()

	type testMessage struct {
		Name string
	}
	m1 := testMessage{Name: "Aozaki"}
	err := internal.SendSQS(&m1, "test-region", "test-url")
	require.NoError(t, err)

	assert.Equal(t, "test-url", dummy.url)
	var m2 testMessage
	err = json.Unmarshal([]byte(dummy.message), &m2)
	require.NoError(t, err)
	assert.Equal(t, "Aozaki", m2.Name)
}
