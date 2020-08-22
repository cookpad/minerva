package adaptor_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3Mock(t *testing.T) {
	t.Run("Can get object saved by PutObject", func(tt *testing.T) {
		bucket := uuid.New().String()
		client := adaptor.NewS3Mock("test")
		_, err := client.PutObject(&s3.PutObjectInput{
			Bucket: &bucket,
			Key:    aws.String("k1/obj"),
			Body:   strings.NewReader("abc"),
		})
		require.NoError(tt, err)

		getOut, err := client.GetObject(&s3.GetObjectInput{
			Bucket: &bucket,
			Key:    aws.String("k1/obj"),
		})
		require.NoError(tt, err)
		raw, err := ioutil.ReadAll(getOut.Body)
		require.NoError(tt, err)
		assert.Equal(tt, "abc", string(raw))
	})

	t.Run("Can not get object unsaved by PutObject", func(tt *testing.T) {
		bucket := uuid.New().String()
		client := adaptor.NewS3Mock("test")

		_, err := client.GetObject(&s3.GetObjectInput{
			Bucket: &bucket,
			Key:    aws.String("k1/obj"),
		})
		require.Error(tt, err)
	})

	t.Run("Can not get deleted object", func(tt *testing.T) {
		bucket := uuid.New().String()
		client := adaptor.NewS3Mock("test")
		_, err := client.PutObject(&s3.PutObjectInput{
			Bucket: &bucket,
			Key:    aws.String("k1/obj"),
			Body:   strings.NewReader("abc"),
		})
		require.NoError(tt, err)

		_, err = client.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &s3.Delete{
				Objects: []*s3.ObjectIdentifier{
					{
						Key: aws.String("k1/obj"),
					},
				},
			},
		})
		require.NoError(tt, err)

		_, err = client.GetObject(&s3.GetObjectInput{
			Bucket: &bucket,
			Key:    aws.String("k1/obj"),
		})
		require.Error(tt, err)
	})
}
