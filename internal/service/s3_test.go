package service_test

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3PutObject(t *testing.T) {
	bucket := uuid.New().String()
	svc := service.NewS3Service(mock.NewS3Client)

	fd, err := ioutil.TempFile("", "*.txt")
	require.NoError(t, err)
	defer os.Remove(fd.Name())
	fd.Write([]byte("five timeless words"))

	filePath := fd.Name()
	dst := models.NewS3Object("dokoka", bucket, "sowaka.txt")
	err = svc.UploadFileToS3(filePath, dst)
	require.NoError(t, err)

	mock := mock.NewS3Client("dokoka")
	out, err := mock.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String("sowaka.txt"),
	})
	require.NoError(t, err)
	raw, err := ioutil.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, "five timeless words", string(raw))
}

func TestDeleteObjects(t *testing.T) {
	bucket := uuid.New().String()
	svc := service.NewS3Service(mock.NewS3Client)

	mock := mock.NewS3Client("dokoka")
	objects := []*models.S3Object{
		{Region: "dokoka", Bucket: bucket, Key: "k1"},
		{Region: "dokoka", Bucket: bucket, Key: "k2"},
		{Region: "dokoka", Bucket: bucket, Key: "k3"},
	}

	for _, obj := range objects {
		_, err := mock.PutObject(&s3.PutObjectInput{
			Bucket: &obj.Bucket,
			Key:    &obj.Key,
			Body:   strings.NewReader("a"),
		})
		require.NoError(t, err)
	}

	err := svc.DeleteS3Objects(objects[:2])
	require.NoError(t, err)

	// Not found
	_, err = mock.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String("k1"),
	})
	require.Error(t, err)
	_, err = mock.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String("k2"),
	})
	require.Error(t, err)

	// Found
	_, err = mock.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String("k3"),
	})
	require.NoError(t, err)
}
