package models_test

import (
	"testing"

	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3Objects(t *testing.T) {
	t.Run("Export and Import", func(tt *testing.T) {
		objects := []*models.S3Object{
			{
				Bucket: "blue",
				Region: "ap-northeast-1",
				Key:    "path/to/obj1",
			},
			{
				Bucket: "blue",
				Region: "ap-northeast-1",
				Key:    "path/to/obj2",
			},
			{
				Bucket: "blue",
				Region: "ap-northeast-1",
				Key:    "path/to/dir/in/obj1",
			},
			{
				Bucket: "orange",
				Region: "ap-northeast-1",
				Key:    "path/to/obj1",
			},
		}

		raw, err := models.EncodeS3Objects(objects)
		require.NoError(tt, err)
		assert.Greater(tt, len(raw), 0)

		newObjects, err := models.DecodeS3Objects(raw)
		require.NoError(tt, err)
		assert.Equal(tt, 4, len(newObjects))
		assert.Contains(tt, newObjects, objects[0])
		assert.Contains(tt, newObjects, objects[1])
		assert.Contains(tt, newObjects, objects[2])
		assert.Contains(tt, newObjects, objects[3])
	})

	t.Run("Decoding invalid data", func(tt *testing.T) {
		newObjects, err := models.DecodeS3Objects([]byte("{"))
		assert.Error(tt, err)
		assert.Nil(tt, newObjects)
	})
}
