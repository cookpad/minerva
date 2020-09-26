package models

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3Objects(t *testing.T) {
	t.Run("Export and Import", func(tt *testing.T) {
		objectSet := []*S3Object{
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

		encObj, err := NewS3Objects(objectSet)
		require.NoError(tt, err)
		assert.Greater(tt, len(encObj.Raw), 0)
		fmt.Println(encObj.Raw)
		newObjects, err := encObj.Export()
		require.NoError(tt, err)
		assert.Equal(tt, 4, len(newObjects))
		assert.Contains(tt, newObjects, objectSet[0])
		assert.Contains(tt, newObjects, objectSet[1])
		assert.Contains(tt, newObjects, objectSet[2])
		assert.Contains(tt, newObjects, objectSet[3])
	})

	t.Run("Decoding invalid data", func(tt *testing.T) {
		newObjects, err := decodeS3Objects([]byte("{"))
		assert.Error(tt, err)
		assert.Nil(tt, newObjects)
	})
}
