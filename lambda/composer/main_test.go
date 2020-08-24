package main_test

import (
	"testing"
	"time"

	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/internal/testutil"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	composer "github.com/m-mizutani/minerva/lambda/composer"
)

func TestComposer(t *testing.T) {
	t.Run("Put new chunk", func(tt *testing.T) {
		chunkRepo := mock.NewChunkMockDB()

		event := testutil.EncapBySQS(models.ComposeQueue{
			S3Object: models.S3Object{
				Bucket: "test",
				Key:    "k1",
				Region: "ap-northeast-1",
			},
			Partition: "dt=1983-04-20",
			Schema:    "index",
			Size:      100,
		})
		args := handler.Arguments{
			EnvVars:   handler.EnvVars{},
			Event:     event,
			ChunkRepo: chunkRepo,
		}

		require.NoError(t, composer.Handler(args))
		chunks, ok := chunkRepo.Data["chunk/index"]
		require.True(tt, ok)
		require.Equal(t, 1, len(chunks))
		for _, chunk := range chunks {
			assert.Equal(tt, int64(100), chunk.TotalSize)
		}
	})

	t.Run("Update chunk", func(tt *testing.T) {
		now := time.Now().UTC()
		chunkRepo := mock.NewChunkMockDB()
		chunkRepo.PutChunk(models.S3Object{
			Bucket: "test1",
			Key:    "k1",
			Region: "ap-northeast-1",
		}, 120, "index", "dt=1983-04-20", now)

		event := testutil.EncapBySQS(models.ComposeQueue{
			S3Object: models.S3Object{
				Bucket: "test2",
				Key:    "k2",
				Region: "ap-northeast-1",
			},
			Partition: "dt=1983-04-20",
			Schema:    "index",
			Size:      100,
		})
		args := handler.Arguments{
			EnvVars:   handler.EnvVars{},
			Event:     event,
			ChunkRepo: chunkRepo,
		}

		require.NoError(t, composer.Handler(args))
		chunks, ok := chunkRepo.Data["chunk/index"]
		require.True(tt, ok)
		require.Equal(t, 1, len(chunks))
		for _, chunk := range chunks {
			assert.Equal(tt, int64(220), chunk.TotalSize)
			assert.Equal(tt, 2, len(chunk.S3Objects))
		}
	})
}
