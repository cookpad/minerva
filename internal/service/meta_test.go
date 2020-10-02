package service_test

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal/mock"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestMetaDynamoDB(t *testing.T) {
	region := os.Getenv("MINERVA_TEST_REGION")
	table := os.Getenv("MINERVA_TEST_TABLE")

	if region == "" || table == "" {
		t.Skip("Both of MINERVA_TEST_REGION and MINERVA_TEST_TABLE are required")
	}

	repo := repository.NewMetaDynamoDB(region, table)
	svc := service.NewMetaService(repo)
	testMetaService(t, svc)
}

func TestMetaMock(t *testing.T) {
	repo := mock.NewMetaRepository()
	svc := service.NewMetaService(repo)
	testMetaService(t, svc)
}

func testMetaService(t *testing.T, svc *service.MetaService) {
	t.Run("ObjectID", func(tt *testing.T) {
		testMetaServiceObjectID(tt, svc)
	})

	t.Run("ObjectPathSet", func(tt *testing.T) {
		testMetaObjectPathSet(tt, svc)
	})

	t.Run("Partition", func(tt *testing.T) {
		testMetaPartition(tt, svc)
	})
}

func testMetaServiceObjectID(t *testing.T, svc *service.MetaService) {
	t.Run("Got same ID by same S3 path", func(tt *testing.T) {
		id1, err := svc.GetObjectID("blue", "obj1")
		require.NoError(tt, err)
		require.NotEqual(tt, 0, id1)
		id2, err := svc.GetObjectID("blue", "obj1")
		require.NoError(tt, err)
		assert.Equal(tt, id1, id2)
	})

	t.Run("Got different ID by different S3 path", func(tt *testing.T) {
		id1, err := svc.GetObjectID("blue", "obj1")
		require.NoError(tt, err)
		require.NotEqual(tt, 0, id1)

		id2, err := svc.GetObjectID("blue", "obj2")
		require.NoError(tt, err)
		assert.NotEqual(tt, id1, id2)
		id3, err := svc.GetObjectID("orange", "obj1")
		require.NoError(tt, err)
		assert.NotEqual(tt, id1, id3)
		assert.NotEqual(tt, id2, id3)
	})
}

func testMetaObjectPathSet(t *testing.T, svc *service.MetaService) {
	t.Run("Can get object path set", func(tt *testing.T) {
		prefix := uuid.New().String()
		id1, id2 := uuid.New().String(), uuid.New().String()
		items := []*repository.MetaRecordObject{
			{
				RecordID: id1,
				Schema:   models.ParquetSchemaIndex,
				S3Object: models.S3Object{
					Bucket: "blue",
					Region: "ap-northeast-1",
					Key:    prefix + "/obj1",
				},
			},
			{
				RecordID: id1,
				S3Object: models.S3Object{
					Bucket: "blue",
					Region: "ap-northeast-1",
					Key:    prefix + "/obj2",
				},
				Schema: models.ParquetSchemaMessage,
			},
			{
				RecordID: id2,
				S3Object: models.S3Object{
					Bucket: "orange",
					Region: "ap-northeast-1",
					Key:    prefix + "/obj1",
				},
				Schema: models.ParquetSchemaIndex,
			},
		}

		err := svc.PutObjects(items)
		require.NoError(tt, err)

		results1, err := svc.GetObjects([]string{id1, id2}, models.ParquetSchemaIndex)
		require.NoError(tt, err)
		require.Equal(tt, 2, len(results1))
		assert.Contains(tt, results1, &items[0].S3Object)
		assert.Contains(tt, results1, &items[2].S3Object)
		assert.NotContains(tt, results1, &items[1].S3Object)

		results2, err := svc.GetObjects([]string{id1, id2}, models.ParquetSchemaMessage)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(results2))
		assert.NotContains(tt, results2, &items[0].S3Object)
		assert.NotContains(tt, results2, &items[2].S3Object)
		assert.Contains(tt, results2, &items[1].S3Object)

		// Only id1
		results3, err := svc.GetObjects([]string{id1}, models.ParquetSchemaIndex)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(results3))
		assert.Contains(tt, results3, &items[0].S3Object)
		assert.NotContains(tt, results3, &items[1].S3Object)
		assert.NotContains(tt, results3, &items[2].S3Object)
	})

	t.Run("Got 1 object even if both schema of index and message exist", func(tt *testing.T) {
		prefix := uuid.New().String()
		id1 := uuid.New().String()
		items := []*repository.MetaRecordObject{
			{
				RecordID: id1,
				Schema:   models.ParquetSchemaIndex,
				S3Object: models.S3Object{
					Bucket: "blue",
					Region: "ap-northeast-1",
					Key:    prefix + "/obj1",
				},
			},
			{
				RecordID: id1,
				S3Object: models.S3Object{
					Bucket: "blue",
					Region: "ap-northeast-1",
					Key:    prefix + "/obj1",
				},
				Schema: models.ParquetSchemaMessage,
			},
		}

		err := svc.PutObjects(items)
		require.NoError(tt, err)

		results1, err := svc.GetObjects([]string{id1}, models.ParquetSchemaIndex)
		require.NoError(tt, err)
		require.Equal(tt, 1, len(results1))
	})
}

func testMetaPartition(t *testing.T, svc *service.MetaService) {
	t.Run("partition does not exist", func(tt *testing.T) {
		prefix := uuid.New().String()
		pkey := prefix + "xxx"
		exists, err := svc.HeadPartition(pkey)
		require.NoError(tt, err)
		assert.False(tt, exists)
	})

	t.Run("partition was registered", func(tt *testing.T) {
		prefix := uuid.New().String()
		pkey := prefix + "xxx"

		err := svc.PutPartition(pkey)
		require.NoError(tt, err)

		exists, err := svc.HeadPartition(pkey)
		require.NoError(tt, err)
		assert.True(tt, exists)
	})
}
